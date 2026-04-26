import type { Pool } from 'pg';

import type { Logger } from '../logger.js';
import { createPipelineLogger } from '../logger.js';

export interface CleanupInput {
  pipelineName: string;
  /**
   * Delete promoted rows older than this many days. 0 disables time-based
   * deletion (the function still reports counts but doesn't mutate).
   * Default 7.
   */
  retentionDays?: number;
  /** When true, also DELETE matching rows. When false, only counts. */
  apply?: boolean;
}

export interface CleanupResult {
  pipelineName: string;
  candidateCount: number;
  deletedCount: number;
  retentionDays: number;
}

export interface CleanupDeps {
  acpPool: Pool;
  logger?: Logger;
}

/**
 * Tail end of the initial state machine. Reports — and optionally deletes —
 * staging rows that are older than the retention window AND already promoted.
 *
 * Default is dry-run (apply=false) so this Lambda is safe to wire in as the
 * Cleanup state without it silently destroying data on first deploy. Flip
 * `apply` to true once you've confirmed the row counts look right.
 */
export async function runCleanup(
  input: CleanupInput,
  deps: CleanupDeps,
): Promise<CleanupResult> {
  const fqTable = `acp_staging.${quoteIdent(input.pipelineName)}`;
  const retentionDays = input.retentionDays ?? 7;
  const apply = input.apply ?? false;
  const logger = (deps.logger ?? createPipelineLogger({})).child({
    component: 'cleanup',
    pipelineName: input.pipelineName,
    retentionDays,
    apply,
  });

  if (retentionDays <= 0) {
    return {
      pipelineName: input.pipelineName,
      candidateCount: 0,
      deletedCount: 0,
      retentionDays,
    };
  }

  const countRes = await deps.acpPool.query<{ count: string }>(
    `SELECT COUNT(*) AS count
       FROM ${fqTable}
      WHERE promoted_at IS NOT NULL
        AND promoted_at < now() - ($1::int * INTERVAL '1 day')`,
    [retentionDays],
  );
  const candidateCount = Number(countRes.rows[0]?.count ?? 0);

  let deletedCount = 0;
  if (apply && candidateCount > 0) {
    const delRes = await deps.acpPool.query(
      `DELETE FROM ${fqTable}
        WHERE promoted_at IS NOT NULL
          AND promoted_at < now() - ($1::int * INTERVAL '1 day')`,
      [retentionDays],
    );
    deletedCount = delRes.rowCount ?? 0;
  }

  const result = {
    pipelineName: input.pipelineName,
    candidateCount,
    deletedCount,
    retentionDays,
  };
  logger.info(result, 'cleanup complete');
  return result;
}

function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}
