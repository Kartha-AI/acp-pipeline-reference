import type { Pool } from 'pg';

import { PromoteError } from '../errors.js';
import type { Logger } from '../logger.js';
import { createPipelineLogger } from '../logger.js';

export interface ValidateInput {
  pipelineName: string;
  batchId: string;
  /** Optional minimum row count expected; pipeline fails fast below this. */
  minRowCount?: number;
}

export interface ValidateResult {
  pipelineName: string;
  batchId: string;
  stagedCount: number;
  rejectedCount: number;
  rejectedReasons: Record<string, number>;
}

export interface ValidateDeps {
  acpPool: Pool;
  logger?: Logger;
}

/**
 * Run between ExtractAll and Promote in the initial state machine. Confirms
 * that staging actually has rows for this batch and surfaces any pre-flagged
 * rejections (e.g., transform failures the extractor already wrote to
 * `last_error`). Cheap query; intended as a sanity gate, not full validation.
 */
export async function runValidateStaging(
  input: ValidateInput,
  deps: ValidateDeps,
): Promise<ValidateResult> {
  const fqTable = `acp_staging.${quoteIdent(input.pipelineName)}`;
  const logger = (deps.logger ?? createPipelineLogger({})).child({
    component: 'validator',
    pipelineName: input.pipelineName,
    batchId: input.batchId,
  });

  const stagedRes = await deps.acpPool.query<{ staged_count: string; rejected_count: string }>(
    `SELECT COUNT(*)                                 AS staged_count,
            COUNT(*) FILTER (WHERE last_error IS NOT NULL) AS rejected_count
       FROM ${fqTable}
      WHERE batch_id = $1::uuid`,
    [input.batchId],
  );
  const reasonsRes = await deps.acpPool.query<{ last_error: string; count: string }>(
    `SELECT last_error, COUNT(*) AS count
       FROM ${fqTable}
      WHERE batch_id = $1::uuid
        AND last_error IS NOT NULL
      GROUP BY last_error
      ORDER BY count DESC`,
    [input.batchId],
  );

  const stagedCount = Number(stagedRes.rows[0]?.staged_count ?? 0);
  const rejectedCount = Number(stagedRes.rows[0]?.rejected_count ?? 0);
  const rejectedReasons = Object.fromEntries(
    reasonsRes.rows.map((r): [string, number] => [r.last_error, Number(r.count)]),
  );

  if (input.minRowCount !== undefined && stagedCount < input.minRowCount) {
    throw new PromoteError(
      `staging row count ${stagedCount} below minimum ${input.minRowCount}`,
      {
        pipelineName: input.pipelineName,
        batchId: input.batchId,
        table: fqTable,
      },
    );
  }

  const result = {
    pipelineName: input.pipelineName,
    batchId: input.batchId,
    stagedCount,
    rejectedCount,
    rejectedReasons,
  };
  logger.info(result, 'staging validation complete');
  return result;
}

function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}
