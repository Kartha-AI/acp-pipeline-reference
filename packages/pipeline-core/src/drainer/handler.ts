import type { Pool, PoolClient } from 'pg';

import { BulkApiError } from '../errors.js';
import type { Logger } from '../logger.js';
import { createPipelineLogger } from '../logger.js';
import type { SevenDimensions, StagingRow } from '../types.js';
import type { WatermarkStore } from '../watermark/store.js';

import { BulkClient } from './bulk-client.js';
import type { BulkResponse } from './bulk-client.js';

export interface DrainInput {
  pipelineName: string;
  batchId: string;
  /** Optional pipeline_runs row to update on success/failure. */
  runId?: string;
  /** Watermark to record as `watermark_set` on success. */
  watermarkAtStart?: string;
}

export interface DrainResult {
  pipelineName: string;
  batchId: string;
  rowsPosted: number;
  rowsCreated: number;
  rowsUpdated: number;
  rowsUnchanged: number;
  rowsRejected: number;
  batchesPosted: number;
}

export interface DrainerDeps {
  acpPool: Pool;
  bulkClient: BulkClient;
  /** Number of staging rows POSTed per /bulk call. */
  bulkBatchSize: number;
  watermarkStore?: WatermarkStore;
  logger?: Logger;
}

interface StagingDbRow {
  staging_id: string;
  subtype: string;
  source: string;
  source_ref: string;
  canonical_name: string;
  context: SevenDimensions;
}

/**
 * Drain unpromoted incremental rows from `acp_staging.<pipeline>` to ACP via
 * `/v1/objects/bulk`. The loop:
 *
 *   1. SELECT … FOR UPDATE SKIP LOCKED — claims a batch of rows for this
 *      drainer; concurrent drainers see different rows.
 *   2. POST batch to /bulk; receive per-row outcomes.
 *   3. UPDATE staging: set promoted_at on accepted rows, set last_error on
 *      rejected rows. Both happen in the same transaction as step 1, so
 *      claimed rows are atomically resolved before COMMIT releases the lock.
 *   4. COMMIT, then loop to step 1 until the partial-index returns no rows.
 */
export async function runDrainer(
  input: DrainInput,
  deps: DrainerDeps,
): Promise<DrainResult> {
  const fqTable = `acp_staging.${quoteIdent(input.pipelineName)}`;
  const logger = (deps.logger ?? createPipelineLogger({})).child({
    component: 'drainer',
    pipelineName: input.pipelineName,
    batchId: input.batchId,
    ...(input.runId !== undefined ? { runId: input.runId } : {}),
  });

  const totals = {
    rowsPosted: 0,
    rowsCreated: 0,
    rowsUpdated: 0,
    rowsUnchanged: 0,
    rowsRejected: 0,
    batchesPosted: 0,
  };

  try {
    while (true) {
      const claimed = await claimBatch(deps.acpPool, fqTable, input.batchId, deps.bulkBatchSize, logger, async (rows, client) => {
        if (rows.length === 0) return rows.length;
        const bulkRows: StagingRow[] = rows.map((r) => ({
          subtype: r.subtype,
          source: r.source,
          source_ref: r.source_ref,
          canonical_name: r.canonical_name,
          context: r.context,
        }));
        const response = await deps.bulkClient.post(
          { rows: bulkRows },
          { pipelineName: input.pipelineName, batchId: input.batchId },
        );
        applyOutcomes(response, rows, totals);
        await applyStagingUpdates(client, fqTable, response, rows);
        totals.batchesPosted++;
        totals.rowsPosted += rows.length;
        return rows.length;
      });
      if (claimed === 0) break;
    }
    logger.info(totals, 'drain complete');
  } catch (cause) {
    if (input.runId !== undefined && deps.watermarkStore !== undefined) {
      await deps.watermarkStore
        .finishRun(input.runId, {
          status: 'failed',
          rowsInserted: totals.rowsCreated + totals.rowsUpdated,
          rowsRejected: totals.rowsRejected,
          error: cause instanceof Error ? cause.message : String(cause),
        })
        .catch((finishErr: unknown) =>
          logger.error({ err: finishErr }, 'failed to mark pipeline_run failed'),
        );
    }
    if (cause instanceof BulkApiError) throw cause;
    throw cause;
  }

  if (input.runId !== undefined && deps.watermarkStore !== undefined) {
    await deps.watermarkStore.finishRun(input.runId, {
      status: 'succeeded',
      ...(input.watermarkAtStart !== undefined ? { watermarkSet: input.watermarkAtStart } : {}),
      rowsInserted: totals.rowsCreated + totals.rowsUpdated,
      rowsRejected: totals.rowsRejected,
    });
  }

  return {
    pipelineName: input.pipelineName,
    batchId: input.batchId,
    ...totals,
  };
}

async function claimBatch(
  pool: Pool,
  fqTable: string,
  batchId: string,
  limit: number,
  logger: Logger,
  process: (rows: StagingDbRow[], client: PoolClient) => Promise<number>,
): Promise<number> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const res = await client.query<StagingDbRow>(
      `SELECT staging_id::text AS staging_id,
              subtype, source, source_ref, canonical_name, context
         FROM ${fqTable}
        WHERE batch_id = $1::uuid
          AND mode = 'incremental'
          AND promoted_at IS NULL
        ORDER BY staging_id
        LIMIT $2
        FOR UPDATE SKIP LOCKED`,
      [batchId, limit],
    );
    const claimed = await process(res.rows, client);
    await client.query('COMMIT');
    if (claimed > 0) {
      logger.debug({ claimed }, 'drainer batch committed');
    }
    return claimed;
  } catch (cause) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw cause;
  } finally {
    client.release();
  }
}

function applyOutcomes(
  response: BulkResponse,
  rows: StagingDbRow[],
  totals: { rowsCreated: number; rowsUpdated: number; rowsUnchanged: number; rowsRejected: number },
): void {
  for (let i = 0; i < rows.length; i++) {
    const result = response.results[i];
    if (result === undefined) continue;
    switch (result.outcome) {
      case 'created':
        totals.rowsCreated++;
        break;
      case 'updated':
        totals.rowsUpdated++;
        break;
      case 'unchanged':
        totals.rowsUnchanged++;
        break;
      case 'rejected':
        totals.rowsRejected++;
        break;
    }
  }
}

async function applyStagingUpdates(
  client: PoolClient,
  fqTable: string,
  response: BulkResponse,
  rows: StagingDbRow[],
): Promise<void> {
  const successIds: string[] = [];
  const rejected: Array<{ id: string; error: string }> = [];
  for (let i = 0; i < rows.length; i++) {
    const result = response.results[i];
    const stagingId = rows[i]?.staging_id;
    if (result === undefined || stagingId === undefined) continue;
    if (result.outcome === 'rejected') {
      rejected.push({ id: stagingId, error: result.error ?? 'rejected by /bulk' });
    } else {
      successIds.push(stagingId);
    }
  }
  if (successIds.length > 0) {
    await client.query(
      `UPDATE ${fqTable}
          SET promoted_at = now(),
              last_error  = NULL
        WHERE staging_id = ANY($1::bigint[])`,
      [successIds],
    );
  }
  if (rejected.length > 0) {
    // Stamp promoted_at so the partial index doesn't keep returning these
    // rows; last_error captures the reason for triage.
    await client.query(
      `UPDATE ${fqTable} AS s
          SET promoted_at = now(),
              last_error  = u.err
         FROM (SELECT unnest($1::bigint[]) AS staging_id,
                      unnest($2::text[])   AS err) u
        WHERE s.staging_id = u.staging_id`,
      [rejected.map((r) => r.id), rejected.map((r) => r.error)],
    );
  }
}

function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}
