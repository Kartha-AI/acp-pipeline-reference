import type { Pool } from 'pg';

import { PromoteError } from '../errors.js';
import type { Logger } from '../logger.js';
import { createPipelineLogger } from '../logger.js';
import type { WatermarkStore } from '../watermark/store.js';

export interface PromoteInput {
  pipelineName: string;
  batchId: string;
  /** Optional pipeline_runs row to update on success/failure. */
  runId?: string;
  /** Watermark to record as `watermark_set` for the next run. */
  watermarkAtStart?: string;
}

export interface PromoteResult {
  pipelineName: string;
  batchId: string;
  insertedCount: number;
  rejectedCount: number;
  durationMs: number;
}

export interface PromoteDeps {
  acpPool: Pool;
  /** Optional — when provided, finishRun is called with results. */
  watermarkStore?: WatermarkStore;
  logger?: Logger;
}

interface PromoteFunctionRow {
  inserted_count: number | string;
  rejected_count: number | string;
  duration_ms: number | string;
}

/**
 * Calls `acp_promote_staging_initial(pipeline_name, batch_id)` and rolls the
 * result up into the `pipeline_runs` row (when one was supplied via runId).
 *
 * The promote function does the heavy lifting in SQL: LOCK TABLE, validation,
 * single bulk INSERT, single change_log entry. This handler is a thin wrapper.
 */
export async function runPromote(
  input: PromoteInput,
  deps: PromoteDeps,
): Promise<PromoteResult> {
  const logger = (deps.logger ?? createPipelineLogger({})).child({
    component: 'promoter',
    pipelineName: input.pipelineName,
    batchId: input.batchId,
    ...(input.runId !== undefined ? { runId: input.runId } : {}),
  });

  let result: PromoteResult;
  try {
    const res = await deps.acpPool.query<PromoteFunctionRow>(
      `SELECT inserted_count, rejected_count, duration_ms
         FROM acp_promote_staging_initial($1::text, $2::uuid)`,
      [input.pipelineName, input.batchId],
    );
    const row = res.rows[0];
    if (row === undefined) {
      throw new Error('acp_promote_staging_initial returned no rows');
    }
    result = {
      pipelineName: input.pipelineName,
      batchId: input.batchId,
      insertedCount: Number(row.inserted_count),
      rejectedCount: Number(row.rejected_count),
      durationMs: Number(row.duration_ms),
    };
    logger.info(
      {
        insertedCount: result.insertedCount,
        rejectedCount: result.rejectedCount,
        durationMs: result.durationMs,
      },
      'promote complete',
    );
  } catch (cause) {
    if (input.runId !== undefined && deps.watermarkStore !== undefined) {
      await deps.watermarkStore
        .finishRun(input.runId, {
          status: 'failed',
          error: cause instanceof Error ? cause.message : String(cause),
        })
        .catch((finishErr: unknown) => {
          logger.error({ err: finishErr }, 'failed to mark pipeline_run failed');
        });
    }
    throw new PromoteError(
      `acp_promote_staging_initial failed for ${input.pipelineName} batch ${input.batchId}`,
      {
        pipelineName: input.pipelineName,
        batchId: input.batchId,
        table: `acp_staging.${input.pipelineName}`,
      },
      { cause },
    );
  }

  if (input.runId !== undefined && deps.watermarkStore !== undefined) {
    await deps.watermarkStore.finishRun(input.runId, {
      status: 'succeeded',
      ...(input.watermarkAtStart !== undefined ? { watermarkSet: input.watermarkAtStart } : {}),
      rowsInserted: result.insertedCount,
      rowsRejected: result.rejectedCount,
    });
  }

  return result;
}
