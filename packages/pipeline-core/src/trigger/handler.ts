import { v4 as uuidv4 } from 'uuid';
import type { Pool } from 'pg';

import { ConnectorError } from '../errors.js';
import type { Logger } from '../logger.js';
import { createPipelineLogger } from '../logger.js';
import type { Partition, PipelineConfig, PipelineMode } from '../types.js';
import { WatermarkStore } from '../watermark/store.js';

export interface TriggerInput {
  pipelineName: string;
  mode: PipelineMode;
  /** Override watermark for incremental. Otherwise reads the latest succeeded run. */
  since?: string;
  /**
   * Optional pre-generated batch_id (for retries that need to target rows the
   * previous attempt staged). Trigger generates a UUID when absent.
   */
  batchId?: string;
}

export interface TriggerExtractionMessage {
  pipelineName: string;
  mode: PipelineMode;
  batchId: string;
  runId: string;
  partition: Partition;
  /** ISO watermark used as the lower bound for incremental extraction. */
  since?: string;
}

export interface TriggerOutput {
  pipelineName: string;
  mode: PipelineMode;
  batchId: string;
  runId: string;
  watermarkUsed: string | undefined;
  watermarkAtStart: string | undefined;
  partitions: Partition[];
}

export interface TriggerDeps {
  pipeline: PipelineConfig;
  acpPool: Pool;
  watermarkStore: WatermarkStore;
  /** Optional SQS publisher (production); omit for local in-process mode. */
  publishExtractionMessage?: (message: TriggerExtractionMessage) => Promise<void>;
  logger?: Logger;
}

/**
 * Trigger Lambda entry point. Idempotent shape: same input → same partition
 * fan-out (modulo source-changing-between-runs).
 *
 * Flow:
 *   1. Resolve `since` from prior pipeline_runs (incremental only).
 *   2. Generate batch_id, write `pipeline_runs` row in `running` state.
 *   3. Ensure the per-pipeline staging table exists.
 *   4. Capture current watermark from source (used as `watermark_set` after success).
 *   5. List partitions; emit one SQS message per partition (or return them for in-process mode).
 */
export async function runTrigger(
  input: TriggerInput,
  deps: TriggerDeps,
): Promise<TriggerOutput> {
  const { pipeline, watermarkStore, acpPool } = deps;
  if (input.pipelineName !== pipeline.name) {
    throw new Error(
      `runTrigger: pipelineName "${input.pipelineName}" does not match config "${pipeline.name}"`,
    );
  }

  const since =
    input.mode === 'incremental'
      ? input.since ?? (await watermarkStore.getLatestSucceededWatermark(input.pipelineName))
      : undefined;
  const batchId = input.batchId ?? uuidv4();
  const baseLogger = (deps.logger ?? createPipelineLogger({})).child({
    component: 'trigger',
  });
  const logger = baseLogger.child({
    pipelineName: input.pipelineName,
    mode: input.mode,
    batchId,
    ...(since !== undefined ? { since } : {}),
  });

  await ensureStagingTable(acpPool, pipeline.name, logger);

  const runId = await watermarkStore.startRun({
    pipelineName: input.pipelineName,
    mode: input.mode,
    batchId,
    ...(since !== undefined ? { watermarkUsed: since } : {}),
  });
  logger.info({ runId }, 'pipeline_runs row created');

  await pipeline.source.connect();
  let partitions: Partition[];
  let watermarkAtStart: string | undefined;
  try {
    // Capture before listing partitions — gives the next run a `since` that
    // is strictly less than any updated_at this run will see, so updates that
    // land mid-run are picked up next time (rather than missed).
    watermarkAtStart = await pipeline.source.getCurrentWatermark();
    partitions = await pipeline.source.listPartitions(since);
  } finally {
    await pipeline.source.disconnect();
  }

  logger.info(
    { partitionCount: partitions.length, watermarkAtStart },
    'partitions discovered',
  );

  if (deps.publishExtractionMessage !== undefined) {
    for (const partition of partitions) {
      await deps.publishExtractionMessage({
        pipelineName: input.pipelineName,
        mode: input.mode,
        batchId,
        runId,
        partition,
        ...(since !== undefined ? { since } : {}),
      });
    }
    logger.info({ partitionCount: partitions.length }, 'fan-out complete');
  }

  return {
    pipelineName: input.pipelineName,
    mode: input.mode,
    batchId,
    runId,
    watermarkUsed: since,
    watermarkAtStart: watermarkAtStart === '' ? undefined : watermarkAtStart,
    partitions,
  };
}

async function ensureStagingTable(
  pool: Pool,
  pipelineName: string,
  logger: Logger,
): Promise<void> {
  try {
    await pool.query('SELECT acp_create_staging_table($1::text)', [pipelineName]);
  } catch (cause) {
    throw new ConnectorError(
      `Failed to create staging table for ${pipelineName}`,
      { pipelineName, source: 'acp-pg', operation: 'ensureStagingTable' },
      { cause },
    );
  }
  logger.debug('staging table ready');
}
