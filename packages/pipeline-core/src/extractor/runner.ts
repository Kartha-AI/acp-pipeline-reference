import type { Pool } from 'pg';

import { ConnectorError, TransformError } from '../errors.js';
import type { Logger } from '../logger.js';
import { createPipelineLogger } from '../logger.js';
import type { Partition, PipelineConfig, PipelineMode, SourceRow, StagingRow } from '../types.js';

import type { StagingRecord } from './stager.js';
import { stageRows } from './stager.js';

export interface ExtractorInput {
  pipelineName: string;
  mode: PipelineMode;
  batchId: string;
  partition: Partition;
  /** Watermark for incremental extraction; ignored for initial. */
  since?: string;
}

export interface ExtractorResult {
  partitionId: string;
  rowsExtracted: number;
  rowsStaged: number;
  rowsTransformFailed: number;
}

export interface ExtractorDeps {
  pipeline: PipelineConfig;
  acpPool: Pool;
  logger?: Logger;
}

/**
 * One-partition extractor. Runs in an ECS Fargate task in production (one task
 * per SQS message); identical code path runs in-process locally — the CLI
 * just calls this directly per partition.
 *
 * Streaming guarantee: the source connector returns an `AsyncIterable<SourceRow>`,
 * the runner pipes it through `transformRows` (also async-iterable), and the
 * stager consumes one record at a time via COPY. At no point is the full
 * partition materialised — resident set scales with row size, not row count.
 */
export async function runExtractor(
  input: ExtractorInput,
  deps: ExtractorDeps,
): Promise<ExtractorResult> {
  const { pipeline, acpPool } = deps;
  if (input.pipelineName !== pipeline.name) {
    throw new Error(
      `runExtractor: input.pipelineName "${input.pipelineName}" does not match pipeline.name "${pipeline.name}"`,
    );
  }

  const logger = (deps.logger ?? createPipelineLogger({})).child({
    component: 'extractor',
    pipelineName: input.pipelineName,
    mode: input.mode,
    batchId: input.batchId,
    partitionId: input.partition.id,
  });

  const counts = { extracted: 0, transformFailed: 0 };

  await pipeline.source.connect();
  let result: ExtractorResult;
  try {
    const sourceRows = pipeline.source.extractPartition(
      input.partition,
      input.since,
    );
    const stagingRecords = enrichWithBatch(
      transformWithCounting(sourceRows, pipeline, counts, logger),
      input,
    );
    const stageResult = await stageRows(stagingRecords, {
      pool: acpPool,
      pipelineName: input.pipelineName,
      partitionId: input.partition.id,
      batchId: input.batchId,
      logger,
    });
    result = {
      partitionId: input.partition.id,
      rowsExtracted: counts.extracted,
      rowsStaged: stageResult.rowsCopied,
      rowsTransformFailed: counts.transformFailed,
    };
    logger.info(result, 'partition extraction complete');
  } catch (cause) {
    if (cause instanceof ConnectorError) throw cause;
    throw new ConnectorError(
      `Extraction failed for partition ${input.partition.id}`,
      {
        pipelineName: input.pipelineName,
        batchId: input.batchId,
        partitionId: input.partition.id,
        source: 'extractor',
        operation: 'runExtractor',
      },
      { cause },
    );
  } finally {
    await pipeline.source.disconnect();
  }
  return result;
}

async function* transformWithCounting(
  source: AsyncIterable<SourceRow>,
  pipeline: PipelineConfig,
  counts: { extracted: number; transformFailed: number },
  logger: Logger,
): AsyncGenerator<StagingRow, void, void> {
  for await (const row of source) {
    counts.extracted++;
    try {
      yield pipeline.transformer.toStaging(row);
    } catch (err) {
      counts.transformFailed++;
      // Per CLAUDE.md: per-row failures are logged and counted but don't
      // abort the partition. The dead-letter table (acp_rejected_rows) is
      // populated by a separate hook in Phase 4 / production wiring.
      if (err instanceof TransformError) {
        logger.warn(
          { err: { name: err.name, message: err.message, context: err.context } },
          'transform failed; skipping row',
        );
      } else {
        logger.warn({ err }, 'transform failed (non-TransformError); skipping row');
      }
    }
  }
}

async function* enrichWithBatch(
  source: AsyncIterable<StagingRow>,
  input: ExtractorInput,
): AsyncGenerator<StagingRecord, void, void> {
  for await (const row of source) {
    yield {
      ...row,
      batch_id: input.batchId,
      partition_key: input.partition.id,
      mode: input.mode,
    };
  }
}
