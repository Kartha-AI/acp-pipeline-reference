import { createAcpPool } from '../db.js';
import { runExtractor } from '../extractor/runner.js';
import type { ExtractorInput, ExtractorResult } from '../extractor/runner.js';
import { createPipelineLogger } from '../logger.js';
import type { Partition, PipelineConfig, PipelineMode } from '../types.js';

/**
 * ECS Fargate task entrypoint for the extractor. Step Functions' ECS RunTask
 * integration passes the partition assignment via container ContainerOverrides
 * environment variables; this entrypoint reads them, calls the same
 * `runExtractor` the Lambda extractor does, and exits.
 *
 * Required env (set by Step Functions Map's ItemSelector + ContainerOverrides):
 *   PIPELINE_NAME      String
 *   MODE               'initial' | 'incremental'
 *   BATCH_ID           UUID
 *   PARTITION_JSON     JSON-encoded Partition
 * Optional env:
 *   SINCE              ISO watermark (incremental only)
 *   ACP_POSTGRES_SECRET  Same convention as Lambdas
 */
export function createEcsExtractorEntrypoint(
  pipeline: PipelineConfig,
): () => Promise<void> {
  return async () => {
    const logger = createPipelineLogger({
      component: 'ecs-extractor',
      pipelineName: pipeline.name,
    });
    const input = readInputFromEnv();
    if (input.pipelineName !== pipeline.name) {
      throw new Error(
        `ECS extractor: PIPELINE_NAME ${input.pipelineName} does not match bundled pipeline ${pipeline.name}`,
      );
    }

    const acpPool = await createAcpPool({
      ...(process.env['ACP_POSTGRES_SECRET'] !== undefined
        ? { secretArn: process.env['ACP_POSTGRES_SECRET'] }
        : {}),
    });
    let result: ExtractorResult;
    try {
      result = await runExtractor(input, { pipeline, acpPool, logger });
    } finally {
      await acpPool.end().catch(() => undefined);
    }
    logger.info(result, 'ecs extractor task complete');
    // Exit 0 implicit. ECS treats non-zero exit as task failure → Step
    // Functions retries per the state machine's Retry config.
  };
}

function readInputFromEnv(): ExtractorInput {
  const pipelineName = requireEnv('PIPELINE_NAME');
  const mode = requireEnv('MODE');
  if (mode !== 'initial' && mode !== 'incremental') {
    throw new Error(`MODE must be 'initial' or 'incremental'; got "${mode}"`);
  }
  const batchId = requireEnv('BATCH_ID');
  const partitionJson = requireEnv('PARTITION_JSON');
  let partition: Partition;
  try {
    partition = JSON.parse(partitionJson) as Partition;
  } catch (cause) {
    throw new Error(`PARTITION_JSON is not valid JSON: ${(cause as Error).message}`);
  }
  if (typeof partition.id !== 'string' || typeof partition.sourceFilter !== 'object') {
    throw new Error(`PARTITION_JSON does not match Partition shape: ${partitionJson}`);
  }
  const since = process.env['SINCE'];
  return {
    pipelineName,
    mode: mode as PipelineMode,
    batchId,
    partition,
    ...(since !== undefined && since !== '' ? { since } : {}),
  };
}

function requireEnv(name: string): string {
  const value = process.env[name];
  if (value === undefined || value === '') {
    throw new Error(`Required environment variable ${name} is not set`);
  }
  return value;
}
