import { createAcpPool } from '../db.js';
import { runExtractor } from '../extractor/runner.js';
import type { ExtractorInput, ExtractorResult } from '../extractor/runner.js';
import { createPipelineLogger } from '../logger.js';
import type { PipelineConfig } from '../types.js';

export type LambdaExtractorEvent = ExtractorInput;
export type LambdaExtractorResult = ExtractorResult;

/**
 * Lambda extractor for incremental mode (small partitions, sub-15-minute
 * runtime). For initial mode at >100k rows per partition, use the ECS task
 * variant in `ecs/extractor.ts` instead — Lambda's 15-minute cap can't hold
 * those.
 */
export function createExtractorHandler(
  pipeline: PipelineConfig,
): (event: LambdaExtractorEvent) => Promise<LambdaExtractorResult> {
  let poolPromise: ReturnType<typeof createAcpPool> | undefined;

  return async (event) => {
    const logger = createPipelineLogger({
      component: 'lambda-extractor',
      pipelineName: pipeline.name,
    });
    const acpPool = await (poolPromise ??= createAcpPool({
      ...(process.env['ACP_POSTGRES_SECRET'] !== undefined
        ? { secretArn: process.env['ACP_POSTGRES_SECRET'] }
        : {}),
    }));

    return runExtractor(event, { pipeline, acpPool, logger });
  };
}
