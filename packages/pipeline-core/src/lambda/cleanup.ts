import { createAcpPool } from '../db.js';
import { runCleanup } from '../lifecycle/cleanup.js';
import type { CleanupInput, CleanupResult } from '../lifecycle/cleanup.js';
import { createPipelineLogger } from '../logger.js';
import type { PipelineConfig } from '../types.js';

export type LambdaCleanupEvent = CleanupInput;
export type LambdaCleanupResult = CleanupResult;

export function createCleanupHandler(
  pipeline: PipelineConfig,
): (event: LambdaCleanupEvent) => Promise<LambdaCleanupResult> {
  let poolPromise: ReturnType<typeof createAcpPool> | undefined;

  return async (event) => {
    const logger = createPipelineLogger({
      component: 'lambda-cleanup',
      pipelineName: pipeline.name,
    });
    const acpPool = await (poolPromise ??= createAcpPool({
      ...(process.env['ACP_POSTGRES_SECRET'] !== undefined
        ? { secretArn: process.env['ACP_POSTGRES_SECRET'] }
        : {}),
    }));
    return runCleanup(event, { acpPool, logger });
  };
}
