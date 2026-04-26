import { createAcpPool } from '../db.js';
import { createPipelineLogger } from '../logger.js';
import { runPromote } from '../promoter/handler.js';
import type { PromoteInput, PromoteResult } from '../promoter/handler.js';
import type { PipelineConfig } from '../types.js';
import { WatermarkStore } from '../watermark/store.js';

export type LambdaPromoteEvent = PromoteInput;
export type LambdaPromoteResult = PromoteResult;

export function createPromoterHandler(
  pipeline: PipelineConfig,
): (event: LambdaPromoteEvent) => Promise<LambdaPromoteResult> {
  let poolPromise: ReturnType<typeof createAcpPool> | undefined;

  return async (event) => {
    const logger = createPipelineLogger({
      component: 'lambda-promoter',
      pipelineName: pipeline.name,
    });
    const acpPool = await (poolPromise ??= createAcpPool({
      ...(process.env['ACP_POSTGRES_SECRET'] !== undefined
        ? { secretArn: process.env['ACP_POSTGRES_SECRET'] }
        : {}),
    }));
    const watermarkStore = new WatermarkStore(acpPool);

    return runPromote(event, { acpPool, watermarkStore, logger });
  };
}
