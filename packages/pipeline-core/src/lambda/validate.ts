import { createAcpPool } from '../db.js';
import { runValidateStaging } from '../lifecycle/validate.js';
import type { ValidateInput, ValidateResult } from '../lifecycle/validate.js';
import { createPipelineLogger } from '../logger.js';
import type { PipelineConfig } from '../types.js';

export type LambdaValidateEvent = ValidateInput;
export type LambdaValidateResult = ValidateResult;

export function createValidateHandler(
  pipeline: PipelineConfig,
): (event: LambdaValidateEvent) => Promise<LambdaValidateResult> {
  let poolPromise: ReturnType<typeof createAcpPool> | undefined;

  return async (event) => {
    const logger = createPipelineLogger({
      component: 'lambda-validate',
      pipelineName: pipeline.name,
    });
    const acpPool = await (poolPromise ??= createAcpPool({
      ...(process.env['ACP_POSTGRES_SECRET'] !== undefined
        ? { secretArn: process.env['ACP_POSTGRES_SECRET'] }
        : {}),
    }));
    return runValidateStaging(event, { acpPool, logger });
  };
}
