import { SendMessageCommand, SQSClient } from '@aws-sdk/client-sqs';

import { createAcpPool } from '../db.js';
import { createPipelineLogger } from '../logger.js';
import { runTrigger } from '../trigger/handler.js';
import type {
  TriggerExtractionMessage,
  TriggerInput,
  TriggerOutput,
} from '../trigger/handler.js';
import type { PipelineConfig } from '../types.js';
import { WatermarkStore } from '../watermark/store.js';

export type LambdaTriggerEvent = TriggerInput;
export type LambdaTriggerResult = TriggerOutput;

/**
 * Wire pipeline-core's `runTrigger` into a Lambda handler. Connection details
 * come from environment variables; the pipeline (a live JS object with
 * connector + transformer instances) is passed in by the per-pipeline entry
 * file so the Lambda bundle is fully self-contained.
 *
 * Required env:
 *   ACP_POSTGRES_SECRET   Secrets Manager ARN with ACP DB credentials
 *   SQS_URL               URL of the partition fan-out queue
 *
 * Optional env:
 *   AWS_REGION            Defaults to the Lambda's region
 *   LOG_LEVEL             pino level
 */
export function createTriggerHandler(
  pipeline: PipelineConfig,
): (event: LambdaTriggerEvent) => Promise<LambdaTriggerResult> {
  const sqs = new SQSClient({});
  // Pool reused across warm invocations; created lazily so cold-start cost is
  // amortised over the first invocation rather than module load.
  let poolPromise: ReturnType<typeof createAcpPool> | undefined;

  return async (event) => {
    const logger = createPipelineLogger({
      component: 'lambda-trigger',
      pipelineName: pipeline.name,
    });
    const acpPool = await (poolPromise ??= createAcpPool({
      ...(process.env['ACP_POSTGRES_SECRET'] !== undefined
        ? { secretArn: process.env['ACP_POSTGRES_SECRET'] }
        : {}),
    }));
    const watermarkStore = new WatermarkStore(acpPool);
    const queueUrl = requireEnv('SQS_URL');

    return runTrigger(event, {
      pipeline,
      acpPool,
      watermarkStore,
      logger,
      publishExtractionMessage: async (message: TriggerExtractionMessage) => {
        await sqs.send(
          new SendMessageCommand({
            QueueUrl: queueUrl,
            MessageBody: JSON.stringify(message),
            MessageAttributes: {
              pipelineName: { DataType: 'String', StringValue: message.pipelineName },
              mode: { DataType: 'String', StringValue: message.mode },
              batchId: { DataType: 'String', StringValue: message.batchId },
            },
          }),
        );
      },
    });
  };
}

function requireEnv(name: string): string {
  const value = process.env[name];
  if (value === undefined || value === '') {
    throw new Error(`Required environment variable ${name} is not set`);
  }
  return value;
}
