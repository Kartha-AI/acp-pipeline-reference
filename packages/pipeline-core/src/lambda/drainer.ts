import { createAcpPool } from '../db.js';
import { BulkClient } from '../drainer/bulk-client.js';
import { runDrainer } from '../drainer/handler.js';
import type { DrainInput, DrainResult } from '../drainer/handler.js';
import { createPipelineLogger } from '../logger.js';
import { PIPELINE_DEFAULTS } from '../types.js';
import type { PipelineConfig } from '../types.js';
import { WatermarkStore } from '../watermark/store.js';

export type LambdaDrainEvent = DrainInput;
export type LambdaDrainResult = DrainResult;

/**
 * Required env:
 *   ACP_POSTGRES_SECRET     Secrets Manager ARN for the staging DB
 *   ACP_BULK_API_URL        Base URL of the ACP API
 * Optional:
 *   ACP_BULK_API_TOKEN      Bearer token (preferred way: pass via Secrets Manager)
 *   ACP_BULK_API_TOKEN_SECRET  Secrets Manager ARN whose SecretString *is* the token
 */
export function createDrainerHandler(
  pipeline: PipelineConfig,
): (event: LambdaDrainEvent) => Promise<LambdaDrainResult> {
  let poolPromise: ReturnType<typeof createAcpPool> | undefined;
  let cachedToken: string | undefined;

  return async (event) => {
    const logger = createPipelineLogger({
      component: 'lambda-drainer',
      pipelineName: pipeline.name,
    });
    const acpPool = await (poolPromise ??= createAcpPool({
      ...(process.env['ACP_POSTGRES_SECRET'] !== undefined
        ? { secretArn: process.env['ACP_POSTGRES_SECRET'] }
        : {}),
    }));
    const watermarkStore = new WatermarkStore(acpPool);
    const apiUrl = requireEnv('ACP_BULK_API_URL');

    if (cachedToken === undefined) {
      const direct = process.env['ACP_BULK_API_TOKEN'];
      const tokenSecretArn = process.env['ACP_BULK_API_TOKEN_SECRET'];
      if (direct !== undefined && direct.length > 0) {
        cachedToken = direct;
      } else if (tokenSecretArn !== undefined && tokenSecretArn.length > 0) {
        cachedToken = await fetchTokenSecret(tokenSecretArn);
      }
    }

    const bulkClient = new BulkClient({
      apiUrl,
      ...(cachedToken !== undefined ? { bearerToken: cachedToken } : {}),
      logger,
    });

    return runDrainer(event, {
      acpPool,
      bulkClient,
      bulkBatchSize:
        pipeline.incremental?.bulkBatchSize ?? PIPELINE_DEFAULTS.incremental.bulkBatchSize,
      watermarkStore,
      logger,
    });
  };
}

async function fetchTokenSecret(arn: string): Promise<string> {
  const { SecretsManagerClient, GetSecretValueCommand } = await import(
    '@aws-sdk/client-secrets-manager'
  );
  const client = new SecretsManagerClient({});
  try {
    const out = await client.send(new GetSecretValueCommand({ SecretId: arn }));
    if (out.SecretString === undefined || out.SecretString === '') {
      throw new Error(`ACP_BULK_API_TOKEN_SECRET ${arn} returned empty SecretString`);
    }
    return out.SecretString;
  } finally {
    client.destroy();
  }
}

function requireEnv(name: string): string {
  const value = process.env[name];
  if (value === undefined || value === '') {
    throw new Error(`Required environment variable ${name} is not set`);
  }
  return value;
}
