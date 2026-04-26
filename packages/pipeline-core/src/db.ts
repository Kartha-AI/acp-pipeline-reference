import {
  GetSecretValueCommand,
  SecretsManagerClient,
} from '@aws-sdk/client-secrets-manager';
import { Pool } from 'pg';
import type { PoolConfig } from 'pg';

import { ConnectorError } from './errors.js';

export interface AcpPgConfig {
  /** Direct connection string. Wins over `secretArn`. */
  connectionString?: string;
  /** Or AWS Secrets Manager ARN with `{ host, port, dbname|database, username, password }` payload. */
  secretArn?: string;
  /** AWS region for Secrets Manager. Defaults to AWS_REGION env. */
  region?: string;
  /** Pool size. Default 4. */
  poolSize?: number;
  /** Per-connection statement_timeout (ms). 0 disables. Default 0. */
  statementTimeoutMs?: number;
  /** Override default `idle_in_transaction_session_timeout` (ms). Default 60_000. */
  idleInTransactionTimeoutMs?: number;
  /** SSL config. RDS-managed secrets default to `{ rejectUnauthorized: false }`. */
  ssl?: PoolConfig['ssl'];
}

interface AcpSecretPayload {
  host: string;
  port: number;
  username: string;
  password: string;
  dbname?: string;
  database?: string;
}

/**
 * Build a Postgres pool against the ACP database. Used by the promoter,
 * drainer, trigger, and watermark store. Resolves credentials from either
 * a Secrets Manager ARN, a connection string, or the ACP_PG_URL env var
 * (the local-dev default).
 */
export async function createAcpPool(config: AcpPgConfig = {}): Promise<Pool> {
  const poolConfig = await resolvePoolConfig(config);
  return new Pool({
    ...poolConfig,
    max: config.poolSize ?? 4,
    idleTimeoutMillis: 30_000,
    statement_timeout: config.statementTimeoutMs ?? 0,
    idle_in_transaction_session_timeout: config.idleInTransactionTimeoutMs ?? 60_000,
  });
}

async function resolvePoolConfig(config: AcpPgConfig): Promise<PoolConfig> {
  if (config.secretArn !== undefined) {
    const secret = await fetchSecret(config.secretArn, config.region);
    const database = secret.dbname ?? secret.database;
    if (database === undefined) {
      throw new ConnectorError(
        `ACP secret ${config.secretArn} missing dbname/database field`,
        { source: 'acp-pg', operation: 'resolve-auth' },
      );
    }
    return {
      host: secret.host,
      port: secret.port,
      database,
      user: secret.username,
      password: secret.password,
      ssl: config.ssl ?? { rejectUnauthorized: false },
    };
  }
  const url =
    config.connectionString ??
    process.env['ACP_PG_URL'] ??
    'postgres://acp:localdev@localhost:5432/acp';
  return {
    connectionString: url,
    ...(config.ssl !== undefined ? { ssl: config.ssl } : {}),
  };
}

async function fetchSecret(arn: string, region: string | undefined): Promise<AcpSecretPayload> {
  const client = new SecretsManagerClient(region !== undefined ? { region } : {});
  try {
    const out = await client.send(new GetSecretValueCommand({ SecretId: arn }));
    if (out.SecretString === undefined || out.SecretString === '') {
      throw new ConnectorError(`Secret ${arn} returned empty SecretString`, {
        source: 'acp-pg',
        operation: 'fetch-secret',
      });
    }
    let parsed: unknown;
    try {
      parsed = JSON.parse(out.SecretString);
    } catch (cause) {
      throw new ConnectorError(
        `Secret ${arn} is not valid JSON`,
        { source: 'acp-pg', operation: 'fetch-secret' },
        { cause },
      );
    }
    if (parsed === null || typeof parsed !== 'object') {
      throw new ConnectorError(`Secret ${arn} is not a JSON object`, {
        source: 'acp-pg',
        operation: 'fetch-secret',
      });
    }
    const obj = parsed as Record<string, unknown>;
    for (const required of ['host', 'port', 'username', 'password'] as const) {
      const expected = required === 'port' ? 'number' : 'string';
      if (typeof obj[required] !== expected) {
        throw new ConnectorError(
          `Secret ${arn} missing required field "${required}" (${expected})`,
          { source: 'acp-pg', operation: 'fetch-secret' },
        );
      }
    }
    return {
      host: obj['host'] as string,
      port: obj['port'] as number,
      username: obj['username'] as string,
      password: obj['password'] as string,
      ...(typeof obj['dbname'] === 'string' ? { dbname: obj['dbname'] } : {}),
      ...(typeof obj['database'] === 'string' ? { database: obj['database'] } : {}),
    };
  } finally {
    client.destroy();
  }
}
