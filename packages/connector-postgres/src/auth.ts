import {
  GetSecretValueCommand,
  SecretsManagerClient,
} from '@aws-sdk/client-secrets-manager';
import { Signer } from '@aws-sdk/rds-signer';
import { ConnectorError } from '@acp/pipeline-core';
import type { ClientConfig } from 'pg';

/** Connection string (or pg `connectionString`) — easiest path for local dev. */
export interface ConnectionStringAuth {
  kind: 'connectionString';
  connectionString: string;
  ssl?: ClientConfig['ssl'];
}

/** AWS Secrets Manager: secret holds JSON with host/port/database/username/password. */
export interface SecretsManagerAuth {
  kind: 'secretsManager';
  secretArn: string;
  region?: string;
  /** Override database from the secret payload (multi-tenant secrets). */
  database?: string;
  ssl?: ClientConfig['ssl'];
}

/**
 * RDS IAM authentication: connection params come from config, password is
 * a short-lived token minted via @aws-sdk/rds-signer at connect time.
 */
export interface IamAuth {
  kind: 'iam';
  region: string;
  host: string;
  port?: number;
  database: string;
  username: string;
  ssl?: ClientConfig['ssl'];
}

export type PostgresConnectorAuth =
  | ConnectionStringAuth
  | SecretsManagerAuth
  | IamAuth;

interface PgSecretPayload {
  host: string;
  port: number;
  username: string;
  password: string;
  /** RDS Secrets-Manager-managed secrets use `dbname`; user-rotated may use `database`. */
  dbname?: string;
  database?: string;
  engine?: string;
}

export interface AuthResolveContext {
  pipelineName?: string;
}

/** Build a pg ClientConfig from an auth descriptor. Called once per connect(). */
export async function resolvePgConfig(
  auth: PostgresConnectorAuth,
  ctx: AuthResolveContext = {},
): Promise<ClientConfig> {
  switch (auth.kind) {
    case 'connectionString':
      return {
        connectionString: auth.connectionString,
        ...(auth.ssl !== undefined ? { ssl: auth.ssl } : {}),
      };

    case 'secretsManager':
      return resolveFromSecretsManager(auth, ctx);

    case 'iam':
      return resolveFromIam(auth, ctx);
  }
}

async function resolveFromSecretsManager(
  auth: SecretsManagerAuth,
  ctx: AuthResolveContext,
): Promise<ClientConfig> {
  const client = new SecretsManagerClient(
    auth.region !== undefined ? { region: auth.region } : {},
  );
  try {
    const out = await client.send(
      new GetSecretValueCommand({ SecretId: auth.secretArn }),
    );
    if (out.SecretString === undefined || out.SecretString === '') {
      throw new ConnectorError('Secrets Manager returned empty SecretString', {
        pipelineName: ctx.pipelineName,
        source: 'postgres',
        operation: 'resolve-auth',
      });
    }
    const secret = parseSecret(out.SecretString, auth.secretArn, ctx);
    const database = auth.database ?? secret.dbname ?? secret.database;
    if (database === undefined) {
      throw new ConnectorError(
        `Secret ${auth.secretArn} has no dbname/database field and no override was provided`,
        { pipelineName: ctx.pipelineName, source: 'postgres', operation: 'resolve-auth' },
      );
    }
    // RDS-managed Postgres requires SSL; the AWS root cert chain is trusted by
    // Node's CA bundle when present. `rejectUnauthorized: false` is acceptable
    // for local dev only — production deployers should pass a verified cert
    // via `auth.ssl`.
    return {
      host: secret.host,
      port: secret.port,
      database,
      user: secret.username,
      password: secret.password,
      ssl: auth.ssl ?? { rejectUnauthorized: false },
    };
  } catch (cause) {
    if (cause instanceof ConnectorError) throw cause;
    throw new ConnectorError(
      `Failed to fetch secret ${auth.secretArn}`,
      { pipelineName: ctx.pipelineName, source: 'postgres', operation: 'resolve-auth' },
      { cause },
    );
  } finally {
    client.destroy();
  }
}

async function resolveFromIam(
  auth: IamAuth,
  ctx: AuthResolveContext,
): Promise<ClientConfig> {
  const port = auth.port ?? 5432;
  const signer = new Signer({
    region: auth.region,
    hostname: auth.host,
    port,
    username: auth.username,
  });
  let token: string;
  try {
    token = await signer.getAuthToken();
  } catch (cause) {
    throw new ConnectorError(
      'Failed to mint RDS IAM auth token',
      { pipelineName: ctx.pipelineName, source: 'postgres', operation: 'resolve-auth' },
      { cause },
    );
  }
  return {
    host: auth.host,
    port,
    database: auth.database,
    user: auth.username,
    password: token,
    // IAM auth requires SSL on RDS; default to verifying the chain. Override
    // via `auth.ssl` if a custom CA bundle is needed.
    ssl: auth.ssl ?? { rejectUnauthorized: true },
  };
}

function parseSecret(
  raw: string,
  arn: string,
  ctx: AuthResolveContext,
): PgSecretPayload {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (cause) {
    throw new ConnectorError(
      `Secret ${arn} is not valid JSON`,
      { pipelineName: ctx.pipelineName, source: 'postgres', operation: 'resolve-auth' },
      { cause },
    );
  }
  if (parsed === null || typeof parsed !== 'object') {
    throw new ConnectorError(`Secret ${arn} is not a JSON object`, {
      pipelineName: ctx.pipelineName,
      source: 'postgres',
      operation: 'resolve-auth',
    });
  }
  const obj = parsed as Record<string, unknown>;
  for (const required of ['host', 'port', 'username', 'password'] as const) {
    if (typeof obj[required] !== (required === 'port' ? 'number' : 'string')) {
      throw new ConnectorError(
        `Secret ${arn} is missing required field "${required}"`,
        { pipelineName: ctx.pipelineName, source: 'postgres', operation: 'resolve-auth' },
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
    ...(typeof obj['engine'] === 'string' ? { engine: obj['engine'] } : {}),
  };
}
