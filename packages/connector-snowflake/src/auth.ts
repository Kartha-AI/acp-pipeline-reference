import { ConnectorError } from '@acp/pipeline-core';

/** Connection coordinates for a Snowflake account. */
export interface SnowflakeAccount {
  /** Account identifier in the form `<orgname>-<account_name>` (e.g. `acme-prod`). */
  account: string;
  /** Service user with key-pair auth attached. */
  username: string;
  warehouse?: string;
  database?: string;
  schema?: string;
  role?: string;
}

export interface JwtAuth {
  kind: 'jwt';
  account: SnowflakeAccount;
  /**
   * AWS Secrets Manager ARN whose SecretString is the PEM private key
   * (no headers) used to sign the JWT.
   */
  privateKeySecretArn: string;
  /** Optional region override for the secret. Defaults to AWS_REGION. */
  region?: string;
}

export type SnowflakeAuth = JwtAuth;

/**
 * Stub. The real implementation must:
 *
 *   1. Fetch the private key PEM from Secrets Manager using
 *      @aws-sdk/client-secrets-manager (same pattern as
 *      packages/connector-postgres/src/auth.ts).
 *   2. Parse it via `crypto.createPrivateKey(pem)`. The PEM stored in the
 *      secret should be the key body only — no `-----BEGIN PRIVATE KEY-----`
 *      headers — so this function reattaches them before parsing.
 *   3. Compute the SHA-256 fingerprint of the *public* key (Snowflake compares
 *      this against the value of `RSA_PUBLIC_KEY_FP` on the user account).
 *   4. Cache the parsed key for the lifetime of the process; key fetches are
 *      slow (Secrets Manager round-trip) and the connector reuses the
 *      auth across many calls.
 *
 * Returning `unknown` here keeps the public surface simple while the real
 * type (a Node `KeyObject`) lives behind `loadPrivateKey`'s implementation.
 */
export async function loadPrivateKey(_auth: JwtAuth): Promise<unknown> {
  throw new ConnectorError(
    'not implemented: loadPrivateKey — see packages/connector-snowflake/README.md "Implement auth.ts"',
    { source: 'snowflake', operation: 'loadPrivateKey' },
  );
}

/**
 * Stub. Snowflake's expected JWT shape:
 *
 *   header:  { alg: "RS256", typ: "JWT" }
 *   payload: {
 *     iss: "<ACCOUNT>.<USER>.SHA256:<base64-pubkey-fp>",
 *     sub: "<ACCOUNT>.<USER>",
 *     iat: <unix seconds>,
 *     exp: <iat + 3600>            // max 1h per Snowflake
 *   }
 *   signature: RS256 over header.payload using the private key
 *
 *   * `<ACCOUNT>` and `<USER>` must be UPPERCASED.
 *   * `<base64-pubkey-fp>` is `Buffer.from(sha256(pubkey-DER)).toString('base64')`.
 *   * Snowflake refuses tokens > 1h. Callers should re-mint within the
 *     connector once the issued token is older than ~50 minutes; track
 *     issuance time in memory and re-call this function lazily.
 *
 * Hand-roll the JWT with `node:crypto` (avoids pulling in `jsonwebtoken` —
 * snowflake-sdk doesn't need it either). Steps:
 *
 *     const header   = base64url(JSON.stringify({ alg: 'RS256', typ: 'JWT' }));
 *     const payload  = base64url(JSON.stringify({ iss, sub, iat, exp }));
 *     const signing  = `${header}.${payload}`;
 *     const sig      = base64url(crypto.sign('RSA-SHA256', Buffer.from(signing), key));
 *     return `${signing}.${sig}`;
 */
export async function generateJwtToken(_auth: JwtAuth): Promise<string> {
  throw new ConnectorError(
    'not implemented: generateJwtToken — see packages/connector-snowflake/README.md "Implement auth.ts"',
    { source: 'snowflake', operation: 'generateJwtToken' },
  );
}
