# @acp/connector-snowflake — implementation guide

A skeleton. Methods compile and return the right TypeScript types but throw
`ConnectorError("not implemented: ...")` until you fill them in. This file
walks through what each method needs to do; inline JSDoc on
`src/connector.ts` and `src/auth.ts` carries the per-method detail.

Estimated effort: **1–2 weeks** for someone familiar with Snowflake, key-pair
auth, and the snowflake-sdk's quirks. The Postgres connector
(`packages/connector-postgres/`) is your reference; most patterns translate
1:1.

## 1. Provision the service user

```sql
-- as ACCOUNTADMIN, in Snowflake
CREATE USER acp_pipeline TYPE = SERVICE
  DEFAULT_ROLE = ACP_PIPELINE
  DEFAULT_WAREHOUSE = ACP_WH
  DEFAULT_NAMESPACE = ACP_DB.PUBLIC;

-- generate the keypair locally:
--   openssl genrsa -out rsa_key.pem 2048
--   openssl rsa -in rsa_key.pem -pubout -out rsa_key.pub
ALTER USER acp_pipeline SET RSA_PUBLIC_KEY = '<contents of rsa_key.pub minus headers>';

GRANT ROLE ACP_PIPELINE TO USER acp_pipeline;
GRANT USAGE ON WAREHOUSE ACP_WH TO ROLE ACP_PIPELINE;
GRANT USAGE ON DATABASE ACP_DB TO ROLE ACP_PIPELINE;
GRANT USAGE ON SCHEMA ACP_DB.PUBLIC TO ROLE ACP_PIPELINE;
GRANT SELECT ON ALL TABLES IN SCHEMA ACP_DB.PUBLIC TO ROLE ACP_PIPELINE;
```

Verify: `SELECT SYSTEM$VERIFY_USER_KEY_PAIR('acp_pipeline');` returns 1.

## 2. Store the private key in Secrets Manager

The connector expects the secret to hold the PEM key body — *no*
`-----BEGIN PRIVATE KEY-----` headers. Wrap with the headers in
`loadPrivateKey()` before passing to `crypto.createPrivateKey`.

```bash
PRIVATE_KEY_BODY=$(grep -v -- '-----' rsa_key.pem | tr -d '\n')
aws secretsmanager create-secret \
  --name acp/source/snowflake/<account>-<user>-key \
  --secret-string "$PRIVATE_KEY_BODY"
```

Naming convention: `acp/source/snowflake/<account>-<user>-key`. The CDK
stack (`infra/cdk/lib/pipeline-stack.ts`) reads the ARN from CDK context
under `sourcePostgresSecretArn` — rename that key when you wire Snowflake
in (or add a parallel `sourceSnowflakeSecretArn`).

## 3. Implement `auth.ts`

`loadPrivateKey()` — fetch from Secrets Manager (mirror
`packages/connector-postgres/src/auth.ts`), reattach PEM headers, parse
with `crypto.createPrivateKey`. Cache the resulting `KeyObject` for the
lifetime of the process.

`generateJwtToken()` — hand-roll with `node:crypto.sign`. Avoid pulling
in `jsonwebtoken`. The JWT shape is documented inline. Track `iat` so the
caller can detect tokens older than ~50 minutes and re-mint.

## 4. Implement `connector.ts`

In order:

| Method | What to do |
| --- | --- |
| `connect()` | Mint JWT, `snowflake-sdk.createConnection({ authenticator: 'SNOWFLAKE_JWT', ... })`, await `connection.connect`, run optional `USE WAREHOUSE/DATABASE/SCHEMA/ROLE`. |
| `listPartitions(since)` | Strategy switch (`natural`, `range`, `date`). Mirrors `connector-postgres`'s implementation — same SQL shape, swap `pg-cursor` for snowflake-sdk's `execute`. |
| `extractPartition(partition, since)` | **Streaming** via `stmt.streamRows()`. ⚠ Don't accumulate rows on the `'data'` event. Wrap with `events.on(stream, 'data')` to get an async iterator that yields per-row, exits on `'end'`, rethrows on `'error'`. |
| `getCurrentWatermark()` | `SELECT MAX(<col>)::string FROM <table>` — matches `connector-postgres`. For CDC, swap for a Snowflake `STREAM` cursor; see inline JSDoc. |
| `disconnect()` | `connection.destroy()` wrapped in a Promise. Swallow errors — this is the cleanup path. |

## 5. Test against a real Snowflake

Replicate the integration test from
`packages/connector-postgres/tests/integration/connector.test.ts`:

```ts
describe('SnowflakeConnector (integration)', () => {
  const connector = snowflakeConnector({
    pipelineName: 'tpch_customer_test',
    auth: {
      kind: 'jwt',
      account: { account: 'acme-prod', username: 'ACP_PIPELINE' },
      privateKeySecretArn: process.env.SNOWFLAKE_PRIVATE_KEY_SECRET!,
    },
    query: 'SELECT c_custkey FROM TPCH_SF1.CUSTOMER WHERE {{partition_filter}} {{since}}',
    partitions: { kind: 'natural', table: 'TPCH_SF1.CUSTOMER', column: 'C_NATIONKEY' },
    watermark: { table: 'TPCH_SF1.CUSTOMER', column: 'UPDATED_AT' },
  });

  beforeAll(() => connector.connect());
  afterAll(() => connector.disconnect());

  test('listPartitions returns expected nation count', async () => {
    const partitions = await connector.listPartitions();
    expect(partitions.length).toBeGreaterThan(0);
  });

  // ... mirror the rest from connector-postgres
});
```

Skip these in default `pnpm test` runs (gate behind a `RUN_SNOWFLAKE_TESTS`
env var) — they cost compute on a real warehouse.

## 6. Wire into a pipeline

Pattern: `pipelines/tpch-customer/pipeline.config.ts` shows the Postgres
wiring. The Snowflake equivalent is identical except for the connector
factory:

```ts
import { definePipeline } from '@acp/pipeline-core';
import { snowflakeConnector } from '@acp/connector-snowflake';
import { tpchCustomerTransformer } from './transformer.js';

export default definePipeline({
  name: 'sf_customer',
  target: 'customer',
  transformer: tpchCustomerTransformer,
  source: snowflakeConnector({
    auth: { kind: 'jwt', account: { ... }, privateKeySecretArn: '...' },
    query: '...',
    partitions: { kind: 'natural', column: 'C_NATIONKEY', table: 'CUSTOMER' },
    watermark: { table: 'CUSTOMER', column: 'UPDATED_AT' },
  }),
  initial: { extractorConcurrency: 5 },
});
```

`runbooks/adding-a-pipeline.md` walks through the full wiring.

## What this skeleton does NOT cover

* OAuth or password auth — JWT is the only path Snowflake recommends for
  service users.
* Pushing transformer logic into a Snowflake `STREAM` view — possible and
  faster for some workloads, but the framework is built around the
  `RowTransformer` boundary; pushing across that boundary is a separate
  design decision.
* Snowpark or Snowflake's stored-procedure runtimes — out of scope.
