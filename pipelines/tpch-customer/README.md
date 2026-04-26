# pipelines/tpch-customer

The reference pipeline. Loads TPC-H `customer` rows out of source-pg and
into the ACP `customer` context object.

## Shape

| Aspect            | Value                                                    |
| ----------------- | -------------------------------------------------------- |
| Source connector  | `@acp/connector-postgres` against the TPC-H schema       |
| Source query      | `customer ⋈ nation ⋈ region` + per-customer order aggs   |
| Partition         | `c_nationkey` (natural — 25 partitions at full scale)    |
| Watermark         | `customer.updated_at`                                    |
| Target subtype    | `customer`                                               |
| `canonical_name`  | `c_name` (e.g. `Customer#000000042`)                     |
| `source_ref`      | `c_custkey` as a string                                  |

## 7-dimension mapping

| Dimension   | Fields                                                          |
| ----------- | --------------------------------------------------------------- |
| attributes  | `custkey`, `phone`, `street_address`, `comment`                 |
| measures    | `account_balance_usd`, `order_count`, `lifetime_spend_usd`      |
| actors      | `customer_name`                                                 |
| temporals   | `source_updated_at`, `last_order_at`                            |
| locations   | `nation_key`, `nation`, `region`                                |
| intents     | `market_segment`                                                |
| processes   | `activity_state` (`active` if has orders else `prospect`), `order_count` |

The transformer is a pure function. `toStaging()` takes one denormalized
TPC-H row and returns a `StagingRow` — no I/O, easily unit tested.
See `transformer.test.ts` for the contract.

## Configuration

Connection comes from the `SOURCE_PG_URL` env var; falls back to
`postgres://tpch:localdev@localhost:5433/tpch` for local docker-compose.

To point at a different source (e.g. an enterprise warehouse via SSH
tunnel), set `SOURCE_PG_URL` before running. To use Secrets Manager or
RDS IAM auth, swap the `auth` block in `pipeline.config.ts`:

```ts
auth: { kind: 'secretsManager', secretArn: process.env.SOURCE_SECRET_ARN! }
// or
auth: { kind: 'iam', region: 'us-east-1', host: '...', database: 'tpch', username: 'reader' }
```

## Running

The pipeline runtime ships in Phase 3. Once available:

```bash
pnpm pipeline:run tpch-customer --mode initial
pnpm pipeline:run tpch-customer --mode incremental --since 2026-04-01T00:00:00Z
```

Until then, you can verify the transformer with `pnpm --filter
@acp/pipeline-tpch-customer test` and the connector against docker-compose
with `pnpm --filter @acp/connector-postgres test:integration`.
