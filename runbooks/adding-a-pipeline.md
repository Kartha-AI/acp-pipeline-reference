# Adding a pipeline

Wiring an existing connector + a new transformer to load a new domain
into ACP. Most enterprises will end up with one pipeline per major
context type (customer, vendor, invoice, ...). The connector is reusable;
the transformer and pipeline config are domain-specific.

## What goes in a pipeline package

`pipelines/tpch-customer/` is the template:

```
pipelines/<name>/
├── package.json            # workspace deps on pipeline-core + connector
├── tsconfig.json
├── transformer.ts          # pure function: source row -> StagingRow
├── pipeline.config.ts      # wires connector + transformer + defaults
├── README.md               # what this pipeline maps and why
└── tests/
    └── transformer.test.ts # vitest, fixture-based
```

## 1. Write the transformer

The transformer is a pure function. Input is one source row (whatever
the connector emits). Output is a `StagingRow`:

```ts
interface StagingRow {
  subtype: string;          // ACP context type ("customer", "vendor", ...)
  source: string;           // e.g. "tpch", "salesforce", "ledger"
  source_ref: string;       // stable identifier in the source system
  canonical_name: string;   // identity-merge key (UNIQUE in context_objects)
  context: SevenDimensions; // see below
}
```

`canonical_name` is the identity-merge key. ACP's `context_objects`
table has `UNIQUE (subtype, canonical_name)`, so two rows that should
collapse into one entity must produce the same canonical_name. For the
reference TPC-H pipeline that's `c_name` (e.g. `Customer#000000042`),
which the source guarantees unique. For real customers — pick the
business key your domain treats as identity.

### The 7 dimensions

ACP's context schema buckets every fact into one of seven dimensions.
Pick the one that answers "what kind of fact is this":

| Dimension | Holds |
| --- | --- |
| `attributes` | Static-ish properties of the entity itself (id, address, comment) |
| `measures` | Numeric/quantitative metrics (balance, count, lifetime spend) |
| `actors` | People/orgs involved in the entity (customer name, sales owner) |
| `temporals` | Time-related fields (source_updated_at, last_order_at) |
| `locations` | Geo (nation, region, building, lat/lon) |
| `intents` | Purpose / segment / category (market_segment, persona) |
| `processes` | Workflow / lifecycle state (active vs prospect, order_count) |

Empty dimensions are allowed but populate at least one key in each
where it makes sense — agents querying ACP rely on the dimension shape.
The reference transformer fills all seven.

### Pure function rules

* No I/O. No `await pool.query(...)`, no `fetch(...)`. The transformer
  runs once per source row at extraction speed; an HTTP call here
  multiplies latency by row count.
* No randomness, no `Date.now()`. Output must be reproducible from
  input. Use the source row's `updated_at`, not a clock.
* Unmappable rows throw `TransformError` from `@acp/pipeline-core`. The
  extractor counts these and skips, doesn't abort the partition.

## 2. Write the pipeline config

`pipeline.config.ts` calls `definePipeline(...)` from
`@acp/pipeline-core` and exports the result as default. Reference is
`pipelines/tpch-customer/pipeline.config.ts`.

```ts
import { definePipeline } from '@acp/pipeline-core';
import { postgresConnector } from '@acp/connector-postgres';
import { vendorTransformer } from './transformer.js';

export default definePipeline({
  name: 'vendor',                 // SQL identifier; becomes acp_staging.vendor table
  target: 'vendor',               // ACP context type
  transformer: vendorTransformer,
  source: postgresConnector({
    pipelineName: 'vendor',
    auth: { kind: 'connectionString',
            connectionString: process.env.SOURCE_PG_URL ?? '...' },
    query: `
      SELECT v.id, v.name, ...
        FROM vendors v
       WHERE {{partition_filter}}
         {{since}}
    `,
    partitions: { kind: 'natural', table: 'vendors', column: 'category_id' },
    watermark: { table: 'vendors', column: 'updated_at' },
  }),
  initial: { extractorConcurrency: 5 },
  incremental: { bulkBatchSize: 500, drainConcurrency: 4 },
});
```

`name` requirements:

* Lowercase letters, digits, underscores.
* Starts with a letter.
* Becomes the suffix of the staging table (`acp_staging.<name>`) — keep
  it short; Postgres caps at 63 bytes total.

## 3. Register the staging table

The trigger Lambda calls `acp_create_staging_table('<name>')`
automatically on the first run, so you don't have to do anything. The
function is idempotent.

## 4. Add tests

`tests/transformer.test.ts` should cover:

* Happy path with realistic source row → all 7 dimensions populated.
* Edge cases (NULLs in optional columns, zero-quantity entities,
  bigint-overflow IDs).
* Required-field violations → throws `TransformError`.

The fixture-based pattern in `tests/fixtures/sample-rows.json` paired
with `tests/integration/transformer-fixtures.test.ts` makes
input/output diffs reviewable in PRs.

## 5. Wire into CDK

`infra/cdk/lambda/*.ts` import a specific pipeline and bind handlers:

```ts
// infra/cdk/lambda/trigger-vendor.ts
import { createTriggerHandler } from '@acp/pipeline-core';
import pipeline from '@acp/pipeline-vendor';
export const handler = createTriggerHandler(pipeline);
```

Add a parallel entry per Lambda role (trigger, extractor, promoter,
drainer, validate, cleanup) and a parallel `PipelineStack` instance
keyed on the pipeline name. Or — simpler if you have many pipelines —
parameterize the existing stack with `pipelineName` and deploy multiple
stack instances:

```bash
pnpm exec cdk deploy --all -c pipelineName=vendor
pnpm exec cdk deploy --all -c pipelineName=invoice
```

## 6. Test locally

```bash
docker compose up -d
pnpm seed --scale 0.01            # only if your source uses TPC-H
# OR set up your own seed data via SQL/CSV against source-pg

pnpm pipeline:run vendor --mode initial
pnpm pipeline:run vendor --mode incremental
```

Verify against the ACP DB:

```sql
SELECT COUNT(*), subtype FROM context_objects GROUP BY subtype;
SELECT context FROM context_objects WHERE subtype = 'vendor' LIMIT 1;
```

Sample row should have all 7 dimensions populated as you intended.

## Checklist

- [ ] `pnpm -r build` clean — workspace pulls in the new package.
- [ ] Transformer unit tests cover happy path, edge cases, error path.
- [ ] `pnpm pipeline:run <name> --mode initial` succeeds locally.
- [ ] Re-running produces 0 new rows (idempotent).
- [ ] `pnpm pipeline:run <name> --mode incremental` after a source
      `UPDATE` writes the right number of `change_log` entries.
- [ ] CDK Lambda entries exist (or `pipelineName` context flow used).
- [ ] `contexts/<name>.yaml` describes the target context type — see
      `contexts/customer.yaml` for the format.
