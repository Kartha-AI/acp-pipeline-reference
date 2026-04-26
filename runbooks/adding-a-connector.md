# Adding a connector

Implementing a new `SourceConnector` for a system that isn't Postgres or
Snowflake. The interface is small and source-agnostic — anything that
can stream rows works.

## The interface

`packages/pipeline-core/src/types.ts`:

```ts
interface SourceConnector {
  connect(): Promise<void>;
  listPartitions(since?: string): Promise<Partition[]>;
  extractPartition(partition: Partition, since?: string): AsyncIterable<SourceRow>;
  getCurrentWatermark(): Promise<string>;
  disconnect(): Promise<void>;
}
```

Five methods. None of them return `any`. None of them know about ACP's
target shape — that's the transformer's job.

## Reference implementations

| Source | Status | Path |
| --- | --- | --- |
| Postgres | Production-ready | `packages/connector-postgres/` |
| Snowflake | Skeleton + 6-step implementation guide | `packages/connector-snowflake/` |

If your source is a SQL warehouse, model on Snowflake — fewer things to
fix. If it's an HTTP API or message stream, model on Postgres for the
streaming patterns and replace the cursor wrapper with whatever your
source uses for paginated/cursored reads.

## Steps

### 1. Scaffold the package

Convention is `packages/connector-<name>/`. Mirror the layout:

```
packages/connector-<name>/
├── package.json
├── tsconfig.json
├── src/
│   ├── index.ts        # barrel
│   ├── auth.ts         # credential resolution, returns SDK config
│   ├── streaming.ts    # AsyncIterable wrapper for the source's stream API
│   └── connector.ts    # SourceConnector implementation
└── tests/
    └── integration/
        └── connector.test.ts
```

`package.json` declares `@acp/pipeline-core` as a workspace dependency
and pulls in whatever SDK you need.

### 2. Implement `auth.ts`

Goal: a single function that returns connection config from a tagged
union of auth descriptors. Postgres has three: `connectionString`,
`secretsManager`, `iam`. Most real connectors only need
`secretsManager` plus a local-dev shortcut.

Use AWS SDK v3 (`@aws-sdk/client-secrets-manager`). Throw
`ConnectorError` from `@acp/pipeline-core` on failure with structured
context (`pipelineName`, `source`, `operation`).

### 3. Implement `streaming.ts`

The whole pipeline depends on memory-bounded extraction. If your SDK
returns an `AsyncIterable<Row>` natively (HTTP fetch with paginated
JSON, gRPC streaming), you're done — wrap it with proper error
propagation and you're set.

If your SDK uses event emitters or callbacks (Snowflake, Kafka), wrap
with `events.on(emitter, 'data')` + 'end'/'error' listeners. **Do not
accumulate rows into an array and yield at the end.** Memory will blow
up at scale.

### 4. Implement `connector.ts`

Class implementing `SourceConnector`. Split logic by partition strategy
— `connector-postgres`'s `listNaturalPartitions` and
`listRangePartitions` are the model. Each strategy decides:

* what discovery query to run for `listPartitions`
* what `Partition.sourceFilter` shape to emit (the framework
  serializes/deserializes this through SQS)
* how to substitute the filter into `{{partition_filter}}` for
  `extractPartition`

For watermark column quoting, validate identifiers against
`/^[A-Za-z_][A-Za-z0-9_.]*$/` before string-interpolating into SQL —
data values always go through the SDK's parameter binding, but column
names can't.

### 5. Test

Mirror `packages/connector-postgres/tests/integration/connector.test.ts`:

```ts
describe('YourConnector (integration)', () => {
  const connector = new YourConnector({...});
  beforeAll(() => connector.connect());
  afterAll(() => connector.disconnect());

  test('listPartitions returns expected count');
  test('extractPartition streams rows');
  test('listPartitions(since) filters incremental discovery');
  test('getCurrentWatermark returns non-empty');
});
```

Gate these behind an env var (`RUN_<NAME>_TESTS=1`) if they hit a real
external system.

### 6. Wire into a pipeline

A pipeline doesn't depend on a specific connector — the
`SourceConnector` interface is the seam. Copy
`pipelines/tpch-customer/pipeline.config.ts` and swap
`postgresConnector(...)` for `yourConnector(...)`. The same transformer
keeps working unmodified, since transformers see source rows, not
connector internals.

## Things to avoid

* **No connection pools larger than `extractorConcurrency`.** Each ECS
  task / Lambda invocation gets its own connector instance. Pooling
  beyond what one task uses is wasted file descriptors.
* **No retries inside the connector.** Step Functions handles retries at
  the partition level. A connector retrying internally hides failures
  from the orchestration layer.
* **No transformations.** Connectors return raw source rows. Field
  renaming, type coercion, dimension mapping all live in the
  transformer.
* **No COPY-equivalents.** Writing to `acp_staging` is the stager's job
  — the connector only reads from source. Don't reach across the seam.

## Checklist before merging

- [ ] All five `SourceConnector` methods implemented (no `not implemented` throws).
- [ ] Per-row streaming verified against a >1M-row source. Memory stays flat.
- [ ] Identifiers validated; data values bound, not interpolated.
- [ ] Errors throw `ConnectorError` with structured context.
- [ ] Integration tests cover `listPartitions`, `extractPartition`, `getCurrentWatermark`, `since` filtering.
- [ ] `pnpm -r build` clean. No `any` in source.
- [ ] At least one pipeline (`pipelines/<name>/`) wires the connector.
