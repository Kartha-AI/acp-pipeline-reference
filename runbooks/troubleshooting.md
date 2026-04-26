# Troubleshooting

Common pipeline failures, sorted by where they show up. Each entry:
**symptom → cause → fix**.

## Local CLI

### `pnpm pipeline:run` hangs at the extract phase

**Cause** — the connector can't reach its source. The pool's first
`connect()` is blocking forever (no statement_timeout because we haven't
opened a transaction yet).

**Fix** — verify the connection string. For docker-compose:

```bash
psql postgres://tpch:localdev@localhost:5433/tpch -c '\dt'
```

For deployed: confirm the Lambda's security group reaches the source DB
SG on 5432, and that the Secrets Manager secret has the right
`host`/`port`. `aws lambda invoke` the trigger function and look at
the CloudWatch error.

### `Promote failed ... table acp_staging.X does not exist`

**Cause** — `acp_create_staging_table()` was never called for this
pipeline. The trigger Lambda calls it automatically; if you skipped the
trigger and went straight to the extractor (manual debugging), the
staging table doesn't exist.

**Fix** — run the trigger first, *or*:

```sql
SELECT acp_create_staging_table('your_pipeline_name');
```

### Drainer reports many rejected rows with `error: "field … is required"`

**Cause** — the transformer's `StagingRow` shape doesn't match what
ACP's `/bulk` (or the local stub) requires. Empty `canonical_name`,
missing `subtype`, etc.

**Fix** — read the rejection details:

```sql
SELECT staging_id, source_ref, last_error
  FROM acp_staging.<pipeline>
 WHERE last_error IS NOT NULL
 ORDER BY ingested_at DESC LIMIT 20;
```

Update the transformer to populate the missing field. If the field is
genuinely optional in your source, add a guard in `toStaging` that
throws `TransformError` instead of producing a malformed row — the
extractor will skip-and-count those rather than letting them reach the
drainer.

### Drainer rejections with schema-shape errors

**Cause** — the deployed ACP context schema (`contexts/<type>.yaml`)
expects a field that the transformer doesn't produce, or vice versa.
ACP's `/bulk` validates against the schema before merging.

**Fix** — bring `contexts/<type>.yaml` and the transformer back in sync.
The reference `contexts/customer.yaml` documents the convention. The
context schema has to be loaded into the deployed ACP separately —
**this pipeline does not load it**.

## Local docker-compose

### `acp-pg` health stays `starting`, init scripts haven't run

**Cause** — usually a typo in one of the SQL files mounted into
`/docker-entrypoint-initdb.d`. The first `*.sql` to fail blocks the
container's `pg_isready` healthcheck.

**Fix** —

```bash
docker compose logs acp-pg | tail -50
```

The error has a line number. Fix the SQL, then:

```bash
docker compose down -v   # NB: -v drops the volume; init scripts only run on a fresh volume
docker compose up -d
```

### Tests fail with "relation acp_staging.tpch_customer does not exist"

**Cause** — the staging table was never created (see above). The
integration tests assume you've run a trigger first; the helper
functions in `tests/integration/helpers.ts` call `runTrigger` which
calls `acp_create_staging_table`.

**Fix** — run the test that creates it (`initial-load.test.ts`)
before tests that assume it. Vitest's `singleFork: true` setting in
`tests/vitest.config.ts` keeps the order deterministic; if you change
that, you'll need explicit setup.

## AWS deployment

### Step Functions execution times out on `ExtractAll`

**Cause** — at least one ECS task ran longer than the state machine's
timeout, or longer than ECS Fargate's task-level timeout (2 hours by
default). Usually means a partition has more rows than the connector
expected.

**Fix** — check CloudWatch logs for the `acp-<name>-extractor` log
group. Look at the partition that hung. Two options:

1. Increase the ECS task memory (`memoryLimitMiB` in
   `infra/cdk/lib/pipeline-stack.ts`) so larger partitions process
   faster.
2. Pick a finer partition strategy in the pipeline config — e.g. switch
   from `kind: 'natural'` to `kind: 'range'` with `partitionCount: 100`
   for high-cardinality keys.

### Lambda init fails: `Cannot find module 'pg'`

**Cause** — NodejsFunction bundling kept `pg` external (it's a native
module) but `nodeModules` in the bundle config didn't list it.

**Fix** — confirm `pipeline-stack.ts`'s `bundling.nodeModules` array
includes every external module: `pg`, `pg-cursor`, `pg-copy-streams`,
`pino`, `thread-stream`, `uuid`, the `@aws-sdk/*` clients you use.
Also confirm the same modules are listed in `infra/cdk/package.json`'s
`dependencies` so pnpm has them locally.

### `change_log` empty after initial-mode run

**Cause** — expected. Initial mode writes one bulk marker entry with
`change_type='initial_load'`, not per-row events. Per-row events come
from incremental mode through `/bulk`.

**Fix** — none needed. To verify the marker is there:

```sql
SELECT change_type, metadata->>'pipeline_name', metadata->>'inserted_count'
  FROM change_log
 WHERE change_type = 'initial_load'
 ORDER BY log_id DESC LIMIT 5;
```

### High memory in extractor; ECS task killed

**Cause** — connector buffering rows. The most common bug in custom
connectors: accumulating into an array on a stream's `'data'` event,
yielding on `'end'`. See `runbooks/adding-a-connector.md` "step 3" and
the warning in `packages/connector-snowflake/src/connector.ts`.

**Fix** — switch to per-row yield via `events.on(stream, 'data')` or
the SDK's native `AsyncIterable`. Verify locally with
`/usr/bin/time -l pnpm pipeline:run <name> --mode initial` at scale 1
(100k rows); peak RSS should be under 250 MB.

### Partition queue depth alarm fires

**Cause** — the trigger Lambda is sending messages but the extractor
isn't consuming them. Either Step Functions Map state isn't running, or
ECS RunTask is failing instantly.

**Fix** — check `acp-<name>-partition-dlq` for messages. If present,
the visibility-timeout-vs-task-duration math is wrong — extractor takes
longer than visibility, message goes back, eventually DLQs. Bump
`partitionQueue.visibilityTimeout` in `pipeline-stack.ts` to comfortably
exceed your worst-case partition runtime.

### `/bulk` returns 401/403

**Cause** — `ACP_BULK_API_TOKEN_SECRET` ARN points at a stale/missing
secret, or the token has rotated and the drainer's cached value is
expired.

**Fix** — rotate the secret value via Secrets Manager. The drainer
caches the token across warm Lambda invocations; force a cold start by
deploying any change to the drainer Lambda, or by waiting ~15 min for
AWS to recycle.

### `pipeline_runs` rows stuck in `running`

**Cause** — a Lambda crashed before reaching `finishRun()`. The
`pipeline_runs` row was never updated.

**Fix** — manually mark it failed:

```sql
UPDATE pipeline_runs
   SET status = 'failed',
       error  = 'manual: orphaned by handler crash',
       finished_at = now()
 WHERE pipeline_name = '<name>'
   AND status = 'running'
   AND started_at < now() - interval '1 hour';
```

The next incremental run reads the latest *succeeded* watermark, so a
stuck `running` row doesn't block new runs — it just clutters the
dashboard.

### Step Functions ASL validation error after editing `*.asl.json`

**Cause** — typo in a state name, JSON syntax error, or a `Next` that
doesn't resolve.

**Fix** — `pnpm exec cdk synth` runs CloudFormation validation locally
and surfaces the bad state name. For deeper checks against the AWS
schema:

```bash
aws stepfunctions validate-state-machine-definition \
  --definition file:///path/to/your.asl.json
```

(needs valid AWS credentials; the API call is server-side.)
