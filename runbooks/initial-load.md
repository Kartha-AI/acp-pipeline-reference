# Initial load

One-time bulk backfill of an entire source dataset into ACP. Bypasses
`/v1/objects/bulk` and writes a single `change_log` marker for the whole
batch.

## When to use initial vs incremental

| | Initial | Incremental |
| --- | --- | --- |
| Throughput | 50k+ rows/sec sustained | 5–15k rows/sec |
| Path | Postgres COPY → SQL promote function | Postgres COPY → POST `/bulk` |
| Per-row `change_log` | No (one bulk marker) | Yes |
| Validation | SQL CHECK constraints only | Full ACP `/bulk` validation |
| Approval gate | Optional human pause before promote | None |
| Use case | First load, full re-bake, re-shard | Hourly/daily sync of source changes |

Use initial when the source has more than ~1M rows and you don't need
per-row change events. Run incremental afterwards to keep ACP current.

## Estimating runtime

Local benchmark on the reference repo: 100k customers + 1M orders loaded
in ~10 seconds at peak ~180 MB RSS.

For production-sized datasets, the binding factors in order are:

1. **Source extraction throughput** — usually the bottleneck. ECS Fargate
   tasks run in parallel (`extractorConcurrency` in pipeline config,
   defaults to 5). Bump to 25 for tables that read at ~10k rows/sec.
2. **`acp_staging.<pipeline>` write rate** — UNLOGGED COPY scales
   linearly until you saturate disk. RDS gp3 with default IOPS handles
   ~50–80k rows/sec; switch to io2 if a single extractor exceeds that.
3. **Promote function lock window** — a `LOCK TABLE ... IN EXCLUSIVE
   MODE` runs for the duration of the bulk INSERT. At 100M rows expect
   1–3 minutes of write contention on the staging table; nothing else
   should be writing there during promote anyway.

Rule of thumb: 100M rows in ~30 minutes end-to-end on properly sized
infrastructure. If you're seeing more than 2 hours, profile extraction
first.

## Running

### Local

```bash
docker compose up -d
pnpm seed --scale 1            # 100k customers
pnpm pipeline:run tpch-customer --mode initial
```

Last log line on success:

```
{"level":"info", "msg":"pipeline:run initial complete",
 "extracted":100000, "staged":100000, "inserted":100000, "rejected":0,
 "totalDurationMs":<n>}
```

### Production (Step Functions)

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:...:acp-tpch-customer-initial \
  --name "initial-$(date +%Y%m%d-%H%M%S)" \
  --input '{"requireApproval":false}'
```

Set `"requireApproval": true` if you want the state machine to pause
between `ValidateStaging` and `Promote`. The default ASL ships with a
`Wait` placeholder for the approval state — production deployers
typically swap this for an SNS `waitForTaskToken` task. See
`infra/stepfunctions/initial-load.asl.json` and uncomment the
`AwaitHumanApproval` template note.

## Monitoring

CloudWatch dashboard `acp-tpch_customer` (created by the monitoring
stack) shows:

* Lambda invocations / errors / duration p95 across all 6 functions
* Partition queue depth (incl. DLQ)
* Step Functions ExecutionsSucceeded / Failed for both modes
* ECS extractor task CPU max

Step Functions console gives the per-state timeline for a single
execution — quickest way to see *which* partition slowed the run.

## Approving the optional pause

If you wired `AwaitHumanApproval` to an SNS topic with
`waitForTaskToken`:

```bash
# Find the task token from the SNS message you received.
aws stepfunctions send-task-success \
  --task-token <token> \
  --output '{"approved":true}'
```

`send-task-failure` aborts the run; the state machine routes to
`RecordFailure` and the `pipeline_runs` row is marked `failed`.

## Verification queries

After promote completes, against the ACP database:

```sql
-- Row count
SELECT COUNT(*) FROM context_objects WHERE subtype = 'customer';

-- Sample row, all 7 dimensions
SELECT context FROM context_objects WHERE subtype = 'customer' LIMIT 1;

-- Run record
SELECT run_id, status, rows_inserted, rows_rejected,
       finished_at - started_at AS duration
  FROM pipeline_runs
 WHERE pipeline_name = 'tpch_customer'
 ORDER BY started_at DESC LIMIT 5;

-- Marker entry in change_log (initial mode writes exactly one)
SELECT change_type, metadata
  FROM change_log
 WHERE change_type = 'initial_load'
   AND metadata->>'pipeline_name' = 'tpch_customer'
 ORDER BY log_id DESC LIMIT 1;

-- Anything rejected?
SELECT COUNT(*), MIN(last_error)
  FROM acp_staging.tpch_customer
 WHERE last_error IS NOT NULL;
```

A clean run shows `rows_rejected = 0` and an empty `acp_staging.X` query
for `last_error IS NOT NULL`.

## Re-running

The promote function uses `ON CONFLICT (subtype, canonical_name) DO
NOTHING`, so a second `--mode initial` run produces 0 new rows and is
safe. The new run gets its own `batch_id`; staging accumulates
already-promoted rows that you can clean via `runCleanup` (Cleanup
Lambda) or a manual `DELETE FROM acp_staging.X WHERE promoted_at IS NOT
NULL AND promoted_at < now() - interval '7 days'`.
