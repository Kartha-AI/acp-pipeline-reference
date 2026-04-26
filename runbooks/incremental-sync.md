# Incremental sync

Ongoing sync of source changes into ACP. Reads only rows whose watermark
column has advanced since the last successful run, POSTs them through
`/v1/objects/bulk`, and writes a per-row `change_log` entry of type
`updated` or `created`.

## How watermarks work

Each `pipeline_runs` row records two timestamps:

* `watermark_used` — the lower bound (`since`) the run filtered on.
* `watermark_set` — the upper bound captured before extraction started;
  becomes the `since` for the next successful run.

Concretely, run N+1 reads:

```
SELECT … WHERE updated_at > <run N's watermark_set>
```

Strict `>` means a row updated at exactly the watermark boundary is in
run N+1, not duplicated across both. Capturing `watermark_set` *before*
extracting rather than after means rows updated mid-extraction are
picked up in the next run rather than missed.

The watermark column is per-pipeline; for the reference TPC-H pipeline
it's `customer.updated_at`. The `BEFORE UPDATE` trigger in
`source-data/tpch-schema.sql` keeps it accurate; production sources need
an equivalent trigger or a CDC stream that the connector queries.

## EventBridge schedule

`infra/cdk/lib/pipeline-stack.ts` wires an EventBridge rule that
invokes the incremental state machine on the cron expression in
`incrementalScheduleCron` (default: `cron(0 */6 * * ? *)` — every 6
hours). Adjust per pipeline:

```json
"incrementalScheduleCron": "cron(0 * * * ? *)"   // hourly
"incrementalScheduleCron": "cron(*/15 * * * ? *)" // every 15 min
```

Avoid overlap. If a sync takes 8 minutes and you schedule every 5, you
get nested runs sharing a `pipeline_runs` table. Extractions are
partition-disjoint so this *technically* works, but `change_log`
ordering becomes harder to reason about. Pick a cadence that comfortably
clears.

## Manual trigger

Identical to scheduled — just invoke the state machine yourself:

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:...:acp-tpch-customer-incremental \
  --name "manual-$(date +%Y%m%d-%H%M%S)" \
  --input '{"since":"2026-04-01T00:00:00Z"}'
```

Omit `since` to use the latest succeeded `pipeline_runs.watermark_set`.
Pass an explicit ISO timestamp to override (useful for replaying a
window after a bug fix).

Local equivalent:

```bash
pnpm pipeline:run tpch-customer --mode incremental
pnpm pipeline:run tpch-customer --mode incremental --since 2026-04-01T00:00:00Z
```

The CLI auto-starts an in-process `/bulk` stub when `BULK_API_URL` is
unset, so a fresh checkout can exercise incremental end-to-end without a
deployed ACP.

## Recovery from a failed run

Incremental is idempotent because the staging row's `batch_id` scopes
everything. If a run fails mid-extraction:

1. `pipeline_runs` shows `status=failed` for that `run_id`.
2. Some rows are in `acp_staging.<pipeline>` with `promoted_at IS NULL`.
3. The next scheduled run starts fresh: new `batch_id`, new
   `pipeline_runs` row, processes whatever changed since the last
   *successful* watermark — which still points before the failed run.

Just let the schedule re-fire, or invoke manually. The drainer's
`FOR UPDATE SKIP LOCKED` plus the `(batch_id, mode, promoted_at IS
NULL)` filter ensures the new run doesn't touch the failed run's
unpromoted rows. A separate cleanup pass (`Cleanup` Lambda) removes
stale unpromoted rows after retention.

If the failed run partially succeeded — some `/bulk` POSTs went through,
`change_log` has entries — that's fine. ACP's `/bulk` is idempotent on
`(subtype, canonical_name)`; replaying the same row produces a
`unchanged` outcome and no extra `change_log` entry.

## Performance tuning

Three knobs in `pipelines/<name>/pipeline.config.ts`:

| Knob | Default | When to change |
| --- | --- | --- |
| `incremental.bulkBatchSize` | 500 | Larger batches reduce HTTP overhead. ACP `/bulk` accepts up to ~5000 per request; bump to 1000–2000 if throughput is HTTP-bound. Beyond that, identity-merge contention inside `/bulk` dominates and larger batches stop helping. |
| `incremental.drainConcurrency` | 4 | Number of drainer Lambda invocations Step Functions runs in parallel. Bump if `/bulk` p95 latency grows under serial load — usually safe up to 8–10 before ACP starts rate-limiting. |
| `cursorBatchSize` (connector) | 1000 | Source-side fetch size. For wide rows (large JSONB context), drop to 250. For narrow rows, push to 5000. |

Watch the dashboard's `Lambda duration p95` for the drainer; spikes
correlate with `/bulk` slowness, not pipeline issues.

## Backfill window

Cold-start incremental against a fresh deploy reads from the beginning
of time (no prior `watermark_set`). For tables where that's too much,
seed `pipeline_runs` manually:

```sql
INSERT INTO pipeline_runs (pipeline_name, mode, batch_id, status, watermark_set,
                           started_at, finished_at)
VALUES ('tpch_customer', 'initial', gen_random_uuid(), 'succeeded',
        '2026-04-01T00:00:00Z', now(), now());
```

The next incremental run will use that `watermark_set` and process only
rows newer than April 1.
