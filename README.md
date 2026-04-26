# acp-pipeline-reference

Reference pipeline for loading enterprise data into the Agent Context
Platform (ACP) at scale.

## What this is

A working monorepo customers fork to load their data into ACP. It implements
the partition-fan-out pattern, a streaming extractor with COPY-to-staging,
and the dual-mode promote/drain path (set-based SQL for bulk backfills, ACP
`/bulk` for ongoing sync). One real Postgres connector and one Snowflake
skeleton ship in the box.

It is **not** a turnkey ETL tool, a managed service, or a UI. It's source
you fork: swap the connector, swap the transformer, deploy. The patterns
matter more than the specific code — the goal is for a senior data
engineer to read the repo, understand the architecture in 10 minutes, and
write their own pipeline in a day.

## Architecture

```
Trigger Lambda
  │  generates batch_id, lists partitions, writes pipeline_runs
  ▼
SQS partition queue (one message per partition)
  ▼
Extractor (ECS Fargate for initial, Lambda for incremental)
  │  streams source rows → transformer (pure fn) → COPY into acp_staging.<pipeline>
  ▼
acp_staging.<pipeline>  (UNLOGGED, per-pipeline, in same RDS as ACP)
  ▼
Promote (mode-dependent)
  │  initial:     SQL function inside Postgres — set-based, fast, one change_log marker
  │  incremental: drainer Lambda → POST /v1/objects/bulk — full validation + per-row change_log
  ▼
context_objects + change_log  (ACP-owned)
```

Two modes share the same staging table; rows are tagged `initial` or
`incremental`. The same handler code runs locally (one Node process) or
on AWS (Lambdas + ECS + Step Functions); production wiring is just
different deps for the same functions.

| | Initial | Incremental |
| --- | --- | --- |
| Throughput | 50k+ rows/sec | 5–15k rows/sec |
| Path | Postgres COPY → SQL promote function | Postgres COPY → POST /bulk |
| Per-row change_log | No (one marker per batch) | Yes |
| Validation | Required-field checks | Full ACP /bulk validation |
| Use | One-time backfill, full re-bake | Hourly/daily sync |

See `runbooks/initial-load.md` and `runbooks/incremental-sync.md` for the
full mode contracts.

## Quickstart

Requires Docker, Node 20.x, pnpm 9+.

```bash
docker compose up -d
pnpm install
pnpm seed --scale 0.01                       # 1k customers, ~10k orders
pnpm pipeline:run tpch-customer --mode initial
```

Last log line on success:

```
{"level":"info","msg":"pipeline:run initial complete",
 "extracted":1000,"staged":1000,"inserted":1000,"rejected":0, ...}
```

Verify the load:

```bash
psql postgres://acp:localdev@localhost:5432/acp -c \
  "SELECT COUNT(*) FROM context_objects WHERE subtype='customer';"
#  count
# -------
#   1000
```

Try incremental:

```bash
psql postgres://tpch:localdev@localhost:5433/tpch -c \
  "UPDATE customer SET c_acctbal = c_acctbal + 1 WHERE c_custkey <= 5;"
pnpm pipeline:run tpch-customer --mode incremental
# ... drain complete: rowsUpdated=5, rowsRejected=0
```

Run the integration tests:

```bash
pnpm test:integration
# Tests  14 passed (14)
```

## Repository structure

```
acp-pipeline-reference/
├── README.md                                # this file
├── docker-compose.yml                       # source-pg + acp-pg for local dev
├── migrations/                              # SQL applied to ACP's Postgres
│   ├── 001_staging_template.sql             #   acp_staging schema + helper
│   ├── 002_promote_function.sql             #   set-based initial promote
│   ├── 003_helper_functions.sql             #   jsonb_deep_merge, changed_paths
│   ├── 004_dead_letter.sql                  #   acp_rejected_rows
│   └── 005_pipeline_runs.sql                #   watermark + run tracking
├── local-dev/00_acp_stub.sql                # local-only stub of ACP-owned tables
├── source-data/                             # synthetic TPC-H source
│   ├── tpch-schema.sql
│   ├── seed.ts                              # `pnpm seed --scale ...`
│   └── README.md
├── packages/
│   ├── pipeline-core/                       # types, errors, logger, runtime
│   ├── connector-postgres/                  # production-ready Postgres connector
│   └── connector-snowflake/                 # skeleton + 6-step implementation guide
├── pipelines/
│   └── tpch-customer/                       # reference pipeline
│       ├── transformer.ts                   #   pure source→7-dim mapping
│       ├── pipeline.config.ts               #   wires connector + transformer
│       └── tests/transformer.test.ts
├── tests/integration/                       # end-to-end tests against docker-compose
├── infra/
│   ├── cdk/                                 # AWS CDK app — Lambdas, ECS, SQS, SFN
│   └── stepfunctions/
│       ├── initial-load.asl.json
│       └── incremental-sync.asl.json
├── contexts/customer.yaml                   # example ACP context schema
├── runbooks/                                # six operational guides
└── bin/pipeline-run.ts                      # local CLI: trigger → extract → promote/drain
```

## Adapting to your stack

Three swaps for most adopters:

1. **Source connector.** If you're on Postgres, `connector-postgres` is
   ready. Otherwise, fork `connector-snowflake` (six implementation
   steps in its README) or write your own following
   `runbooks/adding-a-connector.md`. The `SourceConnector` interface is
   five methods.
2. **Transformer.** `pipelines/tpch-customer/transformer.ts` is a pure
   function mapping a denormalized source row to a `StagingRow` with
   the seven ACP context dimensions populated. Replace it with your
   domain mapping — see `runbooks/adding-a-pipeline.md` for the dimension
   conventions.
3. **Context schema.** `contexts/customer.yaml` describes the target
   context type the transformer produces. Replace with your domain's
   schema and load it into your ACP deployment (the pipeline does not
   load context schemas — that's an ACP platform operation).

The CDK stack is parameterized via context (`infra/cdk/cdk.json`); deploy
multiple pipelines by deploying the stack with different `pipelineName`
values.

## Production deployment

`runbooks/deploying.md` walks through first-time CDK deploy. Prerequisites:

* AWS account with CDK bootstrapped
* Existing VPC with private subnets that have NAT egress
* ACP platform deployed and reachable
* Three Secrets Manager secrets created (ACP DB, source DB, ACP API token)

Deploy:

```bash
cd infra/cdk
pnpm install
pnpm exec cdk diff
pnpm exec cdk deploy --all
```

## Versioning and stability

This is a reference, not a product. There are no SLAs, no semantic-version
guarantees, and no upgrade tooling. Customers fork at the commit they
deploy, own their fork, and pull fixes from upstream by hand if they
want them.

Migrations and the `SourceConnector` interface are the most likely
breaking points; both are intentionally small to keep porting cheap.

## License

MIT. See `LICENSE`.
