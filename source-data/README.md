# source-data

Synthetic TPC-H source for local development. The schema is the
TPC-H subset used by the reference pipeline: `region`, `nation`,
`customer`, `orders`. Column names follow the standard `c_*`/`o_*`/`n_*`
conventions.

## What gets created

| Table     | Rows at scale 1.0 | Notes                                    |
| --------- | ----------------- | ---------------------------------------- |
| region    | 5                 | Hardcoded TPC-H regions                  |
| nation    | 25                | Hardcoded TPC-H nations                  |
| customer  | 100,000           | Synthetic via @faker-js/faker            |
| orders    | ~1,050,000        | 1–20 per customer, uniform               |

`updated_at` on `customer` and `orders` is spread uniformly across the
last 30 days so incremental sync (filtering on `updated_at > $since`)
has realistic row distribution.

## Seeding

The schema lives at `source-data/tpch-schema.sql` and is mounted into
the `source-pg` container by `docker-compose.yml`, so the tables exist
as soon as the container is healthy.

To populate them with synthetic rows, run from the repo root:

```bash
pnpm seed --scale 0.01    # 1k customers, ~10k orders, fast iteration
pnpm seed --scale 0.1     # 10k customers, ~100k orders
pnpm seed --scale 1.0     # 100k customers, ~1M orders (default)
pnpm seed --scale 10      # 1M customers, ~10M orders
```

Each run TRUNCATEs the four tables (with `CASCADE`) before inserting,
so re-running at a different scale produces a clean dataset rather than
appending. Output is reproducible: faker is seeded with `--seed 42` by
default; pass `--seed <int>` to vary.

## Connection

The script reads `SOURCE_PG_URL` if set, otherwise defaults to
`postgres://tpch:localdev@localhost:5433/tpch` — the credentials and
port published by `docker-compose.yml`.
