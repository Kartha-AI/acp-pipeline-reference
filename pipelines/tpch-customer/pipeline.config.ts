import { definePipeline } from '@acp/pipeline-core';
import type { PipelineConfig } from '@acp/pipeline-core';
import { postgresConnector } from '@acp/connector-postgres';

import { tpchCustomerTransformer } from './transformer.js';

const SOURCE_PG_URL =
  process.env['SOURCE_PG_URL'] ?? 'postgres://tpch:localdev@localhost:5433/tpch';

/**
 * Joins customer with nation, region, and per-customer order aggregates so
 * the transformer sees one denormalized row per customer.
 *
 * The `LEFT JOIN` on the orders aggregate is intentional: customers with
 * zero orders should still flow through (transformer flags them as
 * 'prospect'). The `COALESCE`s keep the row shape uniform.
 *
 * `{{partition_filter}}` is substituted with `c_nationkey = $1` (one
 * partition per nation, 25 total at full scale). `{{since}}` is replaced
 * with `AND updated_at > $2` for incremental runs and dropped for initial.
 */
const EXTRACTION_QUERY = `
  SELECT
    c.c_custkey,
    c.c_name,
    c.c_address,
    c.c_nationkey,
    c.c_phone,
    c.c_acctbal,
    c.c_mktsegment,
    c.c_comment,
    c.updated_at,
    n.n_nationkey,
    n.n_name,
    n.n_regionkey,
    r.r_name,
    COALESCE(o.order_count, 0)        AS order_count,
    COALESCE(o.total_orderprice, 0)   AS total_orderprice,
    o.last_order_date                 AS last_order_date
  FROM customer c
  JOIN nation n ON c.c_nationkey = n.n_nationkey
  JOIN region r ON n.n_regionkey = r.r_regionkey
  LEFT JOIN (
    SELECT
      o_custkey,
      COUNT(*)::INTEGER          AS order_count,
      SUM(o_totalprice)          AS total_orderprice,
      MAX(o_orderdate)           AS last_order_date
    FROM orders
    GROUP BY o_custkey
  ) o ON o.o_custkey = c.c_custkey
  WHERE {{partition_filter}}
    {{since}}
`;

export const pipeline: PipelineConfig = definePipeline({
  name: 'tpch_customer',
  target: 'customer',
  transformer: tpchCustomerTransformer,
  source: postgresConnector({
    pipelineName: 'tpch_customer',
    auth: { kind: 'connectionString', connectionString: SOURCE_PG_URL },
    query: EXTRACTION_QUERY,
    partitions: {
      kind: 'natural',
      table: 'customer',
      column: 'c_nationkey',
    },
    watermark: {
      table: 'customer',
      column: 'updated_at',
    },
    cursorBatchSize: 1000,
    poolSize: 4,
  }),
  initial: {
    estimatedTotalRows: 100_000,
    extractorConcurrency: 5,
  },
  incremental: {
    bulkBatchSize: 500,
    drainConcurrency: 4,
  },
});

export default pipeline;
