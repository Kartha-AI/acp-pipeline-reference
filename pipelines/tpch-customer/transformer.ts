import { TransformError } from '@acp/pipeline-core';
import type { RowTransformer, StagingRow } from '@acp/pipeline-core';

/**
 * Shape of one row emitted by the extraction query in `pipeline.config.ts`.
 * Postgres returns DECIMAL as string and BIGINT as string-or-bigint depending
 * on the column type and pg's parser config — the transformer normalizes
 * everything into JSON-safe primitives.
 *
 * The index signature lets this satisfy `SourceRow` (which is
 * `Record<string, unknown>` by definition) so it can plug directly into
 * `RowTransformer<TpchCustomerSourceRow>`.
 */
export interface TpchCustomerSourceRow {
  [key: string]: unknown;
  c_custkey: number | bigint | string;
  c_name: string;
  c_address: string | null;
  c_nationkey: number;
  c_phone: string | null;
  c_acctbal: number | string;
  c_mktsegment: string | null;
  c_comment: string | null;
  updated_at: Date | string;
  n_nationkey: number;
  n_name: string;
  n_regionkey: number;
  r_name: string;
  order_count: number | string;
  total_orderprice: number | string | null;
  last_order_date: Date | string | null;
}

export const TPCH_CUSTOMER_SOURCE = 'tpch';
export const TPCH_CUSTOMER_SUBTYPE = 'customer';
export const TPCH_CUSTOMER_TARGET = 'customer';

export const tpchCustomerTransformer: RowTransformer<TpchCustomerSourceRow> = {
  targetContextType: TPCH_CUSTOMER_TARGET,

  toStaging(row: TpchCustomerSourceRow): StagingRow {
    if (row.c_custkey === undefined || row.c_custkey === null) {
      throw new TransformError('TPC-H row is missing c_custkey', {
        rowSnapshot: row as unknown as Record<string, unknown>,
      });
    }
    if (typeof row.c_name !== 'string' || row.c_name.length === 0) {
      throw new TransformError('TPC-H row is missing c_name', {
        sourceRef: String(row.c_custkey),
        rowSnapshot: row as unknown as Record<string, unknown>,
      });
    }

    const sourceRef = String(row.c_custkey);
    const acctBalanceUsd = toNumber(row.c_acctbal);
    const orderCount = Math.trunc(toNumber(row.order_count));
    const lifetimeSpendUsd = toNumber(row.total_orderprice ?? 0);
    const sourceUpdatedAt = toIsoString(row.updated_at);
    const lastOrderAt =
      row.last_order_date === null || row.last_order_date === undefined
        ? null
        : toIsoString(row.last_order_date);

    return {
      subtype: TPCH_CUSTOMER_SUBTYPE,
      source: TPCH_CUSTOMER_SOURCE,
      source_ref: sourceRef,
      // canonical_name is the identity-merge key in ACP; for TPC-H the
      // c_name field is unique by construction (Customer#000000001 etc.).
      canonical_name: row.c_name,
      context: {
        attributes: {
          custkey: sourceRef,
          phone: row.c_phone,
          street_address: row.c_address,
          comment: row.c_comment,
        },
        measures: {
          account_balance_usd: acctBalanceUsd,
          order_count: orderCount,
          lifetime_spend_usd: lifetimeSpendUsd,
        },
        actors: {
          customer_name: row.c_name,
        },
        temporals: {
          source_updated_at: sourceUpdatedAt,
          last_order_at: lastOrderAt,
        },
        locations: {
          nation_key: row.n_nationkey,
          nation: row.n_name,
          region: row.r_name,
        },
        intents: {
          market_segment: row.c_mktsegment,
        },
        processes: {
          activity_state: orderCount > 0 ? 'active' : 'prospect',
          order_count: orderCount,
        },
      },
    };
  },
};

function toNumber(value: number | bigint | string | null | undefined): number {
  if (value === null || value === undefined) return 0;
  if (typeof value === 'number') return value;
  if (typeof value === 'bigint') return Number(value);
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw new TransformError(`Expected numeric value, got "${value}"`, {});
  }
  return parsed;
}

function toIsoString(value: Date | string): string {
  if (value instanceof Date) return value.toISOString();
  // pg returns TIMESTAMPTZ as a Date object by default; this branch covers
  // string-typed columns and the case where pg's type parsers are disabled.
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    throw new TransformError(`Expected timestamp, got "${value}"`, {});
  }
  return parsed.toISOString();
}
