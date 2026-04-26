import { describe, expect, test } from 'vitest';

import {
  TPCH_CUSTOMER_SOURCE,
  TPCH_CUSTOMER_SUBTYPE,
  TPCH_CUSTOMER_TARGET,
  tpchCustomerTransformer,
} from '../transformer.js';
import type { TpchCustomerSourceRow } from '../transformer.js';

const SEVEN_DIMENSIONS = [
  'attributes',
  'measures',
  'actors',
  'temporals',
  'locations',
  'intents',
  'processes',
] as const;

function activeRow(overrides: Partial<TpchCustomerSourceRow> = {}): TpchCustomerSourceRow {
  return {
    c_custkey: 42,
    c_name: 'Customer#000000042',
    c_address: '17 Example Lane',
    c_nationkey: 24,
    c_phone: '555-0042',
    c_acctbal: '1234.56',
    c_mktsegment: 'BUILDING',
    c_comment: 'frequent buyer',
    updated_at: '2026-04-01T12:00:00Z',
    n_nationkey: 24,
    n_name: 'UNITED STATES',
    n_regionkey: 1,
    r_name: 'AMERICA',
    order_count: 7,
    total_orderprice: '83451.10',
    last_order_date: '2026-03-30',
    ...overrides,
  };
}

describe('tpchCustomerTransformer', () => {
  test('exposes the configured target context type', () => {
    expect(tpchCustomerTransformer.targetContextType).toBe(TPCH_CUSTOMER_TARGET);
  });

  test('maps active customer row into all 7 dimensions', () => {
    const staging = tpchCustomerTransformer.toStaging(activeRow());

    expect(staging.subtype).toBe(TPCH_CUSTOMER_SUBTYPE);
    expect(staging.source).toBe(TPCH_CUSTOMER_SOURCE);
    expect(staging.source_ref).toBe('42');
    expect(staging.canonical_name).toBe('Customer#000000042');

    for (const dim of SEVEN_DIMENSIONS) {
      expect(staging.context[dim]).toBeDefined();
      expect(Object.keys(staging.context[dim]).length).toBeGreaterThan(0);
    }
  });

  test('parses decimals and aggregates as JSON-safe numbers', () => {
    const { context } = tpchCustomerTransformer.toStaging(activeRow());
    expect(context.measures['account_balance_usd']).toBe(1234.56);
    expect(context.measures['order_count']).toBe(7);
    expect(context.measures['lifetime_spend_usd']).toBe(83451.1);
    expect(context.processes['activity_state']).toBe('active');
    expect(context.processes['order_count']).toBe(7);
  });

  test('normalizes timestamps to ISO 8601', () => {
    const { context } = tpchCustomerTransformer.toStaging(
      activeRow({
        updated_at: new Date('2026-04-01T12:00:00Z'),
        last_order_date: new Date('2026-03-30T00:00:00Z'),
      }),
    );
    expect(context.temporals['source_updated_at']).toBe('2026-04-01T12:00:00.000Z');
    expect(context.temporals['last_order_at']).toBe('2026-03-30T00:00:00.000Z');
  });

  test('maps customers with zero orders as prospects', () => {
    const { context } = tpchCustomerTransformer.toStaging(
      activeRow({
        order_count: 0,
        total_orderprice: null,
        last_order_date: null,
      }),
    );
    expect(context.processes['activity_state']).toBe('prospect');
    expect(context.processes['order_count']).toBe(0);
    expect(context.measures['lifetime_spend_usd']).toBe(0);
    expect(context.temporals['last_order_at']).toBeNull();
  });

  test('threads geography from joined nation/region', () => {
    const { context } = tpchCustomerTransformer.toStaging(activeRow());
    expect(context.locations).toMatchObject({
      nation_key: 24,
      nation: 'UNITED STATES',
      region: 'AMERICA',
    });
    expect(context.intents['market_segment']).toBe('BUILDING');
  });

  test('throws TransformError when c_name is missing', () => {
    expect(() =>
      tpchCustomerTransformer.toStaging(activeRow({ c_name: '' })),
    ).toThrow(/c_name/);
  });
});
