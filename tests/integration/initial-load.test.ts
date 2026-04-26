import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import type { Pool } from 'pg';

import pipeline from '@acp/pipeline-tpch-customer';

import {
  createAcpPool,
  resetAcp,
  runInitial,
  seedSource,
} from './helpers.js';

const CUSTOMER_COUNT = 200;

const SEVEN_DIMENSIONS = [
  'attributes',
  'measures',
  'actors',
  'temporals',
  'locations',
  'intents',
  'processes',
] as const;

describe('initial-load (end-to-end)', () => {
  let pool: Pool;

  beforeAll(async () => {
    pool = createAcpPool();
    try {
      await resetAcp();
      await seedSource(CUSTOMER_COUNT);
    } catch (cause) {
      throw new Error(
        `initial-load fixture setup failed; check that docker compose is up. Cause: ${(cause as Error).message}`,
      );
    }
  }, 60_000);

  afterAll(async () => {
    await pool.end().catch(() => undefined);
  });

  test('first run inserts every customer with all 7 dimensions populated', async () => {
    const result = await runInitial(pipeline, pool);
    expect(result.extracts.length).toBeGreaterThan(0);
    const totalExtracted = result.extracts.reduce((s, e) => s + e.rowsExtracted, 0);
    const totalStaged = result.extracts.reduce((s, e) => s + e.rowsStaged, 0);
    expect(totalExtracted).toBe(CUSTOMER_COUNT);
    expect(totalStaged).toBe(CUSTOMER_COUNT);
    expect(result.validate.stagedCount).toBe(CUSTOMER_COUNT);
    expect(result.validate.rejectedCount).toBe(0);
    expect(result.promote.insertedCount).toBe(CUSTOMER_COUNT);
    expect(result.promote.rejectedCount).toBe(0);

    const countRes = await pool.query<{ count: string }>(
      `SELECT COUNT(*) AS count FROM context_objects WHERE subtype = 'customer'`,
    );
    expect(Number(countRes.rows[0]?.count ?? 0)).toBe(CUSTOMER_COUNT);

    const sampleRes = await pool.query<{ context: Record<string, Record<string, unknown>> }>(
      `SELECT context FROM context_objects WHERE subtype = 'customer' LIMIT 1`,
    );
    const sample = sampleRes.rows[0]?.context;
    expect(sample).toBeDefined();
    if (sample === undefined) return;
    for (const dim of SEVEN_DIMENSIONS) {
      expect(sample[dim]).toBeDefined();
      expect(Object.keys(sample[dim] ?? {}).length).toBeGreaterThan(0);
    }
  });

  test('change_log has exactly one initial_load entry with correct counts', async () => {
    const res = await pool.query<{ change_type: string; metadata: Record<string, unknown> }>(
      `SELECT change_type, metadata FROM change_log WHERE change_type = 'initial_load'`,
    );
    expect(res.rows.length).toBe(1);
    const meta = res.rows[0]!.metadata;
    expect(meta['pipeline_name']).toBe(pipeline.name);
    expect(Number(meta['inserted_count'])).toBe(CUSTOMER_COUNT);
    expect(Number(meta['rejected_count'])).toBe(0);
  });

  test('re-running initial mode is idempotent (0 new rows inserted)', async () => {
    const before = await pool.query<{ count: string }>(
      `SELECT COUNT(*) AS count FROM context_objects WHERE subtype = 'customer'`,
    );
    const beforeCount = Number(before.rows[0]?.count ?? 0);

    const result = await runInitial(pipeline, pool);
    expect(result.promote.insertedCount).toBe(0);
    expect(result.promote.rejectedCount).toBe(0);

    const after = await pool.query<{ count: string }>(
      `SELECT COUNT(*) AS count FROM context_objects WHERE subtype = 'customer'`,
    );
    expect(Number(after.rows[0]?.count ?? 0)).toBe(beforeCount);
  });

  test('pipeline_runs records both runs as succeeded', async () => {
    const res = await pool.query<{ status: string; rows_inserted: number | null }>(
      `SELECT status, rows_inserted
         FROM pipeline_runs
        WHERE pipeline_name = $1
        ORDER BY started_at`,
      [pipeline.name],
    );
    expect(res.rows.length).toBeGreaterThanOrEqual(2);
    for (const row of res.rows) {
      expect(row.status).toBe('succeeded');
    }
    expect(Number(res.rows[0]!.rows_inserted)).toBe(CUSTOMER_COUNT);
    expect(Number(res.rows[res.rows.length - 1]!.rows_inserted)).toBe(0);
  });
});
