import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import { Client } from 'pg';
import type { Pool } from 'pg';

import {
  BulkClient,
  WatermarkStore,
  runDrainer,
  startBulkStub,
} from '@acp/pipeline-core';
import type { BulkStubHandle } from '@acp/pipeline-core';
import pipeline from '@acp/pipeline-tpch-customer';

import {
  createAcpPool,
  resetAcp,
  runIncremental,
  runInitial,
  seedSource,
  SOURCE_PG_URL,
} from './helpers.js';

const CUSTOMER_COUNT = 200;
const STUB_PORT = 3110;

describe('incremental-sync (end-to-end)', () => {
  let pool: Pool;

  beforeAll(async () => {
    pool = createAcpPool();
    try {
      await resetAcp();
      await seedSource(CUSTOMER_COUNT);
      // Bring context_objects to a fully promoted state so incremental has a
      // realistic starting point.
      await runInitial(pipeline, pool);
    } catch (cause) {
      throw new Error(
        `incremental-sync setup failed; check docker compose. Cause: ${(cause as Error).message}`,
      );
    }
  }, 60_000);

  afterAll(async () => {
    await pool.end().catch(() => undefined);
  });

  test('incremental detects only the rows changed in source and updates them', async () => {
    const updatedKeys = await touchSourceRows(5);
    const result = await runIncremental(pipeline, pool, { stubPort: STUB_PORT });
    try {
      expect(result.extracts.reduce((s, e) => s + e.rowsExtracted, 0)).toBe(5);
      expect(result.drain.rowsPosted).toBe(5);
      expect(result.drain.rowsUpdated).toBe(5);
      expect(result.drain.rowsCreated).toBe(0);
      expect(result.drain.rowsRejected).toBe(0);

      const refs = updatedKeys.map((k) => String(k));
      const objs = await pool.query<{ source_ref: string; context: Record<string, Record<string, unknown>> }>(
        `SELECT source_ref, context
           FROM context_objects
          WHERE subtype = 'customer' AND source_ref = ANY($1::text[])`,
        [refs],
      );
      expect(objs.rows.length).toBe(5);
      // The bumped balance should be reflected in measures.
      for (const row of objs.rows) {
        const balance = Number(row.context['measures']?.['account_balance_usd']);
        expect(Number.isFinite(balance)).toBe(true);
      }

      const log = await pool.query<{ metadata: { changed_paths: string[]; canonical_name: string } }>(
        `SELECT metadata FROM change_log
          WHERE change_type = 'updated'
          ORDER BY log_id DESC
          LIMIT 5`,
      );
      expect(log.rows.length).toBe(5);
      for (const entry of log.rows) {
        expect(entry.metadata.changed_paths).toEqual(
          expect.arrayContaining(['measures.account_balance_usd', 'temporals.source_updated_at']),
        );
      }
    } finally {
      await result.stub.close();
    }
  });

  test('drainer marks a forced bad row as rejected without crashing', async () => {
    const insert = await pool.query<{ batch_id: string }>(
      `INSERT INTO acp_staging.tpch_customer
         (subtype, source, source_ref, canonical_name, context, batch_id, partition_key, mode)
       VALUES ('customer', 'tpch', 'bad-row-1', '',
               '{"attributes":{},"measures":{},"actors":{},"temporals":{},"locations":{},"intents":{},"processes":{}}'::jsonb,
               gen_random_uuid(), 'manual', 'incremental')
       RETURNING batch_id::text AS batch_id`,
    );
    const batchId = insert.rows[0]!.batch_id;

    const stub: BulkStubHandle = await startBulkStub({ pool, port: STUB_PORT + 1 });
    try {
      const bulkClient = new BulkClient({ apiUrl: stub.url });
      const drainResult = await runDrainer(
        { pipelineName: pipeline.name, batchId },
        {
          acpPool: pool,
          bulkClient,
          bulkBatchSize: 50,
          watermarkStore: new WatermarkStore(pool),
        },
      );
      expect(drainResult.rowsPosted).toBe(1);
      expect(drainResult.rowsRejected).toBe(1);
      expect(drainResult.rowsUpdated).toBe(0);

      const stagingRes = await pool.query<{ last_error: string; resolved: boolean }>(
        `SELECT last_error, promoted_at IS NOT NULL AS resolved
           FROM acp_staging.tpch_customer
          WHERE batch_id = $1::uuid`,
        [batchId],
      );
      expect(stagingRes.rows.length).toBe(1);
      expect(stagingRes.rows[0]!.resolved).toBe(true);
      expect(stagingRes.rows[0]!.last_error).toMatch(/canonical_name/);
    } finally {
      await stub.close();
    }
  });

  test('a follow-up incremental with no source changes posts zero rows', async () => {
    const result = await runIncremental(pipeline, pool, { stubPort: STUB_PORT + 2 });
    try {
      expect(result.extracts.reduce((s, e) => s + e.rowsExtracted, 0)).toBe(0);
      expect(result.drain.rowsPosted).toBe(0);
    } finally {
      await result.stub.close();
    }
  });
});

async function touchSourceRows(n: number): Promise<number[]> {
  const client = new Client({ connectionString: SOURCE_PG_URL });
  await client.connect();
  try {
    const res = await client.query<{ c_custkey: string }>(
      `UPDATE customer
          SET c_acctbal = c_acctbal + 1
        WHERE c_custkey IN (SELECT c_custkey FROM customer ORDER BY c_custkey LIMIT $1)
        RETURNING c_custkey::text AS c_custkey`,
      [n],
    );
    return res.rows.map((r) => Number(r.c_custkey));
  } finally {
    await client.end();
  }
}
