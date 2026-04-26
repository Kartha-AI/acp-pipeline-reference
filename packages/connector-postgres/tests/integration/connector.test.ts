import { afterAll, beforeAll, describe, expect, test } from 'vitest';

import { postgresConnector } from '../../src/index.js';
import type { PostgresConnector } from '../../src/index.js';

const SOURCE_URL =
  process.env['SOURCE_PG_URL'] ?? 'postgres://tpch:localdev@localhost:5433/tpch';

const QUERY = `
  SELECT c.c_custkey, c.c_name, c.c_nationkey
    FROM customer c
   WHERE {{partition_filter}}
     {{since}}
   ORDER BY c.c_custkey
`;

describe('PostgresConnector against docker-compose source-pg', () => {
  let connector: PostgresConnector;

  beforeAll(async () => {
    connector = postgresConnector({
      pipelineName: 'tpch_customer_test',
      auth: { kind: 'connectionString', connectionString: SOURCE_URL },
      query: QUERY,
      partitions: { kind: 'natural', table: 'customer', column: 'c_nationkey' },
      watermark: { table: 'customer', column: 'updated_at' },
      cursorBatchSize: 100,
    });
    try {
      await connector.connect();
    } catch (cause) {
      throw new Error(
        `Could not reach source-pg at ${SOURCE_URL}. ` +
          `Run \`docker compose up -d && pnpm seed --scale 0.01\` from the repo root before running integration tests. ` +
          `Underlying error: ${(cause as Error).message}`,
      );
    }
  }, 30_000);

  afterAll(async () => {
    if (connector !== undefined) await connector.disconnect();
  });

  test('listPartitions returns one entry per nation present in the seed', async () => {
    const partitions = await connector.listPartitions();
    expect(partitions.length).toBeGreaterThan(0);
    expect(partitions.length).toBeLessThanOrEqual(25);
    expect(partitions[0]?.id).toMatch(/^c_nationkey=/);
    const decoded = partitions[0]?.sourceFilter as Record<string, unknown>;
    expect(decoded['kind']).toBe('natural');
    expect(decoded['column']).toBe('c_nationkey');
  });

  test('extractPartition streams rows for a partition', async () => {
    const partitions = await connector.listPartitions();
    const target = partitions[0];
    expect(target).toBeDefined();
    if (target === undefined) return;

    let count = 0;
    let lastCustkey: number | bigint | string | undefined;
    for await (const row of connector.extractPartition(target)) {
      count++;
      expect(row['c_custkey']).toBeDefined();
      expect(row['c_nationkey']).toBeDefined();
      lastCustkey = row['c_custkey'] as number | bigint | string;
      if (count >= 50) break;
    }
    expect(count).toBeGreaterThan(0);
    expect(lastCustkey).toBeDefined();
  });

  test('listPartitions filters by `since` for incremental discovery', async () => {
    // A future watermark should prune all partitions.
    const future = new Date(Date.now() + 1000 * 60 * 60 * 24 * 365).toISOString();
    const partitions = await connector.listPartitions(future);
    expect(partitions).toEqual([]);
  });

  test('getCurrentWatermark returns a non-empty ISO-ish string', async () => {
    const wm = await connector.getCurrentWatermark();
    expect(typeof wm).toBe('string');
    expect(wm.length).toBeGreaterThan(0);
  });
});
