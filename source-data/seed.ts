/**
 * Synthetic TPC-H seed for local-dev source-pg.
 *
 * Usage:
 *   pnpm seed --scale 1.0    # 100k customers (default)
 *   pnpm seed --scale 0.1    # 10k customers
 *   pnpm seed --scale 0.01   # 1k customers (fast iteration)
 *
 * Reproducibility: faker is seeded with a fixed value so re-runs at the
 * same scale produce identical rows. Use --seed <int> to override.
 *
 * Idempotency: TRUNCATEs nation/region/customer/orders before inserting,
 * so the script is safe to re-run. (We TRUNCATE here, not in the pipeline
 * — see CLAUDE.md "things to NOT do" #3.)
 */

import { faker } from '@faker-js/faker';
import { Client } from 'pg';

const REGIONS: ReadonlyArray<{ r_regionkey: number; r_name: string }> = [
  { r_regionkey: 0, r_name: 'AFRICA' },
  { r_regionkey: 1, r_name: 'AMERICA' },
  { r_regionkey: 2, r_name: 'ASIA' },
  { r_regionkey: 3, r_name: 'EUROPE' },
  { r_regionkey: 4, r_name: 'MIDDLE EAST' },
];

const NATIONS: ReadonlyArray<{ n_nationkey: number; n_name: string; n_regionkey: number }> = [
  { n_nationkey: 0,  n_name: 'ALGERIA',        n_regionkey: 0 },
  { n_nationkey: 1,  n_name: 'ARGENTINA',      n_regionkey: 1 },
  { n_nationkey: 2,  n_name: 'BRAZIL',         n_regionkey: 1 },
  { n_nationkey: 3,  n_name: 'CANADA',         n_regionkey: 1 },
  { n_nationkey: 4,  n_name: 'EGYPT',          n_regionkey: 4 },
  { n_nationkey: 5,  n_name: 'ETHIOPIA',       n_regionkey: 0 },
  { n_nationkey: 6,  n_name: 'FRANCE',         n_regionkey: 3 },
  { n_nationkey: 7,  n_name: 'GERMANY',        n_regionkey: 3 },
  { n_nationkey: 8,  n_name: 'INDIA',          n_regionkey: 2 },
  { n_nationkey: 9,  n_name: 'INDONESIA',      n_regionkey: 2 },
  { n_nationkey: 10, n_name: 'IRAN',           n_regionkey: 4 },
  { n_nationkey: 11, n_name: 'IRAQ',           n_regionkey: 4 },
  { n_nationkey: 12, n_name: 'JAPAN',          n_regionkey: 2 },
  { n_nationkey: 13, n_name: 'JORDAN',         n_regionkey: 4 },
  { n_nationkey: 14, n_name: 'KENYA',          n_regionkey: 0 },
  { n_nationkey: 15, n_name: 'MOROCCO',        n_regionkey: 0 },
  { n_nationkey: 16, n_name: 'MOZAMBIQUE',     n_regionkey: 0 },
  { n_nationkey: 17, n_name: 'PERU',           n_regionkey: 1 },
  { n_nationkey: 18, n_name: 'CHINA',          n_regionkey: 2 },
  { n_nationkey: 19, n_name: 'ROMANIA',        n_regionkey: 3 },
  { n_nationkey: 20, n_name: 'SAUDI ARABIA',   n_regionkey: 4 },
  { n_nationkey: 21, n_name: 'VIETNAM',        n_regionkey: 2 },
  { n_nationkey: 22, n_name: 'RUSSIA',         n_regionkey: 3 },
  { n_nationkey: 23, n_name: 'UNITED KINGDOM', n_regionkey: 3 },
  { n_nationkey: 24, n_name: 'UNITED STATES',  n_regionkey: 1 },
];

const MARKET_SEGMENTS = ['AUTOMOBILE', 'BUILDING', 'FURNITURE', 'HOUSEHOLD', 'MACHINERY'];
const ORDER_PRIORITIES = ['1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED', '5-LOW'];
const ORDER_STATUSES = ['O', 'F', 'P'];

const BASE_CUSTOMERS = 100_000;
const CUSTOMER_INSERT_BATCH = 500;
const ORDER_INSERT_BATCH = 500;
const SPREAD_DAYS = 30;

interface CliArgs {
  scale: number;
  seed: number;
  url: string;
}

function parseArgs(argv: ReadonlyArray<string>): CliArgs {
  let scale = 1;
  let seed = 42;
  const url =
    process.env['SOURCE_PG_URL'] ?? 'postgres://tpch:localdev@localhost:5433/tpch';
  for (let i = 0; i < argv.length; i++) {
    const flag = argv[i];
    const next = argv[i + 1];
    if (flag === '--scale' && next !== undefined) {
      const parsed = Number(next);
      if (!Number.isFinite(parsed) || parsed <= 0) {
        throw new Error(`--scale must be a positive number; got "${next}"`);
      }
      scale = parsed;
      i++;
    } else if (flag === '--seed' && next !== undefined) {
      const parsed = Number(next);
      if (!Number.isInteger(parsed)) {
        throw new Error(`--seed must be an integer; got "${next}"`);
      }
      seed = parsed;
      i++;
    } else if (flag === '--help' || flag === '-h') {
      printUsage();
      process.exit(0);
    }
  }
  return { scale, seed, url };
}

function printUsage(): void {
  process.stdout.write(
    [
      'Usage: pnpm seed [--scale <n>] [--seed <int>]',
      '  --scale <n>   Customer count multiplier; 1.0 = 100k customers (default 1).',
      '  --seed <int>  Faker PRNG seed for reproducibility (default 42).',
      'Connection: SOURCE_PG_URL env var (default postgres://tpch:localdev@localhost:5433/tpch).',
      '',
    ].join('\n'),
  );
}

async function run(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  faker.seed(args.seed);

  const customerCount = Math.max(1, Math.round(BASE_CUSTOMERS * args.scale));
  const startedAt = Date.now();

  const client = new Client({ connectionString: args.url });
  await client.connect();
  log(`connected to ${args.url}`);

  try {
    await truncateAll(client);
    await insertRegions(client);
    await insertNations(client);
    await insertCustomers(client, customerCount);
    const orderCount = await insertOrders(client, customerCount);
    const elapsed = ((Date.now() - startedAt) / 1000).toFixed(1);
    log(
      `seeded ${customerCount.toLocaleString()} customers, ` +
        `${orderCount.toLocaleString()} orders in ${elapsed}s`,
    );
  } finally {
    await client.end();
  }
}

async function truncateAll(client: Client): Promise<void> {
  log('truncating existing seed data');
  await client.query('TRUNCATE orders, customer, nation, region RESTART IDENTITY CASCADE');
}

async function insertRegions(client: Client): Promise<void> {
  await client.query(
    'INSERT INTO region (r_regionkey, r_name) SELECT * FROM unnest($1::int[], $2::text[])',
    [REGIONS.map((r) => r.r_regionkey), REGIONS.map((r) => r.r_name)],
  );
  log(`inserted ${REGIONS.length} regions`);
}

async function insertNations(client: Client): Promise<void> {
  await client.query(
    'INSERT INTO nation (n_nationkey, n_name, n_regionkey) ' +
      'SELECT * FROM unnest($1::int[], $2::text[], $3::int[])',
    [
      NATIONS.map((n) => n.n_nationkey),
      NATIONS.map((n) => n.n_name),
      NATIONS.map((n) => n.n_regionkey),
    ],
  );
  log(`inserted ${NATIONS.length} nations`);
}

async function insertCustomers(client: Client, count: number): Promise<void> {
  const cols = [
    'c_custkey',
    'c_name',
    'c_address',
    'c_nationkey',
    'c_phone',
    'c_acctbal',
    'c_mktsegment',
    'c_comment',
    'updated_at',
  ];

  for (let offset = 0; offset < count; offset += CUSTOMER_INSERT_BATCH) {
    const batchSize = Math.min(CUSTOMER_INSERT_BATCH, count - offset);
    const values: unknown[] = [];
    const tuples: string[] = [];
    for (let i = 0; i < batchSize; i++) {
      const custkey = offset + i + 1;
      const nationkey = NATIONS[faker.number.int({ min: 0, max: NATIONS.length - 1 })]!.n_nationkey;
      const tuple: unknown[] = [
        custkey,
        `Customer#${String(custkey).padStart(9, '0')}`,
        faker.location.streetAddress({ useFullAddress: true }).slice(0, 40),
        nationkey,
        faker.phone.number().slice(0, 15),
        Number(faker.finance.amount({ min: -999, max: 9999, dec: 2 })),
        MARKET_SEGMENTS[faker.number.int({ min: 0, max: MARKET_SEGMENTS.length - 1 })],
        faker.lorem.sentence({ min: 4, max: 12 }).slice(0, 117),
        randomTimestamp(),
      ];
      const start = values.length;
      values.push(...tuple);
      tuples.push(
        '(' + tuple.map((_, j) => `$${start + j + 1}`).join(',') + ')',
      );
    }
    await client.query(
      `INSERT INTO customer (${cols.join(',')}) VALUES ${tuples.join(',')}`,
      values,
    );
    if ((offset + batchSize) % 10_000 === 0 || offset + batchSize === count) {
      log(`inserted ${(offset + batchSize).toLocaleString()} / ${count.toLocaleString()} customers`);
    }
  }
}

async function insertOrders(client: Client, customerCount: number): Promise<number> {
  const cols = [
    'o_orderkey',
    'o_custkey',
    'o_orderstatus',
    'o_totalprice',
    'o_orderdate',
    'o_orderpriority',
    'updated_at',
  ];

  let orderkey = 1;
  let total = 0;
  let buffered: unknown[][] = [];

  const flush = async (): Promise<void> => {
    if (buffered.length === 0) return;
    const values: unknown[] = [];
    const tuples: string[] = [];
    for (const tuple of buffered) {
      const start = values.length;
      values.push(...tuple);
      tuples.push('(' + tuple.map((_, j) => `$${start + j + 1}`).join(',') + ')');
    }
    await client.query(
      `INSERT INTO orders (${cols.join(',')}) VALUES ${tuples.join(',')}`,
      values,
    );
    total += buffered.length;
    buffered = [];
  };

  for (let custkey = 1; custkey <= customerCount; custkey++) {
    const orders = faker.number.int({ min: 1, max: 20 });
    for (let i = 0; i < orders; i++) {
      const ts = randomTimestamp();
      buffered.push([
        orderkey++,
        custkey,
        ORDER_STATUSES[faker.number.int({ min: 0, max: ORDER_STATUSES.length - 1 })],
        Number(faker.finance.amount({ min: 50, max: 500_000, dec: 2 })),
        ts.slice(0, 10),
        ORDER_PRIORITIES[faker.number.int({ min: 0, max: ORDER_PRIORITIES.length - 1 })],
        ts,
      ]);
      if (buffered.length >= ORDER_INSERT_BATCH) await flush();
    }
    if (custkey % 10_000 === 0) {
      await flush();
      log(`inserted orders for ${custkey.toLocaleString()} customers (${total.toLocaleString()} orders so far)`);
    }
  }
  await flush();
  log(`inserted ${total.toLocaleString()} orders`);
  return total;
}

function randomTimestamp(): string {
  const now = Date.now();
  const offsetMs = faker.number.int({ min: 0, max: SPREAD_DAYS * 24 * 60 * 60 * 1000 });
  return new Date(now - offsetMs).toISOString();
}

function log(message: string): void {
  process.stdout.write(`[seed] ${message}\n`);
}

run().catch((err: unknown) => {
  process.stderr.write(`[seed] failed: ${(err as Error).stack ?? String(err)}\n`);
  process.exit(1);
});
