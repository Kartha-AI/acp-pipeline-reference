import { faker } from '@faker-js/faker';
import { Client } from 'pg';
import { Pool } from 'pg';
import type { Pool as PgPool } from 'pg';

import {
  BulkClient,
  WatermarkStore,
  runDrainer,
  runExtractor,
  runPromote,
  runTrigger,
  runValidateStaging,
  startBulkStub,
} from '@acp/pipeline-core';
import type {
  BulkStubHandle,
  DrainResult,
  ExtractorResult,
  Logger,
  PipelineConfig,
  PromoteResult,
  TriggerOutput,
  ValidateResult,
} from '@acp/pipeline-core';

export const SOURCE_PG_URL =
  process.env['SOURCE_PG_URL'] ?? 'postgres://tpch:localdev@localhost:5433/tpch';
export const ACP_PG_URL =
  process.env['ACP_PG_URL'] ?? 'postgres://acp:localdev@localhost:5432/acp';

const REGIONS = [
  { r_regionkey: 0, r_name: 'AFRICA' },
  { r_regionkey: 1, r_name: 'AMERICA' },
  { r_regionkey: 2, r_name: 'ASIA' },
  { r_regionkey: 3, r_name: 'EUROPE' },
  { r_regionkey: 4, r_name: 'MIDDLE EAST' },
];

const NATIONS = [
  { n_nationkey: 0, n_name: 'ALGERIA', n_regionkey: 0 },
  { n_nationkey: 1, n_name: 'ARGENTINA', n_regionkey: 1 },
  { n_nationkey: 6, n_name: 'FRANCE', n_regionkey: 3 },
  { n_nationkey: 12, n_name: 'JAPAN', n_regionkey: 2 },
  { n_nationkey: 18, n_name: 'CHINA', n_regionkey: 2 },
  { n_nationkey: 24, n_name: 'UNITED STATES', n_regionkey: 1 },
];

const MARKET_SEGMENTS = ['AUTOMOBILE', 'BUILDING', 'FURNITURE', 'HOUSEHOLD', 'MACHINERY'];

export interface SeedResult {
  customerCount: number;
  orderCount: number;
}

export async function seedSource(
  customerCount: number,
  seed = 4242,
): Promise<SeedResult> {
  faker.seed(seed);
  const client = new Client({ connectionString: SOURCE_PG_URL });
  await client.connect();
  try {
    await client.query('TRUNCATE orders, customer, nation, region RESTART IDENTITY CASCADE');
    await client.query(
      'INSERT INTO region (r_regionkey, r_name) SELECT * FROM unnest($1::int[], $2::text[])',
      [REGIONS.map((r) => r.r_regionkey), REGIONS.map((r) => r.r_name)],
    );
    await client.query(
      'INSERT INTO nation (n_nationkey, n_name, n_regionkey) SELECT * FROM unnest($1::int[], $2::text[], $3::int[])',
      [
        NATIONS.map((n) => n.n_nationkey),
        NATIONS.map((n) => n.n_name),
        NATIONS.map((n) => n.n_regionkey),
      ],
    );

    let orderkey = 1;
    let totalOrders = 0;
    for (let custkey = 1; custkey <= customerCount; custkey++) {
      const nation = NATIONS[faker.number.int({ min: 0, max: NATIONS.length - 1 })]!;
      const updated = isoMinusDays(faker.number.int({ min: 0, max: 30 }));
      await client.query(
        `INSERT INTO customer (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
        [
          custkey,
          `Customer#${String(custkey).padStart(9, '0')}`,
          faker.location.streetAddress().slice(0, 40),
          nation.n_nationkey,
          faker.phone.number().slice(0, 15),
          Number(faker.finance.amount({ min: -500, max: 9999, dec: 2 })),
          MARKET_SEGMENTS[faker.number.int({ min: 0, max: MARKET_SEGMENTS.length - 1 })],
          faker.lorem.sentence({ min: 3, max: 8 }).slice(0, 117),
          updated,
        ],
      );
      const orders = faker.number.int({ min: 1, max: 5 });
      for (let i = 0; i < orders; i++) {
        const orderUpdated = isoMinusDays(faker.number.int({ min: 0, max: 30 }));
        await client.query(
          `INSERT INTO orders (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, updated_at)
           VALUES ($1, $2, 'O', $3, $4, '3-MEDIUM', $5)`,
          [
            orderkey++,
            custkey,
            Number(faker.finance.amount({ min: 50, max: 5000, dec: 2 })),
            orderUpdated.slice(0, 10),
            orderUpdated,
          ],
        );
        totalOrders++;
      }
    }
    return { customerCount, orderCount: totalOrders };
  } finally {
    await client.end();
  }
}

export async function resetAcp(): Promise<void> {
  const client = new Client({ connectionString: ACP_PG_URL });
  await client.connect();
  try {
    // Order matters: change_log first (FK-free), then context_objects, then
    // pipeline_runs, then truncate any per-pipeline staging tables we know
    // about.
    await client.query('TRUNCATE change_log RESTART IDENTITY');
    await client.query('TRUNCATE context_objects RESTART IDENTITY CASCADE');
    await client.query('TRUNCATE pipeline_runs');
    await client.query('TRUNCATE acp_rejected_rows');
    const stagingTables = await client.query<{ table_name: string }>(
      `SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'acp_staging' AND table_name <> '_template'`,
    );
    for (const { table_name } of stagingTables.rows) {
      await client.query(`TRUNCATE acp_staging."${table_name.replace(/"/g, '""')}"`);
    }
  } finally {
    await client.end();
  }
}

export function createAcpPool(): PgPool {
  return new Pool({ connectionString: ACP_PG_URL, max: 4 });
}

export interface RunInitialResult {
  trigger: TriggerOutput;
  extracts: ExtractorResult[];
  validate: ValidateResult;
  promote: PromoteResult;
}

export async function runInitial(
  pipeline: PipelineConfig,
  pool: PgPool,
  logger?: Logger,
): Promise<RunInitialResult> {
  const watermarkStore = new WatermarkStore(pool);
  const triggerArgs = logger !== undefined ? { logger } : {};
  const trigger = await runTrigger(
    { pipelineName: pipeline.name, mode: 'initial' },
    { pipeline, acpPool: pool, watermarkStore, ...triggerArgs },
  );
  const extracts: ExtractorResult[] = [];
  for (const partition of trigger.partitions) {
    const extractArgs = logger !== undefined ? { logger } : {};
    extracts.push(
      await runExtractor(
        {
          pipelineName: pipeline.name,
          mode: 'initial',
          batchId: trigger.batchId,
          partition,
        },
        { pipeline, acpPool: pool, ...extractArgs },
      ),
    );
  }
  const validate = await runValidateStaging(
    { pipelineName: pipeline.name, batchId: trigger.batchId },
    { acpPool: pool, ...(logger !== undefined ? { logger } : {}) },
  );
  const promote = await runPromote(
    {
      pipelineName: pipeline.name,
      batchId: trigger.batchId,
      runId: trigger.runId,
      ...(trigger.watermarkAtStart !== undefined
        ? { watermarkAtStart: trigger.watermarkAtStart }
        : {}),
    },
    { acpPool: pool, watermarkStore, ...(logger !== undefined ? { logger } : {}) },
  );
  return { trigger, extracts, validate, promote };
}

export interface RunIncrementalResult {
  trigger: TriggerOutput;
  extracts: ExtractorResult[];
  drain: DrainResult;
  stub: BulkStubHandle;
}

export async function runIncremental(
  pipeline: PipelineConfig,
  pool: PgPool,
  options: { stubPort?: number; logger?: Logger } = {},
): Promise<RunIncrementalResult> {
  const stub = await startBulkStub({ pool, port: options.stubPort ?? 3100 });
  try {
    const watermarkStore = new WatermarkStore(pool);
    const triggerArgs = options.logger !== undefined ? { logger: options.logger } : {};
    const trigger = await runTrigger(
      { pipelineName: pipeline.name, mode: 'incremental' },
      { pipeline, acpPool: pool, watermarkStore, ...triggerArgs },
    );
    const extracts: ExtractorResult[] = [];
    for (const partition of trigger.partitions) {
      const extractArgs = options.logger !== undefined ? { logger: options.logger } : {};
      extracts.push(
        await runExtractor(
          {
            pipelineName: pipeline.name,
            mode: 'incremental',
            batchId: trigger.batchId,
            partition,
            ...(trigger.watermarkUsed !== undefined ? { since: trigger.watermarkUsed } : {}),
          },
          { pipeline, acpPool: pool, ...extractArgs },
        ),
      );
    }
    const bulkClient = new BulkClient({
      apiUrl: stub.url,
      ...(options.logger !== undefined ? { logger: options.logger } : {}),
    });
    const drain = await runDrainer(
      {
        pipelineName: pipeline.name,
        batchId: trigger.batchId,
        runId: trigger.runId,
        ...(trigger.watermarkAtStart !== undefined
          ? { watermarkAtStart: trigger.watermarkAtStart }
          : {}),
      },
      {
        acpPool: pool,
        bulkClient,
        bulkBatchSize: pipeline.incremental?.bulkBatchSize ?? 500,
        watermarkStore,
        ...(options.logger !== undefined ? { logger: options.logger } : {}),
      },
    );
    return { trigger, extracts, drain, stub };
  } catch (err) {
    await stub.close().catch(() => undefined);
    throw err;
  }
}

function isoMinusDays(days: number): string {
  const ms = days * 24 * 60 * 60 * 1000;
  return new Date(Date.now() - ms).toISOString();
}
