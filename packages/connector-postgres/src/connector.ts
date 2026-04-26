import { Pool } from 'pg';
import { ConnectorError } from '@acp/pipeline-core';
import type {
  Logger,
  Partition,
  SourceConnector,
  SourceRow,
} from '@acp/pipeline-core';
import { rootLogger } from '@acp/pipeline-core';
import type { PoolClient } from 'pg';

import { resolvePgConfig } from './auth.js';
import type { PostgresConnectorAuth } from './auth.js';
import { streamQuery } from './streaming.js';

/**
 * Partition by every distinct value of a column (e.g. nation key, region id).
 * Best for low-cardinality columns where the value list is bounded and stable.
 */
export interface NaturalPartitionStrategy {
  kind: 'natural';
  /** Column expression substituted into `{{partition_filter}}` (e.g. 'c_nationkey'). */
  column: string;
  /** Source table for the DISTINCT discovery query. */
  table: string;
  /** Optional WHERE applied to the discovery query (e.g. 'archived = false'). */
  filter?: string;
}

/**
 * Partition by integer range. The connector reads MIN/MAX of `column` and
 * splits into `partitionCount` evenly-sized half-open intervals
 * [start, end). Use for high-cardinality numeric keys (custkey, orderkey).
 */
export interface RangePartitionStrategy {
  kind: 'range';
  column: string;
  table: string;
  partitionCount: number;
  /** Optional WHERE applied to the MIN/MAX discovery query. */
  filter?: string;
}

export type PartitionStrategy =
  | NaturalPartitionStrategy
  | RangePartitionStrategy;

export interface WatermarkConfig {
  /** Table the watermark column lives on. */
  table: string;
  /** Column whose MAX value is the watermark (typically updated_at). */
  column: string;
}

export interface PostgresConnectorConfig {
  /** Pipeline name for log/error context (used as a tag, not a SQL identifier). */
  pipelineName?: string;
  auth: PostgresConnectorAuth;
  /**
   * Extraction query template. Must contain `{{partition_filter}}` (always)
   * and `{{since}}` (substituted with watermark filter when in incremental
   * mode, or empty string otherwise).
   */
  query: string;
  partitions: PartitionStrategy;
  watermark: WatermarkConfig;
  /** Pool size. Default 4. */
  poolSize?: number;
  /** Cursor batch size for extractPartition. Default 1000. */
  cursorBatchSize?: number;
  /** Per-connection statement_timeout (ms). 0 disables. Default 0. */
  statementTimeoutMs?: number;
  /** Per-connection idle_in_transaction_session_timeout (ms). Default 60_000. */
  idleInTransactionTimeoutMs?: number;
  /** Override logger. Defaults to the pipeline-core root logger. */
  logger?: Logger;
}

interface NaturalPartitionFilter {
  kind: 'natural';
  column: string;
  value: unknown;
}

interface RangePartitionFilter {
  kind: 'range';
  column: string;
  start: number;
  end: number;
}

type PartitionFilter = NaturalPartitionFilter | RangePartitionFilter;

const PARTITION_FILTER_TOKEN = '{{partition_filter}}';
const SINCE_TOKEN = '{{since}}';

const SAFE_IDENT_PATTERN = /^[A-Za-z_][A-Za-z0-9_.]*$/;

export class PostgresConnector implements SourceConnector {
  private readonly config: PostgresConnectorConfig;
  private readonly logger: Logger;
  private pool: Pool | undefined;

  constructor(config: PostgresConnectorConfig) {
    validateConfig(config);
    this.config = config;
    this.logger = (config.logger ?? rootLogger).child({
      component: 'connector-postgres',
      ...(config.pipelineName !== undefined ? { pipelineName: config.pipelineName } : {}),
    });
  }

  async connect(): Promise<void> {
    if (this.pool !== undefined) return;
    const pgConfig = await resolvePgConfig(this.config.auth, {
      ...(this.config.pipelineName !== undefined
        ? { pipelineName: this.config.pipelineName }
        : {}),
    });
    const pool = new Pool({
      ...pgConfig,
      max: this.config.poolSize ?? 4,
      // Cursor reads block on the same client until close; idle clients are
      // recycled aggressively to avoid stale connections behind RDS proxies.
      idleTimeoutMillis: 30_000,
      statement_timeout: this.config.statementTimeoutMs ?? 0,
      idle_in_transaction_session_timeout:
        this.config.idleInTransactionTimeoutMs ?? 60_000,
    });
    pool.on('error', (err) => {
      // Pool-level errors fire on idle clients; log but don't crash, the
      // affected client is already removed from the pool.
      this.logger.error({ err }, 'postgres pool error on idle client');
    });
    try {
      const probe = await pool.connect();
      probe.release();
    } catch (cause) {
      await pool.end().catch(() => undefined);
      throw new ConnectorError(
        'Failed to acquire initial Postgres connection',
        {
          ...(this.config.pipelineName !== undefined
            ? { pipelineName: this.config.pipelineName }
            : {}),
          source: 'postgres',
          operation: 'connect',
        },
        { cause },
      );
    }
    this.pool = pool;
    this.logger.info({ poolSize: this.config.poolSize ?? 4 }, 'connector connected');
  }

  async listPartitions(since?: string): Promise<Partition[]> {
    const pool = this.requirePool('listPartitions');
    const strategy = this.config.partitions;
    if (strategy.kind === 'natural') {
      return this.listNaturalPartitions(pool, strategy, since);
    }
    return this.listRangePartitions(pool, strategy);
  }

  extractPartition(
    partition: Partition,
    since?: string,
  ): AsyncIterable<SourceRow> {
    const pool = this.requirePool('extractPartition');
    const filter = decodePartitionFilter(partition);
    return this.streamPartition(pool, filter, since, partition.id);
  }

  async getCurrentWatermark(): Promise<string> {
    const pool = this.requirePool('getCurrentWatermark');
    const sql =
      `SELECT MAX(${quoteIdent(this.config.watermark.column)})::text AS watermark ` +
      `FROM ${quoteQualified(this.config.watermark.table)}`;
    try {
      const res = await pool.query<{ watermark: string | null }>(sql);
      return res.rows[0]?.watermark ?? '';
    } catch (cause) {
      throw new ConnectorError(
        'Failed to read current watermark',
        {
          ...(this.config.pipelineName !== undefined
            ? { pipelineName: this.config.pipelineName }
            : {}),
          source: 'postgres',
          operation: 'getCurrentWatermark',
        },
        { cause },
      );
    }
  }

  async disconnect(): Promise<void> {
    if (this.pool === undefined) return;
    const pool = this.pool;
    this.pool = undefined;
    try {
      await pool.end();
    } catch (cause) {
      // Disconnect failures are logged but not thrown — this is the cleanup
      // path and re-throwing here would mask the user's primary error.
      this.logger.warn({ err: cause }, 'pool.end() failed during disconnect');
    }
  }

  private async listNaturalPartitions(
    pool: Pool,
    strategy: NaturalPartitionStrategy,
    since: string | undefined,
  ): Promise<Partition[]> {
    const params: unknown[] = [];
    const whereParts: string[] = [];
    if (strategy.filter !== undefined && strategy.filter.length > 0) {
      whereParts.push(`(${strategy.filter})`);
    }
    if (since !== undefined && since !== '') {
      params.push(since);
      whereParts.push(`${quoteIdent(this.config.watermark.column)} > $${params.length}`);
    }
    const where = whereParts.length > 0 ? `WHERE ${whereParts.join(' AND ')}` : '';
    const sql =
      `SELECT DISTINCT ${quoteIdent(strategy.column)} AS value ` +
      `FROM ${quoteQualified(strategy.table)} ${where} ` +
      `ORDER BY value`;
    const res = await pool
      .query<{ value: unknown }>(sql, params)
      .catch((cause: unknown) => {
        throw new ConnectorError(
          'Partition discovery query failed',
          {
            ...(this.config.pipelineName !== undefined
              ? { pipelineName: this.config.pipelineName }
              : {}),
            source: 'postgres',
            operation: 'listPartitions:natural',
          },
          { cause },
        );
      });
    return res.rows
      .filter((r) => r.value !== null)
      .map((r): Partition => ({
        id: `${strategy.column}=${String(r.value)}`,
        sourceFilter: { kind: 'natural', column: strategy.column, value: r.value },
      }));
  }

  private async listRangePartitions(
    pool: Pool,
    strategy: RangePartitionStrategy,
  ): Promise<Partition[]> {
    const where =
      strategy.filter !== undefined && strategy.filter.length > 0
        ? `WHERE ${strategy.filter}`
        : '';
    const sql =
      `SELECT MIN(${quoteIdent(strategy.column)})::bigint AS lo, ` +
      `MAX(${quoteIdent(strategy.column)})::bigint AS hi, ` +
      `COUNT(*)::bigint AS n ` +
      `FROM ${quoteQualified(strategy.table)} ${where}`;
    const res = await pool
      .query<{ lo: string | null; hi: string | null; n: string }>(sql)
      .catch((cause: unknown) => {
        throw new ConnectorError(
          'Range partition discovery query failed',
          {
            ...(this.config.pipelineName !== undefined
              ? { pipelineName: this.config.pipelineName }
              : {}),
            source: 'postgres',
            operation: 'listPartitions:range',
          },
          { cause },
        );
      });
    const row = res.rows[0];
    if (row === undefined || row.lo === null || row.hi === null) return [];
    const lo = Number(row.lo);
    const hi = Number(row.hi);
    const total = Number(row.n);
    const count = Math.max(1, strategy.partitionCount);
    const span = hi - lo + 1;
    const stride = Math.ceil(span / count);
    const partitions: Partition[] = [];
    for (let i = 0; i < count; i++) {
      const start = lo + i * stride;
      if (start > hi) break;
      const end = Math.min(start + stride, hi + 1);
      partitions.push({
        id: `${strategy.column}=[${start},${end})`,
        sourceFilter: {
          kind: 'range',
          column: strategy.column,
          start,
          end,
        },
        estimatedRows: Math.ceil(total / count),
      });
    }
    return partitions;
  }

  private async *streamPartition(
    pool: Pool,
    filter: PartitionFilter,
    since: string | undefined,
    partitionId: string,
  ): AsyncGenerator<SourceRow, void, void> {
    const { sql, values } = this.buildExtractionQuery(filter, since);
    let client: PoolClient;
    try {
      client = await pool.connect();
    } catch (cause) {
      throw new ConnectorError(
        'Failed to acquire client for partition extraction',
        {
          ...(this.config.pipelineName !== undefined
            ? { pipelineName: this.config.pipelineName }
            : {}),
          source: 'postgres',
          operation: 'extractPartition',
          partitionId,
        },
        { cause },
      );
    }
    try {
      yield* streamQuery<SourceRow>(client, sql, values, {
        batchSize: this.config.cursorBatchSize ?? 1000,
      });
    } catch (cause) {
      throw new ConnectorError(
        `Cursor stream failed for partition ${partitionId}`,
        {
          ...(this.config.pipelineName !== undefined
            ? { pipelineName: this.config.pipelineName }
            : {}),
          source: 'postgres',
          operation: 'extractPartition',
          partitionId,
        },
        { cause },
      );
    } finally {
      client.release();
    }
  }

  private buildExtractionQuery(
    filter: PartitionFilter,
    since: string | undefined,
  ): { sql: string; values: unknown[] } {
    const values: unknown[] = [];
    let partitionFragment: string;
    if (filter.kind === 'natural') {
      values.push(filter.value);
      partitionFragment = `${quoteIdent(filter.column)} = $${values.length}`;
    } else {
      values.push(filter.start);
      const startIdx = values.length;
      values.push(filter.end);
      const endIdx = values.length;
      partitionFragment =
        `${quoteIdent(filter.column)} >= $${startIdx} ` +
        `AND ${quoteIdent(filter.column)} < $${endIdx}`;
    }
    let sinceFragment = '';
    if (since !== undefined && since !== '') {
      values.push(since);
      sinceFragment = `AND ${quoteIdent(this.config.watermark.column)} > $${values.length}`;
    }
    const sql = this.config.query
      .split(PARTITION_FILTER_TOKEN)
      .join(partitionFragment)
      .split(SINCE_TOKEN)
      .join(sinceFragment);
    return { sql, values };
  }

  private requirePool(operation: string): Pool {
    if (this.pool === undefined) {
      throw new ConnectorError(
        `connect() must be called before ${operation}()`,
        {
          ...(this.config.pipelineName !== undefined
            ? { pipelineName: this.config.pipelineName }
            : {}),
          source: 'postgres',
          operation,
        },
      );
    }
    return this.pool;
  }
}

/** Factory matching the lowercase form referenced in CLAUDE.md. */
export function postgresConnector(
  config: PostgresConnectorConfig,
): PostgresConnector {
  return new PostgresConnector(config);
}

function validateConfig(config: PostgresConnectorConfig): void {
  if (typeof config.query !== 'string' || config.query.length === 0) {
    throw new Error('PostgresConnector: `query` is required');
  }
  if (!config.query.includes(PARTITION_FILTER_TOKEN)) {
    throw new Error(
      `PostgresConnector: \`query\` must contain ${PARTITION_FILTER_TOKEN}`,
    );
  }
  if (!config.query.includes(SINCE_TOKEN)) {
    throw new Error(`PostgresConnector: \`query\` must contain ${SINCE_TOKEN}`);
  }
  validateIdent('partitions.column', config.partitions.column);
  validateIdent('partitions.table', config.partitions.table);
  validateIdent('watermark.column', config.watermark.column);
  validateIdent('watermark.table', config.watermark.table);
  if (config.partitions.kind === 'range') {
    if (
      !Number.isInteger(config.partitions.partitionCount) ||
      config.partitions.partitionCount <= 0
    ) {
      throw new Error(
        'PostgresConnector: `partitions.partitionCount` must be a positive integer',
      );
    }
  }
}

// SAFE_IDENT_PATTERN allows letters, digits, underscore, and one optional dot
// for `schema.table` style references. We never interpolate user-supplied
// strings into SQL outside of this guard; data values always go through pg's
// parameter binding ($1, $2, ...).
function validateIdent(label: string, value: string): void {
  if (typeof value !== 'string' || value.length === 0) {
    throw new Error(`PostgresConnector: \`${label}\` must be a non-empty string`);
  }
  if (!SAFE_IDENT_PATTERN.test(value)) {
    throw new Error(
      `PostgresConnector: \`${label}\` must match ${SAFE_IDENT_PATTERN} (letters, digits, underscore, optional .). Got "${value}".`,
    );
  }
}

function quoteIdent(name: string): string {
  return name
    .split('.')
    .map((part) => `"${part.replace(/"/g, '""')}"`)
    .join('.');
}

function quoteQualified(name: string): string {
  return quoteIdent(name);
}

function decodePartitionFilter(partition: Partition): PartitionFilter {
  const sf = partition.sourceFilter as Record<string, unknown>;
  if (sf['kind'] === 'natural' && typeof sf['column'] === 'string') {
    return { kind: 'natural', column: sf['column'], value: sf['value'] };
  }
  if (
    sf['kind'] === 'range' &&
    typeof sf['column'] === 'string' &&
    typeof sf['start'] === 'number' &&
    typeof sf['end'] === 'number'
  ) {
    return {
      kind: 'range',
      column: sf['column'],
      start: sf['start'],
      end: sf['end'],
    };
  }
  throw new ConnectorError(
    `Partition ${partition.id} has unrecognized sourceFilter shape`,
    { source: 'postgres', operation: 'extractPartition', partitionId: partition.id },
  );
}
