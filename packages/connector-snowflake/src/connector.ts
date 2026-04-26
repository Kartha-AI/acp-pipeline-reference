import { ConnectorError } from '@acp/pipeline-core';
import type {
  Partition,
  SourceConnector,
  SourceRow,
} from '@acp/pipeline-core';

import type { SnowflakeAuth } from './auth.js';

/**
 * Partition strategies the implementer should support. Names mirror the
 * Postgres connector so a customer who already has a `pipeline.config.ts`
 * pointing at Postgres can swap connectors with minimal config changes.
 */
export type SnowflakePartitionStrategy =
  | {
      kind: 'natural';
      /** SQL expression substituted into `{{partition_filter}}`. */
      column: string;
      /** Source table for DISTINCT discovery. */
      table: string;
      filter?: string;
    }
  | {
      kind: 'range';
      column: string;
      table: string;
      partitionCount: number;
    }
  | {
      kind: 'date';
      /** Date/timestamp column to bucket by. */
      column: string;
      table: string;
      /** Bucket grain: 'day' | 'hour'. */
      grain: 'day' | 'hour';
      /** Look back this many buckets for `since=undefined` runs. */
      maxLookbackBuckets?: number;
    };

export interface SnowflakeConnectorConfig {
  pipelineName?: string;
  auth: SnowflakeAuth;
  /**
   * Extraction query. Same template syntax as the Postgres connector:
   *   * `{{partition_filter}}` — SQL fragment isolating one partition
   *   * `{{since}}` — `AND <watermark_col> > ?` for incremental runs, empty otherwise
   *
   * Use `?` placeholders (Snowflake's binding syntax), not `$1`.
   */
  query: string;
  partitions: SnowflakePartitionStrategy;
  watermark: { table: string; column: string };
  /** Result-set fetch size; default 10_000. */
  fetchSize?: number;
}

/**
 * Skeleton implementation of `SourceConnector` for Snowflake. Every method
 * throws `ConnectorError("not implemented: ...")` with a hint pointing at the
 * relevant section of `README.md`. The class compiles, has the right shape,
 * and types correctly — implementers fill in each method.
 *
 * Estimated effort to complete: 1–2 weeks for someone familiar with
 * Snowflake (key-pair auth, async query execution, streaming result sets).
 *
 * Reference for streaming patterns: `packages/connector-postgres/src/streaming.ts`.
 */
export class SnowflakeConnector implements SourceConnector {
  // Stash these so an implementer's IDE can hover and see the config shape.
  // `connection` is `unknown` until snowflake-sdk's connection type is wired
  // in by the implementer; left unread by the skeleton.
  private readonly config: SnowflakeConnectorConfig;
  protected connection: unknown;

  constructor(config: SnowflakeConnectorConfig) {
    this.config = config;
    this.connection = undefined;
  }

  /**
   * What `connect()` must do:
   *
   *   1. Call `loadPrivateKey(this.config.auth)` and `generateJwtToken(...)`
   *      from `./auth.ts`.
   *   2. Construct a Snowflake connection via `snowflake-sdk`'s
   *      `createConnection({ authenticator: 'SNOWFLAKE_JWT', token, ... })`.
   *   3. Call `connection.connect((err) => ...)` and resolve when ready.
   *      Wrap in a Promise.
   *   4. Run a one-time `USE WAREHOUSE`/`USE DATABASE`/`USE SCHEMA` if those
   *      are set on `auth.account`.
   *
   * Gotchas:
   *   * Tokens are valid for 1 hour. For long-running extracts, re-mint and
   *     re-bind via `connection.refreshToken()` before expiry. Track issuedAt
   *     and refresh at ~50 min.
   *   * Account identifier format: `<orgname>-<account_name>` for accounts in
   *     orgs, OR `<account_locator>.<region>` for legacy accounts. Both work
   *     with snowflake-sdk's `account` field; pick the one your customer uses.
   *   * Set `clientStoreTemporaryCredential: false` and
   *     `clientRequestMfaToken: false` — the JWT path doesn't need either.
   */
  async connect(): Promise<void> {
    throw this.notImplemented('connect');
  }

  /**
   * `listPartitions(since?)` — strategy-specific.
   *
   * Pattern reference (`packages/connector-postgres/src/connector.ts`,
   * `listNaturalPartitions`/`listRangePartitions`).
   *
   *   * `natural`: `SELECT DISTINCT <col> FROM <table> WHERE <since>` —
   *     returns one Partition per distinct value. Best for dimension keys
   *     with bounded cardinality (region_id, tenant_id).
   *   * `range`: `SELECT MIN/MAX/COUNT FROM <table>`, then split [min,max]
   *     into `partitionCount` buckets. Best for high-cardinality numeric
   *     keys where DISTINCT would be too large.
   *   * `date`: bucket by `DATE_TRUNC('day'|'hour', <col>)`. Best when the
   *     watermark column is the same column you partition on, and the
   *     dataset is mostly time-series.
   *
   * Less common but useful for very large tables: introspect
   * `INFORMATION_SCHEMA.TABLE_STORAGE_METRICS` to align partitions with
   * Snowflake's micro-partition boundaries — extracts hit fewer files. The
   * skeleton doesn't ship this; add it if you find serial extracts of
   * monolithic dimensions are too slow.
   */
  async listPartitions(_since?: string): Promise<Partition[]> {
    throw this.notImplemented('listPartitions');
  }

  /**
   * `extractPartition(partition, since?)` — must yield rows one at a time
   * with constant memory. The single biggest landmine in Snowflake
   * extraction:
   *
   *   ⚠️  `streamRows()` returns an event emitter. The naive
   *      implementation accumulates rows in an array on the 'data' event
   *      and yields them on 'end'. **DO NOT DO THIS.** It blows memory
   *      proportional to partition size.
   *
   * Correct shape — yield each row as it arrives:
   *
   *     const stmt = await execute(connection, sql, values);
   *     const rows = stmt.streamRows({ start: 0 });
   *     for await (const row of asyncIterableFromStream(rows)) {
   *       yield row as SourceRow;
   *     }
   *
   * `asyncIterableFromStream` is a one-line wrapper around
   * `events.on(emitter, 'data')` that exits on 'end' and rethrows on 'error'.
   * `node:events.on` (Node 12+) gives you exactly that — no third-party dep
   * needed.
   *
   * Substitute `{{partition_filter}}` and `{{since}}` into `this.config.query`
   * the same way the Postgres connector does (split-join, not regex; values
   * always go through the binding array).
   *
   * For large partitions: pass `fetchSize` to the statement so each network
   * round-trip pulls 10k rows at a time. Don't rely on the SDK default
   * (which is small).
   */
  extractPartition(
    _partition: Partition,
    _since?: string,
  ): AsyncIterable<SourceRow> {
    const error = this.notImplemented('extractPartition');
    return {
      [Symbol.asyncIterator](): AsyncIterator<SourceRow> {
        return {
          next(): Promise<IteratorResult<SourceRow>> {
            return Promise.reject(error);
          },
        };
      },
    };
  }

  /**
   * `getCurrentWatermark()` — pick one of:
   *
   *   * `SELECT current_timestamp()::string` — coarse but works without
   *     touching the source table. Subject to the source/Snowflake clock
   *     drift. Fine when watermark precision can be in seconds.
   *   * `SELECT MAX(<watermark_col>)::string FROM <table>` — same as the
   *     Postgres connector. Most accurate; costs one COUNT-style scan
   *     against the column.
   *   * For true CDC: query a Snowflake `STREAM`'s offset and store *that*.
   *     Streams give you ordered change records and let the next run pick
   *     up exactly where this one left off, with no missed/duplicated rows.
   *     Significantly more setup (the source must own the stream); the
   *     reference pipeline doesn't ship this — implementers add it once
   *     they need real CDC semantics.
   *
   * The current default skeleton would be MAX(watermark_col) for parity
   * with the Postgres connector.
   */
  async getCurrentWatermark(): Promise<string> {
    throw this.notImplemented('getCurrentWatermark');
  }

  /**
   * `disconnect()` — call `connection.destroy(callback)`, wrap in a Promise,
   * resolve regardless of error (cleanup path; never let it throw).
   * Clear any cached private key / JWT to free memory in long-lived
   * processes.
   */
  async disconnect(): Promise<void> {
    throw this.notImplemented('disconnect');
  }

  private notImplemented(method: string): ConnectorError {
    return new ConnectorError(
      `not implemented: ${method} — see packages/connector-snowflake/README.md`,
      {
        ...(this.config.pipelineName !== undefined
          ? { pipelineName: this.config.pipelineName }
          : {}),
        source: 'snowflake',
        operation: method,
      },
    );
  }
}

/** Lowercase factory matching the Postgres connector's idiom. */
export function snowflakeConnector(
  config: SnowflakeConnectorConfig,
): SnowflakeConnector {
  return new SnowflakeConnector(config);
}
