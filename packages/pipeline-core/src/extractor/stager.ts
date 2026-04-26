import { from as copyFrom } from 'pg-copy-streams';
import type { Pool, PoolClient } from 'pg';

import { StagingError } from '../errors.js';
import type { Logger } from '../logger.js';
import type { PipelineMode, StagingRow } from '../types.js';

/**
 * One staging-table row. The shape mirrors `acp_staging._template` minus the
 * server-managed columns (staging_id BIGSERIAL, ingested_at default,
 * promoted_at NULL, last_error NULL).
 */
export interface StagingRecord extends StagingRow {
  batch_id: string;
  partition_key: string;
  mode: PipelineMode;
}

export interface StageRowsOptions {
  pool: Pool;
  /** Pipeline name (also the staging table name in `acp_staging.<name>`). */
  pipelineName: string;
  /** For log/error context. */
  partitionId?: string;
  /** For log/error context. */
  batchId?: string;
  logger?: Logger;
}

export interface StageRowsResult {
  rowsCopied: number;
}

const STAGING_COLUMNS = [
  'subtype',
  'source',
  'source_ref',
  'canonical_name',
  'context',
  'batch_id',
  'partition_key',
  'mode',
] as const;

/**
 * Stream an `AsyncIterable<StagingRecord>` into `acp_staging.<pipelineName>`
 * via a server-side COPY. Memory-bounded: each row is encoded and written
 * before the next is pulled; backpressure is honoured by waiting on `drain`.
 *
 * The COPY is wrapped in a transaction so a partial-extract failure leaves
 * the staging table untouched — the next run with the same batch_id can
 * retry cleanly.
 */
export async function stageRows(
  records: AsyncIterable<StagingRecord>,
  opts: StageRowsOptions,
): Promise<StageRowsResult> {
  const fqTable = `acp_staging.${quoteIdent(opts.pipelineName)}`;
  const colList = STAGING_COLUMNS.map(quoteIdent).join(', ');
  const sql = `COPY ${fqTable} (${colList}) FROM STDIN`;

  let client: PoolClient;
  try {
    client = await opts.pool.connect();
  } catch (cause) {
    throw new StagingError(
      'Failed to acquire client for COPY into staging',
      stagingContext(opts, 0),
      { cause },
    );
  }

  let copied = 0;
  let txnOpen = false;
  try {
    await client.query('BEGIN');
    txnOpen = true;
    const stream = client.query(copyFrom(sql));
    try {
      for await (const record of records) {
        const line = encodeCopyRow(record);
        if (!stream.write(line)) {
          await onceDrain(stream);
        }
        copied++;
      }
    } catch (rowErr) {
      stream.destroy(rowErr instanceof Error ? rowErr : new Error(String(rowErr)));
      throw rowErr;
    }
    await endStream(stream);
    await client.query('COMMIT');
    txnOpen = false;
    opts.logger?.info({ rowsCopied: copied }, 'COPY into staging committed');
    return { rowsCopied: copied };
  } catch (cause) {
    if (txnOpen) {
      await client.query('ROLLBACK').catch(() => undefined);
    }
    throw new StagingError(
      `COPY into ${fqTable} failed`,
      stagingContext(opts, copied),
      { cause },
    );
  } finally {
    client.release();
  }
}

function stagingContext(
  opts: StageRowsOptions,
  copied: number,
): {
  pipelineName: string;
  table: string;
  rowsAttempted: number;
  partitionId?: string;
  batchId?: string;
} {
  return {
    pipelineName: opts.pipelineName,
    table: `acp_staging.${opts.pipelineName}`,
    rowsAttempted: copied,
    ...(opts.partitionId !== undefined ? { partitionId: opts.partitionId } : {}),
    ...(opts.batchId !== undefined ? { batchId: opts.batchId } : {}),
  };
}

function encodeCopyRow(record: StagingRecord): string {
  const fields: string[] = [
    encodeText(record.subtype),
    encodeText(record.source),
    encodeText(record.source_ref),
    encodeText(record.canonical_name),
    encodeJsonb(record.context),
    encodeText(record.batch_id),
    encodeText(record.partition_key),
    encodeText(record.mode),
  ];
  return fields.join('\t') + '\n';
}

/**
 * COPY text-format escaping per Postgres docs:
 *   \\ → \\, tab → \t, LF → \n, CR → \r, NULL marker is `\N`.
 * Escape `\` first; later substitutions don't introduce new backslashes.
 */
function encodeText(value: string | null | undefined): string {
  if (value === null || value === undefined) return '\\N';
  return value
    .replace(/\\/g, '\\\\')
    .replace(/\t/g, '\\t')
    .replace(/\n/g, '\\n')
    .replace(/\r/g, '\\r');
}

function encodeJsonb(value: unknown): string {
  return encodeText(JSON.stringify(value));
}

function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

function onceDrain(stream: NodeJS.WritableStream): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const onDrain = (): void => {
      stream.removeListener('error', onError);
      resolve();
    };
    const onError = (err: Error): void => {
      stream.removeListener('drain', onDrain);
      reject(err);
    };
    stream.once('drain', onDrain);
    stream.once('error', onError);
  });
}

function endStream(stream: NodeJS.WritableStream): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    stream.once('finish', () => resolve());
    stream.once('error', (err) => reject(err));
    stream.end();
  });
}
