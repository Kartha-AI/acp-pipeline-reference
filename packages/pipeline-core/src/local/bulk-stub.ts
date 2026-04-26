import { createServer } from 'node:http';
import type { IncomingMessage, Server, ServerResponse } from 'node:http';
import type { Pool } from 'pg';

import type { Logger } from '../logger.js';
import { createPipelineLogger } from '../logger.js';
import type { SevenDimensions, StagingRow } from '../types.js';
import type { BulkResponse, BulkRowResult } from '../drainer/bulk-client.js';

export interface BulkStubOptions {
  /** Pool against acp-pg (local docker-compose). */
  pool: Pool;
  /** Port to listen on. Default 3000. */
  port?: number;
  /** Optional bearer token to require on inbound requests. */
  bearerToken?: string;
  logger?: Logger;
}

export interface BulkStubHandle {
  port: number;
  url: string;
  close(): Promise<void>;
}

const REQUIRED_FIELDS: ReadonlyArray<keyof StagingRow> = [
  'subtype',
  'source',
  'source_ref',
  'canonical_name',
  'context',
];

/**
 * Local-only stub of `POST /v1/objects/bulk`.
 *
 * Implements the minimum surface the drainer needs:
 *   * Identity merge by (subtype, canonical_name) — UPDATE on hit, INSERT on miss
 *   * Per-row outcome: created | updated | unchanged | rejected
 *   * Per-row change_log entry on actual mutation (uses jsonb_changed_paths
 *     installed by migration 003 to detect no-op merges)
 *
 * NOT a substitute for the real ACP API — no auth, no rate limiting, no
 * schema validation beyond required fields. Used only by `pnpm pipeline:run`
 * for end-to-end local testing.
 */
export async function startBulkStub(opts: BulkStubOptions): Promise<BulkStubHandle> {
  const port = opts.port ?? 3000;
  const logger = (opts.logger ?? createPipelineLogger({})).child({
    component: 'bulk-stub',
    port,
  });

  const server = createServer((req, res) => {
    void handleRequest(req, res, opts, logger).catch((err: unknown) => {
      logger.error({ err }, 'unhandled error in /bulk handler');
      sendJson(res, 500, { error: 'internal_error' });
    });
  });

  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, '127.0.0.1', () => resolve());
  });

  logger.info({ url: `http://127.0.0.1:${port}` }, 'bulk stub listening');

  return {
    port,
    url: `http://127.0.0.1:${port}`,
    close: () => closeServer(server),
  };
}

async function handleRequest(
  req: IncomingMessage,
  res: ServerResponse,
  opts: BulkStubOptions,
  logger: Logger,
): Promise<void> {
  if (req.method !== 'POST' || req.url !== '/v1/objects/bulk') {
    sendJson(res, 404, { error: 'not_found', method: req.method, url: req.url });
    return;
  }
  if (opts.bearerToken !== undefined) {
    const expected = `Bearer ${opts.bearerToken}`;
    if (req.headers['authorization'] !== expected) {
      sendJson(res, 401, { error: 'unauthorized' });
      return;
    }
  }

  let body: string;
  try {
    body = await readBody(req);
  } catch (err) {
    logger.warn({ err }, 'failed to read body');
    sendJson(res, 400, { error: 'invalid_body' });
    return;
  }

  let payload: unknown;
  try {
    payload = JSON.parse(body);
  } catch (err) {
    logger.warn({ err }, 'invalid JSON');
    sendJson(res, 400, { error: 'invalid_json' });
    return;
  }

  if (
    payload === null ||
    typeof payload !== 'object' ||
    !Array.isArray((payload as { rows?: unknown }).rows)
  ) {
    sendJson(res, 400, { error: 'expected { rows: [...] }' });
    return;
  }

  const rows = (payload as { rows: StagingRow[] }).rows;
  const response = await processRows(rows, opts.pool, logger);
  sendJson(res, 200, response);
}

async function processRows(
  rows: StagingRow[],
  pool: Pool,
  logger: Logger,
): Promise<BulkResponse> {
  const results: BulkRowResult[] = new Array(rows.length);
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i] as StagingRow;
      const validation = validateRow(row);
      if (validation !== null) {
        results[i] = { index: i, outcome: 'rejected', error: validation };
        continue;
      }
      results[i] = await mergeRow(client, row, i);
    }
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK').catch(() => undefined);
    logger.error({ err }, 'bulk transaction rolled back');
    throw err;
  } finally {
    client.release();
  }
  return { results };
}

function validateRow(row: StagingRow | undefined): string | null {
  if (row === undefined || row === null || typeof row !== 'object') {
    return 'row must be an object';
  }
  for (const field of REQUIRED_FIELDS) {
    const v = (row as unknown as Record<string, unknown>)[field];
    if (field === 'context') {
      if (v === null || typeof v !== 'object') return `field "${field}" is required`;
    } else if (typeof v !== 'string' || v.length === 0) {
      return `field "${field}" is required`;
    }
  }
  return null;
}

async function mergeRow(
  client: import('pg').PoolClient,
  row: StagingRow,
  index: number,
): Promise<BulkRowResult> {
  const existing = await client.query<{ object_id: string; context: SevenDimensions }>(
    `SELECT object_id::text AS object_id, context
       FROM context_objects
      WHERE subtype = $1 AND canonical_name = $2
      FOR UPDATE`,
    [row.subtype, row.canonical_name],
  );
  if (existing.rows.length === 0) {
    const ins = await client.query<{ object_id: string }>(
      `INSERT INTO context_objects (subtype, source, source_ref, canonical_name, context)
       VALUES ($1, $2, $3, $4, $5::jsonb)
       RETURNING object_id::text AS object_id`,
      [
        row.subtype,
        row.source,
        row.source_ref,
        row.canonical_name,
        JSON.stringify(row.context),
      ],
    );
    const objectId = ins.rows[0]!.object_id;
    await client.query(
      `INSERT INTO change_log (change_type, subject_id, metadata)
       VALUES ('created', $1::bigint, $2::jsonb)`,
      [
        objectId,
        JSON.stringify({
          subtype: row.subtype,
          canonical_name: row.canonical_name,
          source: row.source,
          source_ref: row.source_ref,
        }),
      ],
    );
    return { index, outcome: 'created', object_id: objectId };
  }

  const current = existing.rows[0]!;
  const merged = await client.query<{ merged: SevenDimensions; changed: string[] }>(
    `SELECT jsonb_deep_merge($1::jsonb, $2::jsonb)        AS merged,
            jsonb_changed_paths($1::jsonb, $2::jsonb)     AS changed`,
    [JSON.stringify(current.context), JSON.stringify(row.context)],
  );
  const mergedRow = merged.rows[0]!;
  const changedPaths = mergedRow.changed ?? [];
  if (changedPaths.length === 0) {
    return { index, outcome: 'unchanged', object_id: current.object_id };
  }

  await client.query(
    `UPDATE context_objects
        SET context    = $1::jsonb,
            source     = $2,
            source_ref = $3,
            updated_at = now()
      WHERE object_id = $4::bigint`,
    [
      JSON.stringify(mergedRow.merged),
      row.source,
      row.source_ref,
      current.object_id,
    ],
  );
  await client.query(
    `INSERT INTO change_log (change_type, subject_id, metadata)
     VALUES ('updated', $1::bigint, $2::jsonb)`,
    [
      current.object_id,
      JSON.stringify({
        subtype: row.subtype,
        canonical_name: row.canonical_name,
        changed_paths: changedPaths,
      }),
    ],
  );
  return { index, outcome: 'updated', object_id: current.object_id };
}

function readBody(req: IncomingMessage): Promise<string> {
  return new Promise<string>((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on('data', (chunk: Buffer) => chunks.push(chunk));
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', (err) => reject(err));
  });
}

function sendJson(res: ServerResponse, status: number, body: unknown): void {
  const text = JSON.stringify(body);
  res.writeHead(status, {
    'content-type': 'application/json',
    'content-length': Buffer.byteLength(text).toString(),
  });
  res.end(text);
}

function closeServer(server: Server): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    server.close((err) => {
      if (err !== undefined && err !== null) reject(err);
      else resolve();
    });
  });
}
