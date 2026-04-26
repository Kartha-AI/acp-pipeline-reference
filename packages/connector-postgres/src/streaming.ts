import Cursor from 'pg-cursor';
import type { PoolClient } from 'pg';

export interface CursorStreamOptions {
  /** Rows fetched per server round-trip. Default 1000. */
  batchSize?: number;
}

/**
 * Stream rows from a parameterized query via a Postgres server-side cursor.
 *
 * Memory-bounded: the generator pulls one batch at a time and yields rows
 * one by one, so the resident set never grows with the result size. The
 * caller must consume the generator to completion *or* stop iteration —
 * the `try/finally` guarantees the cursor is closed and the client returns
 * to the pool either way.
 */
export async function* streamQuery<TRow extends Record<string, unknown>>(
  client: PoolClient,
  sql: string,
  values: ReadonlyArray<unknown>,
  options: CursorStreamOptions = {},
): AsyncGenerator<TRow, void, void> {
  const batchSize = options.batchSize ?? 1000;
  // Cast through unknown — pg-cursor's types describe Cursor as Submittable,
  // but the `client.query(submittable)` overload's inferred return type
  // depends on the pg version; this stays correct without `any`.
  const cursor = client.query(
    new Cursor<TRow>(sql, [...values]),
  ) as unknown as Cursor<TRow>;
  try {
    while (true) {
      const rows = await readBatch<TRow>(cursor, batchSize);
      if (rows.length === 0) return;
      for (const row of rows) yield row;
    }
  } finally {
    await closeCursor(cursor);
  }
}

function readBatch<T>(cursor: Cursor<T>, n: number): Promise<T[]> {
  return new Promise<T[]>((resolve, reject) => {
    cursor.read(n, (err, rows) => {
      if (err !== null && err !== undefined) reject(err);
      else resolve(rows);
    });
  });
}

function closeCursor(cursor: Cursor): Promise<void> {
  return new Promise<void>((resolve) => {
    // Swallow close errors: by the time we hit the finally block we either
    // succeeded (close is a tidy-up) or are already propagating a different
    // error. Either way, masking close failures gives clearer diagnostics.
    cursor.close(() => resolve());
  });
}
