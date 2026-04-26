import type { Pool } from 'pg';

import type { PipelineMode } from '../types.js';

export type RunStatus = 'running' | 'succeeded' | 'failed';

export interface StartRunParams {
  pipelineName: string;
  mode: PipelineMode;
  batchId: string;
  /** Watermark at the start of this run (for incremental). */
  watermarkUsed?: string;
}

export interface FinishRunParams {
  status: 'succeeded' | 'failed';
  /** New watermark to use as `since` for the next run. */
  watermarkSet?: string;
  rowsInserted?: number;
  rowsRejected?: number;
  error?: string;
}

interface PipelineRunRow {
  run_id: string;
  watermark_set: string | null;
}

/**
 * Thin wrapper around the `pipeline_runs` table. Owned by trigger and
 * promoter/drainer handlers — the trigger writes (insert) and the terminal
 * stage writes (update). All other readers (e.g. the next trigger
 * invocation) only call `getLatestSucceededWatermark`.
 */
export class WatermarkStore {
  constructor(private readonly pool: Pool) {}

  /**
   * Returns the `watermark_set` of the most recent successful run for this
   * pipeline, or `undefined` if there are no successful runs yet (cold start).
   * The next incremental run uses this as `since`.
   */
  async getLatestSucceededWatermark(pipelineName: string): Promise<string | undefined> {
    const res = await this.pool.query<{ watermark_set: string | null }>(
      `SELECT watermark_set
         FROM pipeline_runs
        WHERE pipeline_name = $1
          AND status = 'succeeded'
        ORDER BY started_at DESC
        LIMIT 1`,
      [pipelineName],
    );
    const value = res.rows[0]?.watermark_set;
    return value === null || value === undefined ? undefined : value;
  }

  /**
   * Insert a new `running` row. Returns the generated run_id. Caller must
   * pair every successful startRun with exactly one finishRun.
   */
  async startRun(params: StartRunParams): Promise<string> {
    const res = await this.pool.query<PipelineRunRow>(
      `INSERT INTO pipeline_runs (pipeline_name, mode, batch_id, watermark_used, status)
       VALUES ($1, $2, $3, $4, 'running')
       RETURNING run_id, watermark_set`,
      [params.pipelineName, params.mode, params.batchId, params.watermarkUsed ?? null],
    );
    const row = res.rows[0];
    if (row === undefined) {
      throw new Error('pipeline_runs insert returned no row');
    }
    return row.run_id;
  }

  async finishRun(runId: string, params: FinishRunParams): Promise<void> {
    await this.pool.query(
      `UPDATE pipeline_runs
          SET status        = $2,
              finished_at   = now(),
              watermark_set = COALESCE($3, watermark_set),
              rows_inserted = COALESCE($4, rows_inserted),
              rows_rejected = COALESCE($5, rows_rejected),
              error         = $6
        WHERE run_id = $1`,
      [
        runId,
        params.status,
        params.watermarkSet ?? null,
        params.rowsInserted ?? null,
        params.rowsRejected ?? null,
        params.error ?? null,
      ],
    );
  }

  /** Used by retry paths that need to bump status back to running on a re-attempt. */
  async setStatus(runId: string, status: RunStatus): Promise<void> {
    await this.pool.query(`UPDATE pipeline_runs SET status = $2 WHERE run_id = $1`, [
      runId,
      status,
    ]);
  }
}
