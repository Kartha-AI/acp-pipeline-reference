-- 005_pipeline_runs.sql
--
-- One row per pipeline invocation. The trigger Lambda reads the most recent
-- succeeded run for a pipeline and uses its watermark_set as the `since`
-- value for the next incremental run.
--
-- batch_id is generated once per logical run and persisted here so retries
-- of the same run reuse the same batch_id and stay idempotent against
-- the staging table and the promote function.

CREATE TABLE IF NOT EXISTS pipeline_runs (
  run_id          UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  pipeline_name   TEXT         NOT NULL,
  mode            TEXT         NOT NULL CHECK (mode IN ('initial', 'incremental')),
  batch_id        UUID         NOT NULL,
  started_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
  finished_at     TIMESTAMPTZ,
  watermark_used  TEXT,                                                 -- watermark at start (for incremental)
  watermark_set   TEXT,                                                 -- new watermark for next run
  status          TEXT         NOT NULL DEFAULT 'running'
                  CHECK (status IN ('running', 'succeeded', 'failed')),
  error           TEXT,
  rows_inserted   INTEGER,
  rows_rejected   INTEGER
);

CREATE INDEX IF NOT EXISTS pipeline_runs_pipeline_started_idx
  ON pipeline_runs (pipeline_name, started_at DESC);

-- Hot path: trigger Lambda asks "what's the latest succeeded run for this
-- pipeline?". Partial index keeps that lookup index-only.
CREATE INDEX IF NOT EXISTS pipeline_runs_latest_success_idx
  ON pipeline_runs (pipeline_name, started_at DESC)
  WHERE status = 'succeeded';
