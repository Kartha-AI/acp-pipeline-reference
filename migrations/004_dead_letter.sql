-- 004_dead_letter.sql
--
-- Captures rows the pipeline could not promote so a backfill can be
-- triaged without losing the source payload. Two writers populate it:
--   * The transformer, when a source row can't be mapped to a StagingRow
--   * The drainer, when /v1/objects/bulk rejects a row with a permanent error
--
-- Initial-mode rejections stay inside the staging table itself (last_error
-- column) so the set-based promote can mark them in a single UPDATE; only
-- transform-time and /bulk-time rejections land here.

CREATE TABLE IF NOT EXISTS acp_rejected_rows (
  rejection_id    BIGSERIAL    PRIMARY KEY,
  pipeline_name   TEXT         NOT NULL,
  batch_id        UUID         NOT NULL,
  partition_key   TEXT,
  source_ref      TEXT,
  source_payload  JSONB        NOT NULL,
  reason_code     TEXT         NOT NULL,
  reason          TEXT         NOT NULL,
  rejected_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
  reprocessed_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS acp_rejected_rows_pipeline_idx
  ON acp_rejected_rows (pipeline_name, rejected_at DESC);

CREATE INDEX IF NOT EXISTS acp_rejected_rows_unresolved_idx
  ON acp_rejected_rows (pipeline_name, reason_code)
  WHERE reprocessed_at IS NULL;
