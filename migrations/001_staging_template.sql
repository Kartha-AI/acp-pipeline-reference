-- 001_staging_template.sql
--
-- Defines the per-pipeline staging area:
--   * acp_staging schema (segregated from ACP's owned objects)
--   * acp_staging._template — canonical column shape every pipeline staging
--     table copies via LIKE INCLUDING ALL
--   * acp_create_staging_table(pipeline_name) — idempotent helper that creates
--     a physical UNLOGGED table per pipeline, plus the indexes needed by the
--     drainer and the promote function
--
-- Per-pipeline tables (rather than one shared table) keep concurrent pipelines
-- from contending on the same rows and let lifecycle (truncate, archive,
-- vacuum) be managed per pipeline.
--
-- UNLOGGED is intentional: staging is replayable from source and not worth
-- WAL overhead during 100M-row backfills. Promote into context_objects is a
-- normal logged INSERT, so durability of promoted data is unaffected.

CREATE SCHEMA IF NOT EXISTS acp_staging;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS acp_staging._template (
  staging_id      BIGSERIAL PRIMARY KEY,
  subtype         TEXT        NOT NULL,
  source          TEXT        NOT NULL,
  source_ref      TEXT        NOT NULL,
  canonical_name  TEXT        NOT NULL,
  context         JSONB       NOT NULL,
  batch_id        UUID        NOT NULL,
  partition_key   TEXT        NOT NULL,
  mode            TEXT        NOT NULL CHECK (mode IN ('initial', 'incremental')),
  ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  promoted_at     TIMESTAMPTZ,
  last_error      TEXT
);

CREATE OR REPLACE FUNCTION acp_create_staging_table(pipeline_name TEXT)
RETURNS VOID
LANGUAGE plpgsql
AS $fn$
BEGIN
  IF pipeline_name !~ '^[a-z][a-z0-9_]{0,62}$' THEN
    RAISE EXCEPTION
      'acp_create_staging_table: pipeline_name % must match ^[a-z][a-z0-9_]{0,62}$',
      pipeline_name;
  END IF;

  EXECUTE format(
    'CREATE UNLOGGED TABLE IF NOT EXISTS acp_staging.%I (LIKE acp_staging._template INCLUDING ALL)',
    pipeline_name
  );

  -- Drainer / promote both filter on (mode, promoted_at IS NULL); partial index
  -- keeps the working set tiny once most rows have been promoted.
  EXECUTE format(
    'CREATE INDEX IF NOT EXISTS %I ON acp_staging.%I (mode, promoted_at) WHERE promoted_at IS NULL',
    pipeline_name || '_unpromoted_idx', pipeline_name
  );

  -- Trigram index supports fuzzy canonical_name lookups during identity-merge
  -- diagnostics; restricted to unpromoted rows to bound size.
  EXECUTE format(
    'CREATE INDEX IF NOT EXISTS %I ON acp_staging.%I USING gin (canonical_name gin_trgm_ops) WHERE promoted_at IS NULL',
    pipeline_name || '_canon_idx', pipeline_name
  );
END;
$fn$;
