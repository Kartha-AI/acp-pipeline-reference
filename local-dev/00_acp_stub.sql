-- 00_acp_stub.sql
--
-- LOCAL-DEV ONLY. The real ACP platform owns `context_objects` and
-- `change_log` and creates them via its own migrations. This file exists
-- so docker-compose's `acp-pg` container can simulate the full target
-- schema in one container, letting `pnpm pipeline:run` exercise the
-- complete flow (initial promote + incremental /bulk drain) without a
-- live ACP deployment.
--
-- This file is mounted as `/docker-entrypoint-initdb.d/00_acp_stub.sql`,
-- so it runs *before* the pipeline migrations (001-005). The pipeline
-- migrations reference these tables but use late binding inside plpgsql
-- functions, so installation order with vs. without these stubs is
-- functionally equivalent — they only need to exist before the functions
-- are *called*.

CREATE TABLE IF NOT EXISTS context_objects (
  object_id      BIGSERIAL    PRIMARY KEY,
  subtype        TEXT         NOT NULL,
  source         TEXT         NOT NULL,
  source_ref     TEXT         NOT NULL,
  canonical_name TEXT         NOT NULL,
  context        JSONB        NOT NULL,
  created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
  CONSTRAINT context_objects_subtype_canonical_name_key
    UNIQUE (subtype, canonical_name)
);

CREATE INDEX IF NOT EXISTS context_objects_source_ref_idx
  ON context_objects (subtype, source, source_ref);

CREATE TABLE IF NOT EXISTS change_log (
  log_id      BIGSERIAL    PRIMARY KEY,
  change_type TEXT         NOT NULL,
  subject_id  BIGINT,
  metadata    JSONB,
  created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS change_log_type_idx
  ON change_log (change_type, created_at DESC);
