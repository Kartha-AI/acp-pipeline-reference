-- 002_promote_function.sql
--
-- Set-based promote for initial mode. Moves staged rows into the ACP-owned
-- context_objects table with a single bulk INSERT, then writes one
-- change_log marker for the whole batch.
--
-- This bypasses /v1/objects/bulk entirely so 100M-row backfills don't pay
-- per-row HTTP, validation, or change_log cost. Trade-off: agents see one
-- 'initial_load' change_log entry per batch, not one per row.
--
-- Assumed ACP-owned schema (managed by the platform, not by this repo):
--   context_objects(subtype, source, source_ref, canonical_name, context jsonb, ...)
--     UNIQUE (subtype, canonical_name)
--   change_log(change_type text, metadata jsonb, created_at timestamptz default now(), ...)
--
-- Re-running with the same (pipeline_name, batch_id) is safe: ON CONFLICT
-- DO NOTHING absorbs duplicates and the WHERE promoted_at IS NULL clause
-- skips already-promoted rows.

CREATE OR REPLACE FUNCTION acp_promote_staging_initial(
  p_pipeline_name TEXT,
  p_batch_id      UUID
)
RETURNS TABLE (
  inserted_count INTEGER,
  rejected_count INTEGER,
  duration_ms    INTEGER
)
LANGUAGE plpgsql
AS $fn$
DECLARE
  v_started_at TIMESTAMPTZ := clock_timestamp();
  v_table      TEXT;
  v_inserted   INTEGER := 0;
  v_rejected   INTEGER := 0;
BEGIN
  IF p_pipeline_name !~ '^[a-z][a-z0-9_]{0,62}$' THEN
    RAISE EXCEPTION
      'acp_promote_staging_initial: invalid pipeline_name %', p_pipeline_name;
  END IF;

  v_table := format('acp_staging.%I', p_pipeline_name);

  -- EXCLUSIVE blocks concurrent extractors writing to this staging table for
  -- the duration of promote, but still allows reads. Concurrent SELECTs (e.g.
  -- monitoring) keep working.
  EXECUTE format('LOCK TABLE %s IN EXCLUSIVE MODE', v_table);

  -- Mark rows missing required fields as rejected so they don't get promoted
  -- and aren't silently dropped. They stay queryable in staging for triage.
  EXECUTE format($q$
    UPDATE %s
       SET last_error  = 'missing_required_field',
           promoted_at = now()
     WHERE batch_id   = $1
       AND mode       = 'initial'
       AND promoted_at IS NULL
       AND (
            subtype        IS NULL OR subtype        = ''
         OR canonical_name IS NULL OR canonical_name = ''
         OR source         IS NULL OR source         = ''
         OR source_ref     IS NULL OR source_ref     = ''
         OR context        IS NULL
       )
  $q$, v_table) USING p_batch_id;
  GET DIAGNOSTICS v_rejected = ROW_COUNT;

  -- Single bulk INSERT for all valid rows. ON CONFLICT DO NOTHING keeps re-runs
  -- idempotent and absorbs concurrent duplicates from incremental pipelines
  -- targeting the same canonical_name.
  EXECUTE format($q$
    INSERT INTO context_objects (subtype, source, source_ref, canonical_name, context)
    SELECT subtype, source, source_ref, canonical_name, context
      FROM %s
     WHERE batch_id   = $1
       AND mode       = 'initial'
       AND promoted_at IS NULL
       AND last_error IS NULL
    ON CONFLICT (subtype, canonical_name) DO NOTHING
  $q$, v_table) USING p_batch_id;
  GET DIAGNOSTICS v_inserted = ROW_COUNT;

  -- Stamp the staging rows we just promoted (including the no-op conflict
  -- rows — they're effectively reconciled).
  EXECUTE format($q$
    UPDATE %s
       SET promoted_at = now()
     WHERE batch_id   = $1
       AND mode       = 'initial'
       AND promoted_at IS NULL
       AND last_error IS NULL
  $q$, v_table) USING p_batch_id;

  -- One marker entry per batch instead of per row. Initial mode is a bulk
  -- load; agents reading change_log don't need 100M individual create events.
  INSERT INTO change_log (change_type, metadata, created_at)
  VALUES (
    'initial_load',
    jsonb_build_object(
      'pipeline_name',  p_pipeline_name,
      'batch_id',       p_batch_id,
      'inserted_count', v_inserted,
      'rejected_count', v_rejected
    ),
    now()
  );

  RETURN QUERY
    SELECT
      v_inserted,
      v_rejected,
      ((EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at))) * 1000)::INTEGER;
END;
$fn$;
