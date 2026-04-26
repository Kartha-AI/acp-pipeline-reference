-- 003_helper_functions.sql
--
-- jsonb helpers used by the /bulk drainer's deep-merge path in incremental
-- mode. Kept here (rather than in application code) so they can run inside
-- ACP's identity-merge SQL without round-tripping payloads to Node.
--
-- Both functions are pure; marked IMMUTABLE so the planner can fold them
-- into expression indexes if a customer ever needs them.

-- jsonb_deep_merge: recursive object merge.
--   * Right-hand side wins on scalar conflicts.
--   * Nested objects merge key-by-key.
--   * Arrays are replaced wholesale (no concat) — same shape as `||`.
--   * NULL on either side returns the other side.
CREATE OR REPLACE FUNCTION jsonb_deep_merge(a jsonb, b jsonb)
RETURNS jsonb
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
    WHEN a IS NULL THEN b
    WHEN b IS NULL THEN a
    WHEN jsonb_typeof(a) <> 'object' OR jsonb_typeof(b) <> 'object' THEN b
    ELSE COALESCE(
      (
        SELECT jsonb_object_agg(
          COALESCE(ka, kb),
          CASE
            WHEN va IS NULL THEN vb
            WHEN vb IS NULL THEN va
            WHEN jsonb_typeof(va) = 'object' AND jsonb_typeof(vb) = 'object'
              THEN jsonb_deep_merge(va, vb)
            ELSE vb
          END
        )
        FROM jsonb_each(a) AS ea(ka, va)
        FULL OUTER JOIN jsonb_each(b) AS eb(kb, vb) ON ka = kb
      ),
      '{}'::jsonb
    )
  END
$$;

-- jsonb_changed_paths: returns dot-paths whose values differ between a and b.
-- Recurses into objects; arrays and scalars are compared as whole values.
-- Used by incremental promote to write a precise change_log entry rather than
-- "the row changed".
CREATE OR REPLACE FUNCTION jsonb_changed_paths(a jsonb, b jsonb, prefix text DEFAULT '')
RETURNS text[]
LANGUAGE plpgsql
IMMUTABLE
AS $fn$
DECLARE
  v_paths    text[] := ARRAY[]::text[];
  v_key      text;
  v_left     jsonb;
  v_right    jsonb;
  v_full     text;
BEGIN
  IF a IS NULL AND b IS NULL THEN
    RETURN v_paths;
  END IF;

  IF a IS NULL OR b IS NULL
     OR jsonb_typeof(a) <> 'object'
     OR jsonb_typeof(b) <> 'object' THEN
    IF a IS DISTINCT FROM b THEN
      RETURN ARRAY[CASE WHEN prefix = '' THEN '$' ELSE prefix END];
    END IF;
    RETURN v_paths;
  END IF;

  FOR v_key IN
    SELECT key
    FROM (
      SELECT jsonb_object_keys(a) AS key
      UNION
      SELECT jsonb_object_keys(b)
    ) k
    ORDER BY key
  LOOP
    v_left  := a -> v_key;
    v_right := b -> v_key;
    v_full  := CASE WHEN prefix = '' THEN v_key ELSE prefix || '.' || v_key END;

    IF v_left IS DISTINCT FROM v_right THEN
      IF jsonb_typeof(v_left)  = 'object'
         AND jsonb_typeof(v_right) = 'object' THEN
        v_paths := v_paths || jsonb_changed_paths(v_left, v_right, v_full);
      ELSE
        v_paths := v_paths || v_full;
      END IF;
    END IF;
  END LOOP;

  RETURN v_paths;
END;
$fn$;
