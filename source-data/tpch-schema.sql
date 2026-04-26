-- TPC-H subset used by the reference pipeline. Loaded into source-pg by
-- docker-compose on first boot. Column names follow the TPC-H spec
-- (`c_*`, `n_*`, etc.) so anyone familiar with the benchmark can read the
-- pipeline mapping at a glance.
--
-- updated_at columns are non-canonical TPC-H but required for incremental
-- sync. They default to now() so seed data without explicit timestamps
-- still gets a reasonable watermark.

CREATE TABLE IF NOT EXISTS region (
  r_regionkey   INTEGER     PRIMARY KEY,
  r_name        VARCHAR(25) NOT NULL
);

CREATE TABLE IF NOT EXISTS nation (
  n_nationkey   INTEGER     PRIMARY KEY,
  n_name        VARCHAR(25) NOT NULL,
  n_regionkey   INTEGER     NOT NULL REFERENCES region(r_regionkey)
);

CREATE TABLE IF NOT EXISTS customer (
  c_custkey     BIGINT         PRIMARY KEY,
  c_name        VARCHAR(25)    NOT NULL,
  c_address     VARCHAR(40),
  c_nationkey   INTEGER        NOT NULL REFERENCES nation(n_nationkey),
  c_phone       VARCHAR(15),
  c_acctbal     DECIMAL(12, 2) NOT NULL DEFAULT 0,
  c_mktsegment  VARCHAR(10),
  c_comment     VARCHAR(117),
  updated_at    TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
  o_orderkey       BIGINT         PRIMARY KEY,
  o_custkey        BIGINT         NOT NULL REFERENCES customer(c_custkey),
  o_orderstatus    CHAR(1)        NOT NULL,
  o_totalprice     DECIMAL(12, 2) NOT NULL,
  o_orderdate      DATE           NOT NULL,
  o_orderpriority  VARCHAR(15),
  updated_at       TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_customer_nationkey ON customer(c_nationkey);
CREATE INDEX IF NOT EXISTS idx_customer_updated   ON customer(updated_at);
CREATE INDEX IF NOT EXISTS idx_orders_custkey     ON orders(o_custkey);
CREATE INDEX IF NOT EXISTS idx_orders_updated     ON orders(updated_at);

-- Bump updated_at on every row mutation so incremental sync's
-- `WHERE updated_at > $since` filter picks up real changes. Without this,
-- a customer-style UPDATE that only touches business columns would leave
-- updated_at static and the next incremental run would miss the change.
CREATE OR REPLACE FUNCTION tpch_touch_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS customer_set_updated_at ON customer;
CREATE TRIGGER customer_set_updated_at
  BEFORE UPDATE ON customer
  FOR EACH ROW EXECUTE FUNCTION tpch_touch_updated_at();

DROP TRIGGER IF EXISTS orders_set_updated_at ON orders;
CREATE TRIGGER orders_set_updated_at
  BEFORE UPDATE ON orders
  FOR EACH ROW EXECUTE FUNCTION tpch_touch_updated_at();
