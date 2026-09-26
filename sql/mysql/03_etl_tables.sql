-- ETL control tables. In a bigger shop these live in a separate control database;
-- here they sit next to the source so one connection does everything.
USE olist;

-- one row per (table, run date): the exact window that run extracted
CREATE TABLE IF NOT EXISTS etl_watermark (
  table_name  VARCHAR(64) NOT NULL,
  run_date    VARCHAR(10) NOT NULL,
  low_ts      VARCHAR(26) NOT NULL,   -- exclusive lower bound (updated_at)
  low_row_id  BIGINT      NOT NULL,   -- tie-break for rows sharing low_ts
  high_ts     VARCHAR(26) NOT NULL,   -- inclusive upper bound
  high_row_id BIGINT      NOT NULL,
  row_count   INT         NOT NULL,
  PRIMARY KEY (table_name, run_date)
);

-- content hash of every document last sent to Cosmos DB, so reruns upsert nothing
CREATE TABLE IF NOT EXISTS etl_cosmos_sync (
  doc_id    VARCHAR(64) NOT NULL,
  container VARCHAR(32) NOT NULL,
  doc_hash  VARCHAR(64) NOT NULL,
  PRIMARY KEY (container, doc_id)
);
