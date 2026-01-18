-- 🔥 DEV MODE: always start clean
DROP TABLE IF EXISTS snapshots CASCADE;

CREATE TABLE snapshots (
  ts           TIMESTAMPTZ NOT NULL,
  symbol       TEXT        NOT NULL,

  best_bid_px  DOUBLE PRECISION,
  best_bid_sz  BIGINT,
  best_ask_px  DOUBLE PRECISION,
  best_ask_sz  BIGINT,

  mid          DOUBLE PRECISION,
  spread       DOUBLE PRECISION,

  PRIMARY KEY (symbol, ts)
);

CREATE INDEX snapshots_symbol_ts_idx
  ON snapshots (symbol, ts DESC);
