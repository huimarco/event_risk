-- Schema: only what the knowledge graph and event betas need. Append-only: scripts only INSERT.
-- Timestamps are UTC. Prices are in dollars (0-1) of the YES / first outcome.

-- One row per script run (collector / backfill / prices), including failed ones, so gaps are visible.
CREATE TABLE IF NOT EXISTS collection_runs (
  run_id VARCHAR, job VARCHAR, started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ,
  status VARCHAR,            -- ok | partial | failed
  code_version VARCHAR, stats JSON, errors VARCHAR);

-- Contract terms, for cross-venue matching, logical edges and entity extraction.
-- New row only when the hash of the terms changes.
CREATE TABLE IF NOT EXISTS market_versions (
  venue VARCHAR, market_id VARCHAR, version_no INTEGER, content_hash VARCHAR,
  first_seen_at TIMESTAMPTZ, run_id VARCHAR,
  event_id VARCHAR, series_id VARCHAR, event_title VARCHAR, title VARCHAR, subtitle VARCHAR,
  rules_primary VARCHAR, rules_secondary VARCHAR, resolution_sources JSON, outcomes JSON,
  strike_type VARCHAR, floor_strike DOUBLE, cap_strike DOUBLE,
  open_time TIMESTAMPTZ, close_time TIMESTAMPTZ, expiration_time TIMESTAMPTZ,
  can_close_early BOOLEAN, neg_risk BOOLEAN, condition_id VARCHAR, clob_token_ids JSON, tags JSON);

-- Probability inputs + liquidity, per market per observation.
--   source = 'realtime': collector.py sweep; ts = fetch time; status = venue status
--   source = 'backfill': backfill.py hourly history; ts = end of the hour; status NULL.
--     Kalshi rows have bid/ask/last/OI/volume_24h; Polymarket rows only have `last`
--     (the venue's historical price series).
CREATE TABLE IF NOT EXISTS market_snapshots (
  source VARCHAR, run_id VARCHAR, ts TIMESTAMPTZ, venue VARCHAR, market_id VARCHAR, status VARCHAR,
  yes_bid DOUBLE, yes_ask DOUBLE, last DOUBLE, volume_24h DOUBLE, open_interest DOUBLE);

-- Daily security prices (prices.py). `date` is the exchange-local trading date.
CREATE TABLE IF NOT EXISTS security_prices (
  ticker VARCHAR, date DATE, close DOUBLE, adj_close DOUBLE, volume DOUBLE, currency VARCHAR,
  source VARCHAR, fetched_at TIMESTAMPTZ, run_id VARCHAR);

-- Cleaned probability: bid/ask mid if spread <= 0.10, else last trade / price.
CREATE OR REPLACE VIEW v_market_prob AS
SELECT source, ts, venue, market_id,
       CASE WHEN yes_ask - yes_bid BETWEEN 0 AND 0.10 THEN round((yes_bid + yes_ask) / 2, 4)
            ELSE last END AS prob,
       open_interest, volume_24h
FROM market_snapshots;

-- Latest known terms per market.
CREATE OR REPLACE VIEW v_market_latest AS
SELECT * FROM market_versions
QUALIFY row_number() OVER (PARTITION BY venue, market_id ORDER BY version_no DESC) = 1;
