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
--   source = 'hourly' / 'daily': the same, for markets in less-traded events, snapshotted less
--     often (collector.tier); 'realtime' rows come every run. All sources feed v_market_prob.
--   volume = the market's lifetime $ volume (collector rows from 2026-10-27; NULL before).
CREATE TABLE IF NOT EXISTS market_snapshots (
  source VARCHAR, run_id VARCHAR, ts TIMESTAMPTZ, venue VARCHAR, market_id VARCHAR, status VARCHAR,
  yes_bid DOUBLE, yes_ask DOUBLE, last DOUBLE, volume_24h DOUBLE, open_interest DOUBLE);
ALTER TABLE market_snapshots ADD COLUMN IF NOT EXISTS volume DOUBLE;

-- Daily security prices (prices.py). `date` is the exchange-local trading date.
CREATE TABLE IF NOT EXISTS security_prices (
  ticker VARCHAR, date DATE, close DOUBLE, adj_close DOUBLE, volume DOUBLE, currency VARCHAR,
  source VARCHAR, fetched_at TIMESTAMPTZ, run_id VARCHAR);

-- How each market ended (collector.py): one row when a market is first seen final. Calibration
-- (api.py /calibration) needs these, and they can't be backfilled once venues stop serving old
-- markets.
--   result = yes | no | void (cancelled, or settled at a split price) | scalar (settled at another value)
--   settlement_value = payout of the YES / first outcome in dollars (0-1)
CREATE TABLE IF NOT EXISTS market_resolutions (
  venue VARCHAR, market_id VARCHAR, status VARCHAR, result VARCHAR, settlement_value DOUBLE,
  resolved_at TIMESTAMPTZ, observed_at TIMESTAMPTZ, run_id VARCHAR);

-- Daily factor levels (prices.py) for the factors in seeds/factors.csv that have a `series`:
-- index / FX / futures closes from Yahoo, credit spreads and yields from FRED (in percent).
-- `date` is the source's own date (exchange-local for Yahoo, US for FRED). Latest fetch wins.
CREATE TABLE IF NOT EXISTS factor_prices (
  factor_id VARCHAR, series VARCHAR, date DATE, level DOUBLE, source VARCHAR,
  fetched_at TIMESTAMPTZ, run_id VARCHAR);

-- Measured question -> factor sensitivities (factor_betas.py), one full set per run; read the
-- latest run. beta = factor move (units: % for prices, bp for spreads / yields) per +1.0 change
-- in the question's probability; shrunk_* = empirical-Bayes estimate pooled across questions
-- with the same factor and channel (see factor_betas.py).
CREATE TABLE IF NOT EXISTS factor_betas (
  run_id VARCHAR, computed_at TIMESTAMPTZ, question_id VARCHAR, factor_id VARCHAR, channel VARCHAR,
  orientation DOUBLE, n_events INTEGER, beta DOUBLE, se DOUBLE, pooled_beta DOUBLE, pooled_se DOUBLE,
  tau DOUBLE, shrunk_beta DOUBLE, shrunk_se DOUBLE, units VARCHAR, events JSON);

-- Measured factor -> security loadings (security_betas.py), one full set per run; read the latest.
-- beta = the security's weekly % move per unit weekly move of the factor (% or bp), controlling for
-- the market (factor equity_us); for equity_us itself, the market beta.
CREATE TABLE IF NOT EXISTS security_loadings (
  run_id VARCHAR, computed_at TIMESTAMPTZ, ticker VARCHAR, factor_id VARCHAR,
  beta DOUBLE, se DOUBLE, t DOUBLE, n_weeks INTEGER);

-- Measured question -> security sensitivities (security_betas.py), % move of the security per +1.0
-- of the question's probability: implied (via the question's factor moves x the security's joint
-- factor loadings), beta / se (direct, market-adjusted, on the question's jump days), shrunk
-- (direct shrunk toward implied). source = factor | structural | both (how the pair was found).
CREATE TABLE IF NOT EXISTS security_betas (
  run_id VARCHAR, computed_at TIMESTAMPTZ, question_id VARCHAR, ticker VARCHAR, source VARCHAR,
  implied DOUBLE, implied_factors JSON, n_events INTEGER, beta DOUBLE, se DOUBLE, tau DOUBLE,
  shrunk DOUBLE, shrunk_se DOUBLE, units VARCHAR, events JSON);

-- Wikipedia / Wikidata lookups (entities.py), fetched once per key. Latest row per (kind, key) wins.
--   kind = title (phrase -> article), df (word -> share of articles containing it),
--          entity (Wikidata item), mentions (article title -> # articles mentioning it),
--          comentions ("A|B" -> # articles mentioning both), meta
CREATE TABLE IF NOT EXISTS wiki_cache (
  kind VARCHAR, key VARCHAR, value JSON, fetched_at TIMESTAMPTZ);

-- Knowledge graph (graph_build.py). Bitemporal and append-only: each row asserts (op='assert') or
-- retracts (op='retract') a node/edge. recorded_at = when the graph learned it (transaction time);
-- valid_from/valid_to = when it is true in the world (valid time).
CREATE TABLE IF NOT EXISTS graph_nodes (
  node_id VARCHAR, node_type VARCHAR,   -- market | proposition | question | entity | security | factor
  name VARCHAR, attrs JSON, attrs_hash VARCHAR, op VARCHAR,
  valid_from TIMESTAMPTZ, valid_to TIMESTAMPTZ, recorded_at TIMESTAMPTZ, run_id VARCHAR);

CREATE TABLE IF NOT EXISTS graph_edges (
  edge_id VARCHAR, src_id VARCHAR, dst_id VARCHAR,
  edge_type VARCHAR,   -- same_as | near_equivalent | mutex | implies | involves | relevant_to | <entity link> | exposure
  sign DOUBLE, weight DOUBLE, confidence DOUBLE, provenance VARCHAR, note VARCHAR,
  attrs JSON, attrs_hash VARCHAR, op VARCHAR,
  valid_from TIMESTAMPTZ, valid_to TIMESTAMPTZ, recorded_at TIMESTAMPTZ, run_id VARCHAR);

-- The graph as recorded by `known_at`, restricted to what was true at `valid_at`.
CREATE OR REPLACE MACRO nodes_asof(valid_at, known_at) AS TABLE
SELECT * EXCLUDE (op, attrs_hash) FROM (
  SELECT * FROM graph_nodes WHERE recorded_at <= known_at
  QUALIFY row_number() OVER (PARTITION BY node_id ORDER BY recorded_at DESC) = 1)
WHERE op = 'assert' AND (valid_from IS NULL OR valid_from <= valid_at)
  AND (valid_to IS NULL OR valid_to > valid_at);

CREATE OR REPLACE MACRO edges_asof(valid_at, known_at) AS TABLE
SELECT * EXCLUDE (op, attrs_hash) FROM (
  SELECT * FROM graph_edges WHERE recorded_at <= known_at
  QUALIFY row_number() OVER (PARTITION BY edge_id ORDER BY recorded_at DESC) = 1)
WHERE op = 'assert' AND (valid_from IS NULL OR valid_from <= valid_at)
  AND (valid_to IS NULL OR valid_to > valid_at)
  AND src_id IN (SELECT node_id FROM nodes_asof(valid_at, known_at))
  AND dst_id IN (SELECT node_id FROM nodes_asof(valid_at, known_at));

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
