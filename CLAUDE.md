# event_risk — project context

## What this is
A data product for institutional investors built on prediction-market odds (Kalshi + Polymarket) for geopolitical events. Two value props:

1. **Preserved history that can't be backfilled** — not just prices (venues partially serve those) but order book depth, the full market universe at each point in time (incl. delisted/voided/renamed), contract rule versions, and resolution records.
2. **A point-in-time knowledge graph** linking markets → canonical propositions → entities → securities, with empirically estimated "event betas."

Positioning: risk-measurement infrastructure, not an alpha signal. Consensus numbers are a feature for risk desks; alpha-seekers get raw history to build their own models.

## Current state
- Collection is scoped to **only what the knowledge graph + event betas need** (decision 2026-10-15; the raw-data-product tables — universe events, resolutions, order books, raw payload archive — were dropped). Append-only DuckDB (`schema.sql`; local file or MotherDuck via `DB=md:...`). Free tier only — no paid infra; spotty data is fine as long as `collection_runs` makes gaps visible.
- `collector.py` (GitHub Actions, every 15 min): sweeps Kalshi `KALSHI_CATEGORIES` (default `World,Politics` — China/Taiwan series live under Politics; `World` alone is nearly empty) and Polymarket `POLY_TAGS` (default `geopolitics`). Writes `market_snapshots` (source=`realtime`; bid/ask/last/volume_24h/OI for non-final markets) and `market_versions` (contract terms, new row on hash change).
- `backfill.py` (manual workflow, one-time, re-runnable): hourly history for `THEME_REGEX` markets (US–China/Taiwan/semis), open and settled, volume ≥ `BACKFILL_MIN_VOLUME`, into `market_snapshots` with source=`backfill`. Kalshi candlesticks (5000/request; `/historical/...` for markets settled before Kalshi's cutoff); Polymarket `/prices-history` (≤15-day windows; price only, no bid/ask/OI).
- `prices.py` (daily workflow): daily close/adj close for `securities.csv` from Yahoo Finance's unofficial chart API into `security_prices`. Research only — not licensed; replace before selling betas.
- Views: `v_market_prob` (mid if spread ≤ 0.10 else last), `v_market_latest`.
- API quirks: Polymarket OI comes from Data API `/oi`, whose WAF 403s some batches (split and retry); Stooq now blocks scripted downloads.

## Data model (target)
**Nodes**
- `market` — venue contract (Kalshi ticker / Polymarket market id)
- `proposition` — venue-agnostic real-world question; probability = liquidity-weighted blend of equivalent markets
- `entity` — country, leader, org, chokepoint, commodity (IDs: ISO codes, Wikidata QIDs, LEI)
- `security` — equity, ETF, future, FX, bond/CDS (IDs: FIGI/ISIN/ticker)

**Edges** (each with sign, weight, confidence, provenance, `valid_from`/`valid_to`, `recorded_at`)
- market → proposition (`same_as` / `near_equivalent`, with resolution-difference note)
- proposition → proposition (logical: implies / mutually exclusive / ladder — deterministic from structure; causal/correlated — proposed, validated by co-movement)
- proposition → entity (`involves`, from entity extraction)
- entity → security (structural exposure: revenue geography, supply chain, domicile, commodity input)
- proposition → security (empirical event beta, shrunk toward structural prior)

Everything bitemporal and append-only so the graph and data can be reconstructed `as_of` any date.

## Methodology
1. **Canonicalization**: embed title + rules, LLM proposes cross-venue matches → human review queue. Don't blend markets whose resolution rules materially differ.
2. **Proposition probability**: single market = cleaned price; multiple equivalent = liquidity/spread-weighted average; ladders allow derived probabilities (e.g. P(between June and Dec) = P(by Dec) − P(by June)). Later: bias adjustment (fees, time value, favorite-longshot) calibrated on resolution history.
3. **Logical edges**: deterministic from date/strike ladders, Polymarket `negRisk`, Kalshi multi-market events.
4. **Entity → security**: reference data; weight = exposure share.
5. **Event beta**: detect probability jumps (e.g. |Δp| > 5pts within 1h, liquidity-filtered) → abnormal returns vs market/sector factor model, or regress returns on Δp with factor controls. Output β, SE, n_events, lag. Bayesian shrinkage toward structural prior (events are rare → noisy).
6. **Storage**: plain `nodes`/`edges` tables in DuckDB/Postgres for MVP; graph DB only if >3-hop queries become routine.

## Fields to add from venue APIs (verify against current docs — names change)
- **Kalshi**: `series_ticker`, category, `open_interest_fp`, `no_bid_dollars`, `no_ask_dollars`, `previous_price_dollars`, `rules_primary`, `rules_secondary`, `open_time`, `expiration_time`, `latest_expiration_time`, `can_close_early`, `result`, `settlement_value`, `strike_type`, `floor_strike`, `cap_strike`. `liquidity_dollars` reportedly returns 0 on public endpoints — derive depth from `/markets/{ticker}/orderbook`. Also: trades, candlesticks (backfill).
- **Polymarket (Gamma)**: `conditionId`, `clobTokenIds`, `liquidityNum`, `spread`, `oneDayPriceChange`, `oneWeekPriceChange`, `volume1wk`, `volume1mo`, `description`, `resolutionSource`, `startDate`, `negRisk`, `outcomes`, `outcomePrices`, `umaResolutionStatus`, event `tags`. Also CLOB `/book` (depth), `/prices-history` (backfill), Data API trades/holders.

## Delivery (how clients consume it)
- **REST API** (FastAPI): `GET /securities/{id}/exposures?as_of=`, `GET /propositions/{id}`, `GET /propositions/{id}/graph?depth=`, `POST /portfolio/scan`, plus raw-data endpoints (ticks, order books, contract versions, universe, resolutions) — all with `as_of`.
- **Bulk**: daily Parquet of nodes, edges, ticks, orderbooks (S3/Snowflake).
- **Alerts**: webhooks/Slack when propositions linked to holdings move; "unrepriced" flags.
- **Later**: Excel add-in, MCP server.
- Tiers: raw data (quants) → + graph (risk/PMs) → + alerts & portfolio scan.

## MVP scope
- One theme (default assumption: US–China / Taiwan / semiconductors), ~30–50 propositions, ~100–200 securities.
- Products: raw history API, geopolitical beta, portfolio scan.
- Priority: (1) market data for graph + betas (realtime + backfill + security prices — done), (2) propositions + mapping, (3) entities/securities, (4) event beta, (5) API + bulk export. Raw-data-product capture (resolutions, order books, universe history, payload archive) is deferred.

## Open risks
- **Licensing**: check Kalshi & Polymarket terms on commercial redistribution of raw quotes before selling raw data. Derived data (consensus probs, betas, graph) is on firmer ground.
- **Equity price data source** for event betas — needs a properly licensed feed for production.
- **Scheduler**: GitHub Actions is too coarse/unreliable for intraday order book capture; move to a real scheduler or websockets.
- Published betas lose alpha value with adoption — position as risk infrastructure.