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
- `graph_build.py` (runs after the collector): builds the knowledge graph into `graph_nodes` / `graph_edges` — bitemporal, append-only assertions (`op` assert/retract, `valid_from/valid_to` = valid time, `recorded_at` = transaction time); each run writes only the diff. Read through table macros `nodes_asof(valid_at, known_at)` / `edges_asof(...)`.
- **Market → proposition matching is algorithmic** (`matching.py`, decision 2026-10-16): each market is parsed into a contract signature (action from a fixed vocabulary, actor/target entities, time window from the venues' templated rules, threshold, qualifying conditions) and compared field by field → `same_as` (one proposition, blended) / `near_equivalent` / `implies` / rejected-with-reason. Guards: every distinctive word of one question must appear in the other's text; entities compare via top-level parents (Trump→US); windows that differ only in elapsed time, and any `same_as`, must be confirmed by prices within `PRICE_TOLERANCE` (the event may already have happened in the gap — e.g. "Trump visits China in 2026" at 99.95% vs a Kalshi market opened after his May visit at 88.5%). No LLM, no string similarity. Humans override via `seeds/match_overrides.csv` (always wins); `python graph_build.py --review` dumps matches, rejections and unparsed markets as CSV.
- **Entities are regenerated from the markets on every build** (`entities.py`, decision 2026-10-16; no hand-made entity lists or links). Candidate phrases in each question are looked up as Wikipedia titles/redirects (longest match, disambiguation pages skipped; a lone surname like "Xi" resolves to the unique already-linked entity containing it). Wikidata gives type (person = instance of human; country = has ISO code; organisation/place = has a country; else concept) and parent (citizenship for people, country otherwise — used by matching, e.g. Trump→US). Concepts are kept only if specific (< 1% of Wikipedia articles mention them) **and** related (≥ `MIN_RELATEDNESS`) to an actor in the same question.
- Entity ↔ entity edges: typed Wikidata statements (citizen_of, country, head_of_state, part_of, …) plus `related`, all weighted by **normalized co-occurrence on Wikipedia text** (the Normalized Google Distance formula, Cilibrasi & Vitányi 2007; same as Milne–Witten on links): counts of articles mentioning A, B and both via the search API. Prose mentions, not links — navboxes create fake co-citation (WuXi Biologics–TSMC: 332 shared links, 1 shared mention). Requires ≥ `MIN_COOCCURRENCE` (10) shared articles. Scored pairs: co-mentioned in a question, Wikidata-linked, or entity × exposure-entity. Google's own counts aren't used: no free API, and its hit counts are unstable estimates.
- All Wikipedia/Wikidata lookups are cached in MotherDuck `wiki_cache` (append-only, saved per batch). First build ≈ 13 min (~2.4k co-occurrence queries); later builds only fetch new phrases/pairs.
- Other edges: mutex + date/strike `implies` ladders within an event. **The only hand-written input is entity → security exposures** (`seeds/exposures.csv`, keyed by Wikidata QID — analyst priors, to be replaced by SEC revenue geography / ETF holdings / event betas; Wikipedia co-occurrence between entity and issuer is a candidate automatic prior). `EXCLUDE_REGEX` drops noise markets.
- `web/index.html`: single-page graph explorer served by the API at `/` (vis-network from CDN; list | network | detail; as-of time travel). Uses `GET /graph/{node_id}` (neighbourhood, markets hidden, top-`fanout` edges per node, ≤3 mutex) and `GET /securities`. Colours = validated categorical slots 1–3 (proposition/entity/security) + distinct shapes + visible labels.
- `api.py` (FastAPI; `uvicorn api:app`, docs at `/docs`): `/propositions`, `/propositions/{id}`, `/history`, `/graph`, `/entities`, `/entities/{id}`, `/securities/{ticker}/exposures`, `POST /portfolio/scan`, `/health`. Every read takes `as_of` (valid time) and `known_as_of` (transaction time). Proposition prob = OI-weighted (else equal) blend of its markets' latest price ≤ as_of within 7 days; `near_equivalent` propositions are linked, never blended. Live-only by default (`include_closed`). Exposure score = path product, a relevance rank — not a beta. Optional `API_KEYS` env → `X-API-Key` auth. Not hosted yet; goal is a public deployment (Render free tier proposed). **No data stored locally** — everything lives in MotherDuck (`DB` defaults to `md:geo_odds`).
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