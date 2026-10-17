# pred_market_risk

Geopolitical event-risk data from prediction markets (Kalshi + Polymarket): probability history,
a point-in-time knowledge graph linking markets → propositions → entities → securities, and an API.

## Pipeline

| Script | Schedule | Writes |
|---|---|---|
| `collector.py` | every 15 min (Actions) | `market_snapshots` (source=`realtime`), `market_versions` |
| `graph_build.py` | after each collect | `graph_nodes`, `graph_edges` (diff only) |
| `prices.py` | weekdays 22:30 UTC | `security_prices` (Yahoo, research use only) |
| `backfill.py` | manual, one-time | `market_snapshots` (source=`backfill`) |
| `securities.py` | manual, after index rebalances | `securities.csv` (S&P 500 from Wikipedia; review and commit) |

All data lives in MotherDuck: put `MOTHERDUCK_TOKEN` in `.env` (copy `.env.example`; git-ignored) or the environment. `DB` defaults to `md:geo_odds`.

## API

```
pip install -r requirements.txt
uvicorn api:app      # reads .env; explorer at http://localhost:8000, docs at /docs
```

```
GET  /propositions?entity=country:TW&sort=move          # what moved, filtered by entity
GET  /propositions/{id}                                 # markets, entities, logical links
GET  /propositions/{id}/history?interval=1d&start=...   # blended probability series
GET  /securities/TSM/exposures                          # propositions a security is exposed to
POST /portfolio/scan  {"holdings":[{"ticker":"TSM","weight":0.1}]}
```

The same server hosts an interactive explorer at `/` (`web/index.html`): browse propositions
(sorted by 24h move), entities and securities; click anything to see its neighbourhood as a
network and its details (prices, markets and rules, Wikipedia description, exposures); set
"As of" to view the graph and prices at any past moment.

The explorer's **API** tab documents every endpoint with examples; `/docs` is the interactive reference.

Every read accepts `as_of` (state of the world at T) and `known_as_of` (what the graph knew at T);
set both to T for a point-in-time, backtest-safe view. Set `API_KEYS` to require `X-API-Key`.

## Scope

Every geopolitical market: Polymarket's `geopolitics` tag, plus Kalshi World/Politics markets that
involve a foreign state actor — a country other than the US, or a person/organisation belonging to
one, per Wikidata ("Will Trump visit China?" in, "Will the Senate confirm…?" out).

## Refreshing cached facts

Wikipedia/Wikidata lookups are fetched once and cached, so the routine refresh never repeats them.
Refresh them manually to pick up real-world changes. A refresh re-fetches, appends the new values
(old ones stay as history), and rebuilds the graph — only facts that actually changed are rewritten.

**Wikidata refresh — monthly.** Entity facts: type, parent country, statements such as *head of
state* or *citizen of* (e.g. a new leader). Takes a few minutes.

```bash
python graph_build.py --refresh wikidata
```

or GitHub: **Actions → refresh → Run workflow → wikidata**.

**Relatedness refresh — yearly.** Wikipedia co-occurrence counts behind entity ↔ entity link
strengths. Slow (around an hour at full scope).

```bash
python graph_build.py --refresh relatedness
```

or GitHub: **Actions → refresh → Run workflow → relatedness**.

The terminal commands need `MOTHERDUCK_TOKEN` in `.env`; the workflow uses the repository secret.

## How markets become propositions

`matching.py` parses every market into a contract signature — action, actors/targets, time window,
threshold, conditions — from venue fields and the venues' templated rules text, then compares them
field by field: `same_as` (merged, prices blended), `near_equivalent` (linked, never blended),
`implies` (A ⇒ B), or rejected with a reason. Prices are the final check: markets that the text
says are identical but trade more than 10 points apart are not merged.

## Curating the graph

- `seeds/match_overrides.csv` — human overrides of the matcher (`same_as`, `near_equivalent`,
  `implies`, `different`); always win. Review queue: `python graph_build.py --review > review.csv`.
- `seeds/exposures.csv` — entity → security exposures, keyed by Wikidata id. The only
  hand-written input; current weights are analyst placeholders, not sourced data.

## How entities are found and connected

`entities.py` regenerates the entity list from the markets on every build: phrases in market
titles are linked to Wikipedia articles, Wikidata supplies each entity's type and parent country,
and entities are connected by Wikidata statements and by normalized co-occurrence on Wikipedia
(how many articles mention both, relative to each alone). Lookups are cached in MotherDuck.
