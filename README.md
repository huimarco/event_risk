# pred_market_risk

Geopolitical event-risk data from prediction markets (Kalshi + Polymarket): probability history,
a point-in-time knowledge graph linking markets → propositions → questions → entities → issuers →
securities and market factors, and an API. Issuer → security is reference data; how much a security
moves with a factor or question is measured from prices (`security_betas.py`), not written down.

## Pipeline

| Script | Workflow (GitHub Actions) | Schedule | Writes |
|---|---|---|---|
| `collector.py` + `graph_build.py` | `collect_markets_and_update_graph` | every 15 min | `market_versions` (every non-excluded market), `market_snapshots` (realtime / hourly / daily by event volume), `market_resolutions`; graph only when its candidate markets change |
| `prices.py` + `factor_betas.py` + `security_betas.py` | `get_security_prices` | weekdays 22:30 UTC | `security_prices` (Yahoo, research use), `factor_prices` (Yahoo + FRED), `factor_betas`, `security_loadings`, `security_betas` |
| `backfill.py` | `backfill_market_history` | manual, one-time | `market_snapshots` (source=`backfill`) |
| `graph_build.py --refresh …` | `refresh_wikipedia_facts` | manual | re-fetched Wikipedia/Wikidata facts, then graph |
| `issuers.py` | — (run locally) | after index rebalances | `issuers.csv` + `securities.csv` (S&P 500 from Wikipedia + hand-written rows; review and commit) |
| `backtest.py` | — (run locally) | after graph or price changes | nothing; prints whether exposure priors predict returns when odds jump |

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
GET  /issuers/Q713418/exposures?sort=risk               # questions an issuer (TSMC) reaches through entities (prior)
GET  /securities/000660.KS/exposures                    # questions a security moves with, measured from prices
GET  /factors/credit_us_hy                              # latest level, linked questions, measured moves
GET  /questions/{id}/curve?date=2028-06-30              # deadline ladder as a hazard curve; P(by a maturity)
GET  /review/ladders                                    # rungs whose prices break the ladder's logic
GET  /calibration?horizon=7                             # how often a price came true (resolved markets)
GET  /markets?status=unparsed                           # match review: unmatched / unparsed / linked / merged
GET  /markets/{id}                                      # signature, parser issues, rejected candidates
```

The same server hosts an interactive explorer at `/` (`web/index.html`) with seven tabs: events (contracts; the
**Events** tab lists markets by match outcome, with their parsed signature and why each near miss was rejected),
questions (canonical events, sorted by 24h move), entities, macro factors, niche factors, issuers and GICS
industries (securities are reached from issuers and questions); click anything to see its neighbourhood as a
network and its details (prices, markets and rules, Wikipedia description, exposures).

The explorer's **API** tab documents every endpoint with examples; `/docs` is the interactive reference.

Every read serves the current graph and latest prices (point-in-time `as_of` reads are planned,
not exposed yet). Set `API_KEYS` to require `X-API-Key`.

## Excel add-in

The API also serves an Excel add-in (`excel/`: Office.js custom functions plus a task pane, no build
step). It works in Microsoft 365 Excel on Windows, Mac and the web. Office only loads add-ins over
HTTPS, so point it at the deployed server.

```
=ER.QUESTIONS("taiwan", 20)          table of questions (first column = id), biggest 24h movers first
=ER.ENTITY("Iran")                   questions involving an entity, live and most traded first
=ER.PROB(A2)  =ER.CHANGE(A2)         probability / 24h change for the id in A2 (q:, prop: or mkt:)
=ER.NAME(A2)  =ER.FIELD(A2, "updated")   wording / one detail (volume_24h, open_interest, headline, rules, ...)
=ER.RUNGS(A2)                        a question's deadlines / thresholds, with rung ids
=ER.CONTRACTS(A2)                    the Kalshi / Polymarket contracts behind it, with rules
=ER.HISTORY(A2, DATE(2026,9,1))      daily series (format column A as a date)
=ER.CURVE(A2)  =ER.PROB_BY(A2, B2)   term structure of a deadline question / P(event by a date, e.g. a maturity)
=ER.FACTORS(A2)                      factors it moves: move per +1.0 probability, if YES / if NO (% or bp)
=ER.FACTOR("credit_us_hy")           a factor's latest level (or "units", "name", "series")
=ER.SENSITIVITY(A2, "brent")         one factor's move per +1.0 probability
=ER.STRESS(A2, D2:D9, E2:E9, "yes")  Σ your loading x factor move if it resolves — computed in Excel
```

Ids: `q:` question (its probability = headline rung, the most-traded live one; it can switch), `prop:`
one rung, `mkt:` one venue contract. The local copy of the add-in uses `ERDEV.` so both can be installed.

The **Event Risk** button on the Home tab opens the pane: search questions and click one to add a
live watchlist row at the selected cell (id, name, prob, change, headline rung); **Stress sheet** adds a sheet for one
question with its factors, live moves if YES / NO and a column for your own loadings (P&L per 1% or per 1 bp), so
holdings stay in the workbook; set the API key there if the server has `API_KEYS` set.
Identical requests are cached for 60 s; **Refresh** in the pane forces a full recalculation.

Install by downloading `https://<your-domain>/excel/manifest.xml` (it already points at that server), then:
- **Excel on the web**: Insert → Add-ins → *Upload My Add-in* → choose the file.
- **Mac**: copy it to `~/Library/Containers/com.microsoft.Excel/Data/Documents/wef/`, restart Excel,
  then Insert → Add-ins → *My Add-ins*.
- **Windows**: put it in a network share, add the share under File → Options → Trust Center →
  Trusted Add-in Catalogs, restart, then Insert → My Add-ins → *Shared Folder*.
- **For a whole organisation**: an M365 admin uploads it under Admin center → Settings →
  Integrated apps. Corporate tenants often block sideloading, so this is the usual route.

Local development needs HTTPS on localhost: `npx office-addin-dev-certs install`, then
`uvicorn api:app --ssl-keyfile ~/.office-addin-dev-certs/localhost.key --ssl-certfile ~/.office-addin-dev-certs/localhost.crt`
and sideload `https://localhost:8000/excel/manifest.xml`.

## Deploying (Railway)

The API and explorer are one web service; GitHub Actions keeps collecting data into MotherDuck.
`railway.json` (start command + health check), `Procfile` (same command, for other hosts),
`.python-version` and the pinned `requirements.txt` are all Railway needs.

1. railway.com → sign in with GitHub → **New Project → Deploy from GitHub repo** → this repo.
2. Service → **Variables**: `MOTHERDUCK_TOKEN` (a read-only token is safest for a public
   service); optionally `API_KEYS=key1,key2` to require `X-API-Key` on data calls.
3. Service → **Settings → Networking → Generate Domain**. The explorer is at `/`, docs at `/docs`.

Every push to `main` redeploys.

## Collection

Everything on both venues except sports, entertainment / pop culture, mentions, social, crypto,
weather and financial price ranges (`KALSHI_EXCLUDE`, `POLY_EXCLUDE_TAGS` / `POLY_KEEP_TAGS`;
sports / games / esports are dropped by Polymarket's API itself). Contract terms are recorded for
every market; prices are snapshotted every run for the graph's core sources and events that have
traded ≥ $250k, hourly for events ≥ $25k, daily for the rest (`SNAPSHOT_*_VOLUME`). Measured
2026-10-27: ~75k open markets → ~9.6k realtime, ~17.8k hourly, ~45.6k daily (~1.4M snapshot rows/day,
3× before). The graph reads only its candidate sources (`graph_build.GRAPH_SOURCES`).

## Scope

Geopolitical and credit-relevant policy markets (`graph_build.scope`): Polymarket's `geopolitics`
tag, plus any market that involves a foreign state actor — a country other than the US, or a
person/organisation belonging to one, per Wikidata ("Will Trump visit China?" in, "Will the Senate
confirm…?" out) — or asks about a policy topic that moves credit and rates (Fed rates, defaults and
restructurings, IMF programmes, shutdowns, debt ceilings, tariffs, sanctions), US-only or not.

Sources: Kalshi `World,Politics` (`KALSHI_CATEGORIES`) and Polymarket `geopolitics` (`POLY_TAGS`)
in full; Kalshi `Elections,Economics` (`KALSHI_SCOPED_CATEGORIES`) and Polymarket
`world,world-elections,tariffs,fed-rates,economic-policy` (`POLY_SCOPED_TAGS`) only for events
that pass the scope test and have traded at least `SCOPED_MIN_VOLUME` ($25k; drops US House,
provincial and mayoral races).

## Refreshing cached facts

Wikipedia/Wikidata lookups are fetched once and cached, so the routine refresh never repeats them.
Refresh them manually to pick up real-world changes. A refresh re-fetches, appends the new values
(old ones stay as history), and rebuilds the graph — only facts that actually changed are rewritten.

**Wikidata refresh — monthly.** Entity facts: type, parent country, statements such as *head of
state* or *citizen of* (e.g. a new leader). Takes a few minutes.

```bash
python graph_build.py --refresh wikidata
```

or GitHub: **Actions → refresh_wikipedia_facts → Run workflow → wikidata**.

**Relatedness refresh — yearly.** Wikipedia co-occurrence counts behind entity ↔ entity link
strengths. Slow (around an hour at full scope).

```bash
python graph_build.py --refresh relatedness
```

or GitHub: **Actions → refresh_wikipedia_facts → Run workflow → relatedness**.

The terminal commands need `MOTHERDUCK_TOKEN` in `.env`; the workflow uses the repository secret.

## How markets become propositions

`matching.py` parses every market into a contract signature — action, actors/targets, time window,
threshold, conditions — from venue fields and the venues' templated rules text, then compares them
field by field: `same_as` (merged, prices blended), `near_equivalent` (linked, never blended),
`implies` (A ⇒ B), or rejected with a reason. Prices are the final check: markets that the text
says are identical but trade more than 10 points apart are not merged. Within one venue the
operative rules are compared as well (names in the rules that neither question has mean different
bets), and a negated question ("agrees *not* to join", "military action *ends*") never matches the
plain one.

## Direction and size of impact

Each `involves` edge carries the entity's role in the contract (actor / target / party / concept)
and what YES means for it: `matching.EFFECTS` maps each action to a channel (conflict, trade,
sanctions, instability, diplomacy), a severity and a direction for target and actor; negated
questions flip it. `seeds/channel_signs.csv` lists the industries that *gain* from escalation on
a channel (defense, oil producers, fertilizers); every other exposure suffers with its entity.
`/issuers/{id}/exposures` sums over every path: `impact` = Σ sign × severity × role weight ×
pass-through × exposure weight (> 0: expected to rise if the event happens), `score` = relevance,
`risk` = |impact| × √(p(1−p)). These are priors in graph units; `python backtest.py` checks them
against abnormal returns on days odds jump (magnitude within each issuer's reference stock, direction hit rate,
calibration slope). First run (2026-10-18, 62 priced securities, daily data): direction at chance
(~50%), so the priors are not yet validated.

## Curating the graph

- `seeds/match_overrides.csv` — human overrides of the matcher (`same_as`, `near_equivalent`,
  `implies`, `different`); always win. Review queue: `python graph_build.py --review > review.csv`.
- `issuers.csv` — issuers: `issuer_id` = Wikidata id, or `local:<name>` for a borrower with no
  Wikidata item (add private borrowers here).
- `securities.csv` — tickers (Yahoo symbols) with their `issuer_id` (blank for funds) and `primary`
  listing. Only the issuer link is written down; sensitivities are measured by `security_betas.py`.
- `seeds/exposures.csv` — entity → issuer exposures, keyed by Wikidata id and `issuer_id`. Hand-written;
  current weights are analyst placeholders, not sourced data.
- `seeds/channel_signs.csv` — which sub-industries / issuers gain (−1) rather than suffer from an
  adverse event on a channel. Hand-written priors, like exposures.
- `seeds/niche_factors.csv`, `seeds/factor_links.csv`, `seeds/gics.csv` — the transmission layers
  (hand-written analyst priors, 2026-10-27): ~370 niche factors (each GICS industry's revenue / cost /
  investment drivers, with the sign of their effect on earnings), ~360 signed factor → factor links
  (macro → macro, macro → niche) and the GICS structure that maps issuers' sub-industries to codes.
  Written as `drives`, `driver_of` (niche factor → `gics:<industry>`) and `classified_as` (issuer → industry) edges.
- `seeds/factors.csv` + `seeds/entity_factors.csv` — macro factors (country equities, FX, oil,
  freight, policy rates…) and which entities make a question relevant to each, on which action
  channels and in which contract roles. Qualitative links (no weights), derived by `factors.py`
  during the graph build and stored as `factor` nodes and `relevant_to` edges. Each factor has a
  `series` (`yahoo:<symbol>` or `fred:<id>`) and `units` (% or bp); `factor_betas.py` measures
  every link: the factor's two-day move after lasting ≥5-pt jumps in the question's odds,
  regressed on the jump, then shrunk toward questions sharing the factor and channel.
  `python factor_betas.py --dry-run` prints the pooled estimates without writing.

## How entities are found and connected

`entities.py` regenerates the entity list from the markets on every build: phrases in market
titles are linked to Wikipedia articles, Wikidata supplies each entity's type and parent country,
and entities are connected by Wikidata statements and by normalized co-occurrence on Wikipedia
(how many articles mention both, relative to each alone). Lookups are cached in MotherDuck.
