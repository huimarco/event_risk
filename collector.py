#!/usr/bin/env python3
"""
Collector for Kalshi + Polymarket: every open market except the excluded categories (sports,
entertainment, mentions, social, crypto, weather, financial price ranges), so the market universe
is on record even before the graph uses it (see schema.sql):
  market_versions     contract terms of every collected market, new row only when they change
  market_snapshots    probability inputs + liquidity for markets not yet final, at a frequency set
                      by how much their event has traded (lifetime $ volume, all its markets):
                        realtime  every run (15 min): the graph's core sources (Kalshi World /
                                  Politics, Polymarket geopolitics) and events >= SNAPSHOT_REALTIME_VOLUME
                        hourly    first run of each hour: events >= SNAPSHOT_HOURLY_VOLUME
                        daily     first run of each UTC day: the rest
                      (~75k open markets, ~28k in events >= $25k: snapshotting all every run
                      would fill the free MotherDuck tier within weeks)
  market_resolutions  how each market ended, once it is final (checked for markets that left the
                      live sweep, up to RESOLUTION_MAX per run, in batches)
  collection_runs     one row per run, even when it fails

Kalshi: every category except KALSHI_EXCLUDE (the category is stored as the market's tag).
Polymarket: every open event except those tagged with POLY_EXCLUDE_TAGS and none of
POLY_KEEP_TAGS ("Fed decision" is tagged finance and economy: kept). Which collected markets the
knowledge graph uses is graph_build's decision (graph_build.GRAPH_SOURCES and scope()).

backfill.py adds older hourly history (source='backfill'); prices.py loads security prices.

DB: md:geo_odds (default) -> MotherDuck cloud database (needs MOTHERDUCK_TOKEN env var).
    All data lives in MotherDuck; a local file path works only for throwaway tests.
"""
import hashlib, json, logging, os, re, sys, uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import duckdb, pyarrow as pa, requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def load_env(path=Path(__file__).parent / ".env"):
    """KEY=value lines from a local .env (git-ignored) into the environment; real env vars win."""
    if path.exists():
        for line in path.read_text().splitlines():
            key, sep, value = line.partition("=")
            key, value = key.strip(), value.split(" #")[0].strip().strip("\"'")
            if sep and key and value and not key.startswith("#"):  # blank placeholders are skipped
                os.environ.setdefault(key, value)

load_env()
DB = os.getenv("DB", "md:geo_odds")
env_set = lambda k, d: {x.strip() for x in os.getenv(k, d).split(",") if x.strip()}
KALSHI_EXCLUDE = env_set("KALSHI_EXCLUDE", "Sports,Entertainment,Mentions,Social,Crypto,Climate and Weather,Financials")
POLY_EXCLUDE_TAGS = env_set("POLY_EXCLUDE_TAGS", "sports,esports,games,pop-culture,culture,entertainment,music,movies,"
                                                 "tv,mentions,crypto,weather,climate,finance")
# Dropped by Gamma itself (exclude_tag_id), so their ~370k markets are never downloaded. These tags
# never share an event with a POLY_KEEP_TAGS tag in practice; the rest are filtered here.
POLY_SERVER_EXCLUDE = env_set("POLY_SERVER_EXCLUDE", "sports,games,esports")
POLY_KEEP_TAGS = env_set("POLY_KEEP_TAGS", "politics,geopolitics,world,elections,world-elections,economy,economic-policy,"
                                           "fed-rates,tariffs,tech,ai,business,science")
KALSHI_REALTIME = env_set("KALSHI_REALTIME", "World,Politics")  # the graph's core sources: snapshotted every run
POLY_REALTIME = env_set("POLY_REALTIME", "geopolitics")
SNAPSHOT_REALTIME_VOLUME = float(os.getenv("SNAPSHOT_REALTIME_VOLUME", "250000"))  # $ per event
SNAPSHOT_HOURLY_VOLUME = float(os.getenv("SNAPSHOT_HOURLY_VOLUME", "25000"))
RESOLUTION_MAX = int(os.getenv("RESOLUTION_MAX", "2000"))  # unresolved markets checked per run
FINAL = {"finalized", "settled", "resolved", "archived"}  # statuses after which prices stop moving

KALSHI = "https://api.elections.kalshi.com/trade-api/v2"
GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
DATA = "https://data-api.polymarket.com"
log = logging.getLogger("collector")

http = requests.Session()
http.mount("https://", HTTPAdapter(pool_maxsize=16, max_retries=Retry(
    total=6, backoff_factor=1.5, status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=("GET",), respect_retry_after_header=True)))

def get(url, params=None, **kw):
    if isinstance(params, dict):
        params = {k: v for k, v in params.items() if v is not None}
    r = http.get(url, params=params, timeout=30, **kw)
    r.raise_for_status()
    return r.json()

def now():
    return datetime.now(timezone.utc)

def num(x):
    try: return float(x)
    except (TypeError, ValueError): return None

def when(x):
    if not x: return None
    try: return datetime.fromisoformat(str(x).replace("Z", "+00:00"))
    except ValueError: return None

def loads(x):
    try: return json.loads(x) if isinstance(x, str) else x
    except ValueError: return None

def chunks(xs, n):
    xs = list(xs)
    return (xs[i:i + n] for i in range(0, len(xs), n))

# A record is one market as seen by a venue call: its terms (contract), current quote, and the
# text the theme filter runs on.
def record(venue, market_id, status, ts, contract, quote, volume, text, event_volume=None, core=False):
    """core = from the graph's core sources (snapshotted every run); event_volume = lifetime $
    traded across the market's event (sets the snapshot frequency)."""
    return dict(venue=venue, market_id=market_id, status=status, ts=ts, contract=contract,
                quote=quote, volume=volume, text=text, event_volume=event_volume, core=core)

# ── Kalshi ────────────────────────────────────────────────────────────────────
def kalshi_record(e, m, ts):
    contract = dict(
        event_id=e["event_ticker"], series_id=e.get("series_ticker"), event_title=e.get("title"),
        title=m.get("title"), subtitle=m.get("yes_sub_title"),
        rules_primary=m.get("rules_primary"), rules_secondary=m.get("rules_secondary"),
        resolution_sources=[s.get("url") or s.get("name") for s in e.get("settlement_sources") or []],
        outcomes=None, strike_type=m.get("strike_type"),
        floor_strike=num(m.get("floor_strike")), cap_strike=num(m.get("cap_strike")),
        open_time=m.get("open_time"), close_time=m.get("close_time"),
        expiration_time=m.get("latest_expiration_time") or m.get("expiration_time"),
        can_close_early=m.get("can_close_early"), neg_risk=e.get("mutually_exclusive"),
        condition_id=None, clob_token_ids=None, tags=[e["category"]] if e.get("category") else None,
        extra={k: m.get(k) for k in ("no_sub_title", "early_close_condition", "custom_strike",
                                     "market_type", "expected_expiration_time")}
              | {"event_sub_title": e.get("sub_title"), "strike_period": e.get("strike_period")})
    quote = dict(yes_bid=num(m.get("yes_bid_dollars")), yes_ask=num(m.get("yes_ask_dollars")),
                 last=num(m.get("last_price_dollars")), volume_24h=num(m.get("volume_24h_fp")),
                 open_interest=num(m.get("open_interest_fp")))
    text = " ".join(filter(None, (e.get("series_ticker"), e.get("title"), e.get("sub_title"),
                                  m.get("title"), m.get("yes_sub_title"))))
    return record("kalshi", m["ticker"], m.get("status"), ts, contract, quote,
                  num(m.get("volume_fp") or m.get("volume")), text)

def kalshi_sweep():
    """All open events outside KALSHI_EXCLUDE. One global pass is cheaper than per-series calls."""
    cursor = None
    while True:
        ts = now()
        d = get(f"{KALSHI}/events", dict(status="open", with_nested_markets="true",
                                         limit=200, cursor=cursor))
        for e in d.get("events") or []:
            if (cat := e.get("category")) and cat not in KALSHI_EXCLUDE:
                recs = [kalshi_record(e, m, ts) for m in e.get("markets") or []]
                total = sum(r["volume"] or 0 for r in recs)
                for r in recs:
                    yield r | dict(event_volume=total, core=cat in KALSHI_REALTIME)
        if not (cursor := d.get("cursor")): return

# ── Polymarket ────────────────────────────────────────────────────────────────
def poly_status(m):
    if m.get("archived"): return "archived"
    if m.get("closed"):
        return "resolved" if m.get("umaResolutionStatus") == "resolved" or m.get("automaticallyResolved") else "closed"
    return "active" if m.get("active") else "inactive"

def poly_record(e, m, ts):
    tags = sorted(t.get("slug") for t in e.get("tags") or [] if t.get("slug"))
    contract = dict(
        event_id=e["id"], series_id=None, event_title=e.get("title"),
        title=m.get("question"), subtitle=m.get("groupItemTitle"),
        rules_primary=m.get("description"), rules_secondary=None,
        resolution_sources=[s for s in (m.get("resolutionSource"), e.get("resolutionSource")) if s],
        outcomes=loads(m.get("outcomes")), strike_type=None, floor_strike=None, cap_strike=None,
        open_time=m.get("startDate"), close_time=m.get("endDate"), expiration_time=None,
        can_close_early=None, neg_risk=m.get("negRisk"), condition_id=m.get("conditionId"),
        clob_token_ids=loads(m.get("clobTokenIds")), tags=tags,
        extra={"event_description": e.get("description"), "question_id": m.get("questionID"),
               "slug": m.get("slug"), "event_neg_risk": e.get("negRisk")})
    quote = dict(yes_bid=num(m.get("bestBid")), yes_ask=num(m.get("bestAsk")),
                 last=num(m.get("lastTradePrice")), volume_24h=num(m.get("volume24hr")),
                 open_interest=None)  # filled by poly_oi
    text = " ".join(filter(None, [e.get("title"), m.get("question"), m.get("groupItemTitle")] + tags))
    return record("polymarket", m["id"], poly_status(m), ts, contract, quote,
                  num(m.get("volumeNum") or m.get("volume")), text)

def poly_tag_ids(slugs):
    return [i for s in sorted(slugs) if (i := get(f"{GAMMA}/tags/slug/{s}").get("id"))]

def poly_events(tag, closed, exclude=()):
    """Events, optionally under one tag; `exclude` = tag ids Gamma drops server-side."""
    cursor = None
    while True:
        ts = now()
        d = get(f"{GAMMA}/events/keyset", [(k, v) for k, v in dict(tag_slug=tag, closed=closed, limit=100,
                                                                   after_cursor=cursor).items() if v is not None]
                + [("exclude_tag_id", i) for i in exclude])
        for e in d.get("events") or []:
            yield e, ts
        if not (cursor := d.get("next_cursor")): return

def poly_keep(e):
    tags = {t.get("slug") for t in e.get("tags") or []}
    return bool(tags & POLY_KEEP_TAGS) or not tags & POLY_EXCLUDE_TAGS

def poly_sweep():
    """Every open event that poly_keep() accepts."""
    seen = set()
    for e, ts in poly_events(None, "false", poly_tag_ids(POLY_SERVER_EXCLUDE)):
        if not poly_keep(e): continue
        core = bool({t.get("slug") for t in e.get("tags") or []} & POLY_REALTIME)
        for m in e.get("markets") or []:
            if m["id"] not in seen:
                seen.add(m["id"])
                yield poly_record(e, m, ts) | dict(event_volume=num(e.get("volume")), core=core)

def poly_oi(recs):
    """Open interest isn't on Gamma; fetch from the Data API. Its WAF sporadically 403s some
    batches of condition ids, so split a refused batch until it passes (lone refusals are skipped)."""
    by_cond = {r["contract"]["condition_id"]: r for r in recs if r["contract"]["condition_id"]}
    def fetch(batch):
        try: return get(f"{DATA}/oi", dict(market=",".join(batch)))
        except requests.HTTPError as err:
            if err.response is None or err.response.status_code != 403: raise
            if len(batch) == 1: return []
            return fetch(batch[:len(batch) // 2]) + fetch(batch[len(batch) // 2:])
    for batch in chunks(by_cond, 20):
        for x in fetch(batch):
            if (r := by_cond.get(x.get("market"))) is not None:
                r["quote"]["open_interest"] = num(x.get("value"))

# ── resolutions ───────────────────────────────────────────────────────────────
def iso(x):
    """Venue timestamps, including Polymarket's "2024-04-01 06:26:38+00"."""
    if isinstance(x, str) and re.search(r"[+-]\d\d$", x): x += ":00"
    return when(x)

def kalshi_resolution(m):
    if m.get("status") not in ("settled", "finalized"): return None
    result, value = (m.get("result") or "").lower(), num(m.get("settlement_value_dollars"))
    if result not in ("yes", "no"):  # scalar markets settle at a value; no value at all = voided
        result = "void" if value is None else "scalar"
    return dict(venue="kalshi", market_id=m["ticker"], status=m["status"], result=result,
                settlement_value=value if value is not None else 1.0 if result == "yes" else 0.0 if result == "no" else None,
                resolved_at=iso(m.get("settlement_ts")) or iso(m.get("close_time")))

def poly_resolution(m):
    if not m.get("closed") or not (m.get("umaResolutionStatus") == "resolved" or m.get("automaticallyResolved")):
        return None
    prices = [num(p) for p in loads(m.get("outcomePrices")) or []]
    first = prices[0] if prices else None
    result = ("yes" if first is not None and first >= 0.99 else "no" if first is not None and first <= 0.01 and
              any(p and p >= 0.99 for p in prices[1:]) else "void" if first in (None, 0.5) or not any(prices) else "scalar")
    return dict(venue="polymarket", market_id=str(m["id"]), status="resolved", result=result, settlement_value=first,
                resolved_at=iso(m.get("closedTime")) or iso(m.get("endDate")))

def resolutions(con, live, run_id):
    """Resolution rows for known markets that are not in this run's live sweep and have none yet:
    up to RESOLUTION_MAX of them, in random order (so markets that never settle can't starve the
    rest), looked up in batches of 100 (Kalshi: /markets, then /historical/markets for markets
    settled before Kalshi's archive cutoff; Polymarket: Gamma /markets)."""
    todo = [(v, m) for v, m in con.execute("""
        SELECT venue, market_id FROM (SELECT DISTINCT venue, market_id FROM market_versions) v
        WHERE NOT EXISTS (SELECT 1 FROM market_resolutions r WHERE r.venue = v.venue AND r.market_id = v.market_id)
        ORDER BY random()""").fetchall() if (v, m) not in live][:RESOLUTION_MAX]
    rows = []
    kalshi = [m for v, m in todo if v == "kalshi"]
    for batch in chunks(kalshi, 100):
        found = {m["ticker"]: m for m in get(f"{KALSHI}/markets", dict(tickers=",".join(batch), limit=100)).get("markets") or []}
        rest = [t for t in batch if t not in found]
        if rest:
            found |= {m["ticker"]: m for m in get(f"{KALSHI}/historical/markets",
                                                  dict(tickers=",".join(rest), limit=100)).get("markets") or []}
        rows += filter(None, map(kalshi_resolution, found.values()))
    for batch in chunks([m for v, m in todo if v == "polymarket"], 100):
        got = get(f"{GAMMA}/markets", [("id", i) for i in batch] + [("closed", "true"), ("limit", 100)])
        rows += filter(None, map(poly_resolution, got or []))
    observed = now()
    return len(todo), [dict(r, observed_at=observed, run_id=run_id) for r in rows]

# ── DB ────────────────────────────────────────────────────────────────────────
TS = pa.timestamp("ms", tz="UTC")
VERSION_COLS = ["event_id", "series_id", "event_title", "title", "subtitle", "rules_primary",
                "rules_secondary", "resolution_sources", "outcomes", "strike_type", "floor_strike",
                "cap_strike", "open_time", "close_time", "expiration_time", "can_close_early",
                "neg_risk", "condition_id", "clob_token_ids", "tags"]
JSON_COLS = {"resolution_sources", "outcomes", "clob_token_ids", "tags"}
TIME_COLS = {"open_time", "close_time", "expiration_time"}
FLOAT_COLS = {"yes_bid", "yes_ask", "last", "volume_24h", "open_interest", "floor_strike",
              "cap_strike", "close", "adj_close", "volume", "sign", "weight", "confidence",
              "settlement_value", "level", "orientation", "beta", "se", "pooled_beta", "pooled_se", "tau",
              "shrunk_beta", "shrunk_se", "t", "implied", "shrunk", "q", "p_effect"}

def pa_type(col):
    if col in ("ts", "first_seen_at", "fetched_at", "started_at", "finished_at", "valid_from",
               "valid_to", "recorded_at", "resolved_at", "observed_at", "computed_at") or col in TIME_COLS:
        return TS
    if col in FLOAT_COLS: return pa.float64()
    if col in ("can_close_early", "neg_risk", "linked"): return pa.bool_()
    if col in ("version_no", "n_events", "n_weeks", "rank", "n_evaluated", "n_live"): return pa.int32()
    if col == "date": return pa.date32()
    return pa.string()

def cursor(con):
    """A new cursor on `con` in the job's database: on MotherDuck a cursor is a fresh session that
    doesn't inherit connect()'s USE, so the graph macros (nodes_asof...) would not resolve."""
    cur = con.cursor()
    if DB.startswith("md:"): cur.execute(f"USE {DB[3:]}")
    return cur

def connect():
    if DB.startswith("md:"):                      # MotherDuck: create the database on first run
        con = duckdb.connect("md:")
        con.execute(f"CREATE DATABASE IF NOT EXISTS {DB[3:]}")
        con.execute(f"USE {DB[3:]}")
    else:
        con = duckdb.connect(DB)
    con.execute((Path(__file__).parent / "schema.sql").read_text())
    return con

def insert(con, table, rows):
    if not rows: return
    cols = list(rows[0])
    batch = pa.Table.from_pylist(rows, schema=pa.schema([(c, pa_type(c)) for c in cols]))
    con.register("batch", batch)
    con.execute(f"INSERT INTO {table} BY NAME SELECT * FROM batch")
    con.unregister("batch")

def load_versions(con):
    return {(v, m): (h, n) for v, m, h, n in con.execute("""
        SELECT venue, market_id, arg_max(content_hash, version_no), max(version_no)
        FROM market_versions GROUP BY ALL""").fetchall()}

def content_hash(contract):
    return hashlib.sha256(json.dumps(contract, sort_keys=True, default=str).encode()).hexdigest()[:16]

def version_rows(recs, versions, run_id):
    """market_versions rows for records whose terms changed; updates `versions` in place."""
    rows = []
    for r in recs:
        k, c = (r["venue"], r["market_id"]), r["contract"]
        h = content_hash(c)
        last_hash, last_no = versions.get(k, (None, 0))
        if h == last_hash: continue
        versions[k] = (h, last_no + 1)
        rows.append(dict(venue=k[0], market_id=k[1], version_no=last_no + 1, content_hash=h,
                         first_seen_at=r["ts"], run_id=run_id,
                         **{col: json.dumps(c[col]) if col in JSON_COLS and c[col] is not None
                            else when(c[col]) if col in TIME_COLS else c[col]
                            for col in VERSION_COLS}))
    return rows

def run_job(job, fn):
    """Run fn(con, run_id, errors, stats) -> ok and log it to collection_runs. Exit code 1 if not ok."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run_id, started, errors, stats, ok = uuid.uuid4().hex, now(), [], {}, False
    con = connect()
    try:
        ok = fn(con, run_id, errors, stats)
    except Exception as err:
        log.exception("%s failed", job); errors.append(f"{job}: {err!r}")
        try: con.rollback()
        except duckdb.Error: pass
    insert(con, "collection_runs", [dict(
        run_id=run_id, job=job, started_at=started, finished_at=now(),
        status="failed" if not ok else "partial" if errors else "ok",
        code_version=os.getenv("GITHUB_SHA"), stats=json.dumps(stats),
        errors="\n".join(errors) or None)])
    con.close()
    log.info("%s %s", job, stats)
    return 0 if ok else 1

# ── run ───────────────────────────────────────────────────────────────────────
def tier(r):
    if r["core"] or (r["event_volume"] or 0) >= SNAPSHOT_REALTIME_VOLUME: return "realtime"
    return "hourly" if (r["event_volume"] or 0) >= SNAPSHOT_HOURLY_VOLUME else "daily"

def due(con, t):
    """Tiers to snapshot this run: realtime always; hourly / daily when the last run that took
    them was in an earlier hour / UTC day (read from collection_runs, so a skipped cron run
    just moves them to the next one)."""
    last = dict(con.execute("""
        SELECT k, epoch(max(started_at)) FROM (
          SELECT started_at, unnest(['hourly', 'daily']) AS k, stats FROM collection_runs
          WHERE job = 'collector' AND status != 'failed' AND started_at > now() - INTERVAL 3 DAY)
        WHERE (stats->'snapshots'->>k)::INT > 0 GROUP BY 1""").fetchall())
    hour, day = t.replace(minute=0, second=0, microsecond=0), t.replace(hour=0, minute=0, second=0, microsecond=0)
    return {"realtime"} | {k for k, start in (("hourly", hour), ("daily", day)) if not last.get(k) or last[k] < start.timestamp()}

def collect(con, run_id, errors, stats):
    recs, ok = [], True
    for venue, sweep in (("kalshi", kalshi_sweep), ("polymarket", poly_sweep)):
        try:  # one venue failing doesn't block the other
            got = list(sweep()); recs += got; stats[venue] = len(got)
        except Exception as err:
            log.exception("%s sweep failed", venue); errors.append(f"{venue} sweep: {err!r}"); ok = False
    live = [r for r in recs if r["status"] not in FINAL]
    tiers = due(con, now())
    take = [r for r in live if tier(r) in tiers]
    try:  # open interest is a blend weight for graph markets; daily-tier markets go without
        poly_oi([r for r in take if r["venue"] == "polymarket" and tier(r) != "daily"])
    except Exception as err:
        log.exception("polymarket OI failed"); errors.append(f"polymarket oi: {err!r}")

    snaps = [dict(source=tier(r), run_id=run_id, ts=r["ts"], venue=r["venue"], market_id=r["market_id"],
                  status=r["status"], volume=r["volume"], **r["quote"]) for r in take]
    stats["snapshots"] = {k: sum(1 for x in snaps if x["source"] == k) for k in ("realtime", "hourly", "daily")}
    vers = version_rows(recs, load_versions(con), run_id)
    con.begin()
    insert(con, "market_snapshots", snaps)
    insert(con, "market_versions", vers)
    con.commit()
    stats |= {"market_snapshots": len(snaps), "market_versions": len(vers)}
    try:  # after the snapshot is saved: a slow venue lookup must not cost us the prices
        checked, res = resolutions(con, {(r["venue"], r["market_id"]) for r in live}, run_id)
        insert(con, "market_resolutions", res)
        stats |= {"resolutions_checked": checked, "market_resolutions": len(res)}
    except Exception as err:
        log.exception("resolutions failed"); errors.append(f"resolutions: {err!r}")
    return ok and bool(recs)

if __name__ == "__main__":
    sys.exit(run_job("collector", collect))
