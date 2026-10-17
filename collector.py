#!/usr/bin/env python3
"""
Real-time collector for geopolitical markets on Kalshi + Polymarket. Collects only what the
knowledge graph and event betas need (see schema.sql):
  market_snapshots  probability inputs + liquidity for every market not yet final (source='realtime')
  market_versions   contract terms, new row only when they change
  collection_runs   one row per run, even when it fails

backfill.py adds older hourly history (source='backfill'); prices.py loads security prices.

DB: md:geo_odds (default) -> MotherDuck cloud database (needs MOTHERDUCK_TOKEN env var).
    All data lives in MotherDuck; a local file path works only for throwaway tests.
"""
import hashlib, json, logging, os, re, sys, uuid
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
KALSHI_CATEGORIES = set(os.getenv("KALSHI_CATEGORIES", "World,Politics").split(","))
POLY_TAGS = os.getenv("POLY_TAGS", "geopolitics").split(",")
THEME_REGEX = re.compile(os.getenv("THEME_REGEX",  # MVP theme: US-China / Taiwan / semis
    r"china|chinese|taiwan|beijing|\bxi\b|jinping|\bprc\b|\bccp\b|\bpla\b|hong kong|south china sea"
    r"|tsmc|semiconductor|\bchips?\b|nvidia|export control|rare earth"), re.I)
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
def record(venue, market_id, status, ts, contract, quote, volume, text):
    return dict(venue=venue, market_id=market_id, status=status, ts=ts, contract=contract,
                quote=quote, volume=volume, text=text)

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
        condition_id=None, clob_token_ids=None, tags=None,
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
    """All open events in KALSHI_CATEGORIES. One global pass is cheaper than per-series calls."""
    cursor = None
    while True:
        ts = now()
        d = get(f"{KALSHI}/events", dict(status="open", with_nested_markets="true",
                                         limit=200, cursor=cursor))
        for e in d.get("events") or []:
            if e.get("category") in KALSHI_CATEGORIES:
                for m in e.get("markets") or []:
                    yield kalshi_record(e, m, ts)
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

def poly_events(tag, closed):
    cursor = None
    while True:
        ts = now()
        d = get(f"{GAMMA}/events/keyset", dict(tag_slug=tag, closed=closed, limit=100,
                                               after_cursor=cursor))
        for e in d.get("events") or []:
            yield e, ts
        if not (cursor := d.get("next_cursor")): return

def poly_sweep():
    seen = set()
    for tag in POLY_TAGS:
        for e, ts in poly_events(tag, "false"):
            for m in e.get("markets") or []:
                if m["id"] not in seen:
                    seen.add(m["id"])
                    yield poly_record(e, m, ts)

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

# ── DB ────────────────────────────────────────────────────────────────────────
TS = pa.timestamp("ms", tz="UTC")
VERSION_COLS = ["event_id", "series_id", "event_title", "title", "subtitle", "rules_primary",
                "rules_secondary", "resolution_sources", "outcomes", "strike_type", "floor_strike",
                "cap_strike", "open_time", "close_time", "expiration_time", "can_close_early",
                "neg_risk", "condition_id", "clob_token_ids", "tags"]
JSON_COLS = {"resolution_sources", "outcomes", "clob_token_ids", "tags"}
TIME_COLS = {"open_time", "close_time", "expiration_time"}
FLOAT_COLS = {"yes_bid", "yes_ask", "last", "volume_24h", "open_interest", "floor_strike",
              "cap_strike", "close", "adj_close", "volume", "sign", "weight", "confidence"}

def pa_type(col):
    if col in ("ts", "first_seen_at", "fetched_at", "started_at", "finished_at", "valid_from",
               "valid_to", "recorded_at") or col in TIME_COLS:
        return TS
    if col in FLOAT_COLS: return pa.float64()
    if col in ("can_close_early", "neg_risk"): return pa.bool_()
    if col == "version_no": return pa.int32()
    if col == "date": return pa.date32()
    return pa.string()

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
def collect(con, run_id, errors, stats):
    recs, ok = [], True
    for venue, sweep in (("kalshi", kalshi_sweep), ("polymarket", poly_sweep)):
        try:  # one venue failing doesn't block the other
            got = list(sweep()); recs += got; stats[venue] = len(got)
        except Exception as err:
            log.exception("%s sweep failed", venue); errors.append(f"{venue} sweep: {err!r}"); ok = False
    live = [r for r in recs if r["status"] not in FINAL]
    try:
        poly_oi([r for r in live if r["venue"] == "polymarket"])
    except Exception as err:
        log.exception("polymarket OI failed"); errors.append(f"polymarket oi: {err!r}")

    snaps = [dict(source="realtime", run_id=run_id, ts=r["ts"], venue=r["venue"],
                  market_id=r["market_id"], status=r["status"], **r["quote"]) for r in live]
    vers = version_rows(recs, load_versions(con), run_id)
    con.begin()
    insert(con, "market_snapshots", snaps)
    insert(con, "market_versions", vers)
    con.commit()
    stats |= {"market_snapshots": len(snaps), "market_versions": len(vers)}
    return ok and bool(recs)

if __name__ == "__main__":
    sys.exit(run_job("collector", collect))
