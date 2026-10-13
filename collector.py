#!/usr/bin/env python3
"""
Snapshot odds for OPEN geopolitical markets on Kalshi + Polymarket.
Each run appends rows to the `ticks` table.

DB: md:geo_odds   -> MotherDuck cloud database (needs MOTHERDUCK_TOKEN env var)
    geo_odds.duckdb -> local file (default, for testing)
"""
import json, logging, os, sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import duckdb, pyarrow as pa, requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DB = os.getenv("DB", "geo_odds.duckdb")
KALSHI_CATEGORIES = os.getenv("KALSHI_CATEGORIES", "World").split(",")
POLY_TAGS = os.getenv("POLY_TAGS", "geopolitics").split(",")
MAX_SPREAD = 0.10  # wider book -> use last trade instead of mid

KALSHI = "https://api.elections.kalshi.com/trade-api/v2"
GAMMA = "https://gamma-api.polymarket.com"
NOW = datetime.now(timezone.utc).replace(microsecond=0)
log = logging.getLogger("collector")

SCHEMA = pa.schema([
    ("ts", pa.timestamp("s", tz="UTC")), ("venue", pa.string()),
    ("market_id", pa.string()), ("event_id", pa.string()),
    ("event_title", pa.string()), ("market_title", pa.string()),
    ("close_time", pa.timestamp("s", tz="UTC")),
    ("prob", pa.float64()), ("yes_bid", pa.float64()), ("yes_ask", pa.float64()),
    ("last", pa.float64()), ("volume", pa.float64()), ("volume_24h", pa.float64()),
])

http = requests.Session()
http.mount("https://", HTTPAdapter(max_retries=Retry(
    total=6, backoff_factor=1.5, status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=("GET",), respect_retry_after_header=True)))

def get(url, **params):
    r = http.get(url, params={k: v for k, v in params.items() if v is not None}, timeout=30)
    r.raise_for_status()
    return r.json()

def num(x):
    try: return float(x)
    except (TypeError, ValueError): return None

def when(x):
    try: return datetime.fromisoformat(str(x).replace("Z", "+00:00"))
    except ValueError: return None

def row(venue, market_id, event_id, event_title, market_title, close, bid, ask, last, vol, vol24):
    mid = (bid + ask) / 2 if bid is not None and ask is not None and 0 <= ask - bid <= MAX_SPREAD else None
    return dict(ts=NOW, venue=venue, market_id=market_id, event_id=event_id,
                event_title=event_title, market_title=market_title, close_time=when(close),
                prob=round(mid, 4) if mid is not None else last,
                yes_bid=bid, yes_ask=ask, last=last, volume=vol, volume_24h=vol24)

# ── Kalshi: category -> series -> open events (with nested markets) ───────────
def kalshi():
    series = {s["ticker"] for c in KALSHI_CATEGORIES
              for s in get(f"{KALSHI}/series", category=c).get("series") or []}

    def events(ticker):
        out, cursor = [], None
        while True:
            d = get(f"{KALSHI}/events", series_ticker=ticker, status="open",
                    with_nested_markets="true", limit=200, cursor=cursor)
            out += d.get("events") or []
            if not (cursor := d.get("cursor")): return out

    with ThreadPoolExecutor(4) as ex:
        for evs in ex.map(events, series):
            for e in evs:
                for m in e.get("markets") or []:
                    if m.get("status") == "active":
                        yield row("kalshi", m["ticker"], e["event_ticker"], e.get("title"),
                                  m.get("yes_sub_title") or m.get("title"), m.get("close_time"),
                                  num(m.get("yes_bid_dollars")), num(m.get("yes_ask_dollars")),
                                  num(m.get("last_price_dollars")),
                                  num(m.get("volume_fp")), num(m.get("volume_24h_fp")))

# ── Polymarket: tag -> open events (markets always nested) ───────────────────
def polymarket():
    seen = set()
    for tag in POLY_TAGS:
        cursor = None
        while True:
            d = get(f"{GAMMA}/events/keyset", tag_slug=tag, closed="false", limit=100, after_cursor=cursor)
            for e in d.get("events") or []:
                for m in e.get("markets") or []:
                    if m.get("closed") or not m.get("active") or m["id"] in seen:
                        continue
                    seen.add(m["id"])
                    yield row("polymarket", m["id"], e["id"], e.get("title"),
                              m.get("groupItemTitle") or m.get("question"), m.get("endDate"),
                              num(m.get("bestBid")), num(m.get("bestAsk")), num(m.get("lastTradePrice")),
                              num(m.get("volumeNum") or m.get("volume")), num(m.get("volume24hr")))
            if not (cursor := d.get("next_cursor")): break

# ── write ─────────────────────────────────────────────────────────────────────
DDL = """CREATE TABLE IF NOT EXISTS ticks (
  ts TIMESTAMPTZ, venue VARCHAR, market_id VARCHAR, event_id VARCHAR,
  event_title VARCHAR, market_title VARCHAR, close_time TIMESTAMPTZ,
  prob DOUBLE, yes_bid DOUBLE, yes_ask DOUBLE, last DOUBLE, volume DOUBLE, volume_24h DOUBLE)"""

def write(rows):
    if DB.startswith("md:"):                      # MotherDuck: create the database on first run
        con = duckdb.connect("md:")
        con.execute(f"CREATE DATABASE IF NOT EXISTS {DB[3:]}")
        con.execute(f"USE {DB[3:]}")
    else:
        con = duckdb.connect(DB)
    con.execute(DDL)
    con.register("batch", pa.Table.from_pylist(rows, schema=SCHEMA))  # one bulk insert
    con.execute("INSERT INTO ticks SELECT * FROM batch")
    con.close()

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    rows, failed = [], False
    for venue in (kalshi, polymarket):  # one venue failing doesn't block the other
        try:
            got = list(venue()); rows += got
            log.info("%-10s %5d markets", venue.__name__, len(got))
        except Exception:
            log.exception("%s failed", venue.__name__); failed = True
    if rows:
        write(rows)
    return 1 if failed or not rows else 0

if __name__ == "__main__":
    sys.exit(main())
