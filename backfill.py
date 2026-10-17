#!/usr/bin/env python3
"""
One-time backfill of hourly price history for geopolitical markets (same scope as the graph:
graph_build.geopolitical), open and settled, into market_snapshots with source='backfill'. Also
records their terms in market_versions. Re-runnable: markets already backfilled are skipped.

  Kalshi:     every series in KALSHI_CATEGORIES -> events (all statuses) -> hourly candlesticks
              (markets settled before Kalshi's historical cutoff come from /historical/...)
  Polymarket: POLY_TAGS events (open + closed) -> CLOB /prices-history for the YES token,
              in 14-day windows (the API's max range at hourly fidelity)

Markets with lifetime volume < BACKFILL_MIN_VOLUME are skipped (too thin for event betas).
"""
import os, sys
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import entities, matching
from collector import (CLOB, KALSHI, KALSHI_CATEGORIES, POLY_TAGS, get, insert, kalshi_record,
                       load_versions, log, now, num, poly_events, poly_record, run_job,
                       version_rows, when)
from graph_build import EXCLUDE_REGEX, geopolitical

MIN_VOLUME = float(os.getenv("BACKFILL_MIN_VOLUME", "1000"))
HOUR = 3600
KALSHI_MAX_CANDLES = 5000   # per request
POLY_WINDOW = 14 * 86400    # max startTs..endTs span at hourly fidelity

def paged(url, key, **params):
    out, cursor = [], None
    while True:
        d = get(url, dict(params, limit=200, cursor=cursor))
        out += d.get(key) or []
        if not (cursor := d.get("cursor")): return out

# ── Kalshi ────────────────────────────────────────────────────────────────────
def kalshi_markets(con):
    """(record, is_historical) for every market in KALSHI_CATEGORIES series, any status."""
    series = [s["ticker"] for c in KALSHI_CATEGORIES
              for s in get(f"{KALSHI}/series", dict(category=c)).get("series") or []]
    with ThreadPoolExecutor(8) as ex:
        events = [(e, now()) for es in ex.map(lambda t: paged(f"{KALSHI}/events", "events", series_ticker=t,
                                                               with_nested_markets="true"), series) for e in es]
    yield from ((kalshi_record(e, m, ts), False) for e, ts in events for m in e.get("markets") or [])

    # Old settled markets are only served by /historical, one request per event: fetch them only
    # for geopolitical events (judged on the event title, before any market is fetched).
    empty = {("kalshi", e["event_ticker"]): (e, ts) for e, ts in events if not e.get("markets")}
    titles = {k: " ".join(filter(None, (e.get("title"), e.get("sub_title")))) for k, (e, _) in empty.items()}
    keep, _ = entities.discover(con, titles, in_scope=geopolitical)
    with ThreadPoolExecutor(8) as ex:
        hist = ex.map(lambda k: paged(f"{KALSHI}/historical/markets", "markets", event_ticker=k[1]), keep)
        for k, ms in zip(keep, hist):
            e, ts = empty[k]
            yield from ((kalshi_record(e, m, ts), True) for m in ms)

def dollars(d, k):  # live candles use "<k>_dollars", historical ones plain "<k>"
    d = d or {}
    return num(d.get(f"{k}_dollars", d.get(k)))

def kalshi_history(rec, hist):
    c, mid = rec["contract"], rec["market_id"]
    start = when(c["open_time"])
    if not start: return []
    end = int(min(when(c["close_time"]) or now(), now()).timestamp())
    url = (f"{KALSHI}/historical/markets/{mid}/candlesticks" if hist
           else f"{KALSHI}/series/{c['series_id']}/markets/{mid}/candlesticks")
    candles, t0 = {}, int(start.timestamp())
    while t0 < end:
        t1 = min(t0 + (KALSHI_MAX_CANDLES - 1) * HOUR, end)
        for x in get(url, dict(start_ts=t0, end_ts=t1, period_interval=60)).get("candlesticks") or []:
            candles[x["end_period_ts"]] = x
        t0 = t1
    rows, window, vol = [], deque(), 0.0
    for t in sorted(candles):
        x = candles[t]
        v = num(x.get("volume_fp", x.get("volume"))) or 0.0
        window.append((t, v)); vol += v
        while window[0][0] <= t - 24 * HOUR:
            vol -= window.popleft()[1]
        price = x.get("price") or {}
        last = dollars(price, "close")
        rows.append(dict(ts=datetime.fromtimestamp(t, timezone.utc),
                         yes_bid=dollars(x.get("yes_bid"), "close"),
                         yes_ask=dollars(x.get("yes_ask"), "close"),
                         last=last if last is not None else dollars(price, "previous"),
                         volume_24h=vol,
                         open_interest=num(x.get("open_interest_fp", x.get("open_interest")))))
    return rows

# ── Polymarket ────────────────────────────────────────────────────────────────
def poly_markets(con):
    seen = set()
    for tag in POLY_TAGS:
        for closed in ("false", "true"):
            for e, ts in poly_events(tag, closed):
                for m in e.get("markets") or []:
                    if m["id"] not in seen:
                        seen.add(m["id"])
                        yield poly_record(e, m, ts), False

def poly_history(rec, _):
    c = rec["contract"]
    start, tokens = when(c["open_time"]), c["clob_token_ids"] or []
    if not start or not tokens: return []
    end = now()
    if rec["status"] != "active" and (close := when(c["close_time"])):
        end = min(end, close + timedelta(days=2))  # resolution usually lands shortly after endDate
    points, t0, end = {}, int(start.timestamp()), int(end.timestamp())
    while t0 < end:
        t1 = min(t0 + POLY_WINDOW, end)
        for x in get(f"{CLOB}/prices-history", dict(market=tokens[0], startTs=t0, endTs=t1,
                                                     fidelity=60)).get("history") or []:
            points[x["t"]] = num(x["p"])
        t0 = t1
    return [dict(ts=datetime.fromtimestamp(t, timezone.utc), yes_bid=None, yes_ask=None, last=p,
                 volume_24h=None, open_interest=None) for t, p in sorted(points.items())]

# ── run ───────────────────────────────────────────────────────────────────────
VENUES = (("kalshi", kalshi_markets, kalshi_history), ("polymarket", poly_markets, poly_history))

def backfill(con, run_id, errors, stats):
    done = set(con.execute("SELECT DISTINCT venue, market_id FROM market_snapshots "
                           "WHERE source = 'backfill'").fetchall())
    versions, todo = load_versions(con), []
    for venue, discover, history in VENUES:
        try:  # liquid, not yet backfilled, then geopolitical (entity lookups only for these)
            cands = [(r, h) for r, h in discover(con) if (r["volume"] or 0) >= MIN_VOLUME
                     and (venue, r["market_id"]) not in done and not EXCLUDE_REGEX.search(r["text"])]
            questions = {(r["venue"], r["market_id"]): matching.question_text(r["contract"]) for r, _ in cands}
            keep, _ = entities.discover(con, questions, in_scope=geopolitical)
            scoped = [(r, h) for r, h in cands if (r["venue"], r["market_id"]) in keep]
        except Exception as err:
            log.exception("%s discovery failed", venue); errors.append(f"{venue} discovery: {err!r}")
            continue
        vers = version_rows([r for r, _ in scoped], versions, run_id)
        con.begin(); insert(con, "market_versions", vers); con.commit()
        todo += [(r, h, history) for r, h in scoped]
        stats[venue] = dict(candidates=len(cands), geopolitical=len(scoped), new_versions=len(vers))
        log.info("%s: %s", venue, stats[venue])

    def one(item):
        r, hist, history = item
        try: return r, history(r, hist), None
        except Exception as err: return r, [], f"{r['venue']} {r['market_id']}: {err!r}"

    batch, n_rows, n_failed = [], 0, 0
    with ThreadPoolExecutor(8) as ex:
        for i, (r, rows, err) in enumerate(ex.map(one, todo), 1):
            if err:
                n_failed += 1
                if n_failed <= 20: errors.append(err)
                log.warning(err)
            batch += [dict(source="backfill", run_id=run_id, venue=r["venue"], market_id=r["market_id"],
                           status=None, **x) for x in rows]
            if len(batch) > 50_000 or i == len(todo):  # commit as we go so a crash keeps progress
                con.begin(); insert(con, "market_snapshots", batch); con.commit()
                n_rows += len(batch); batch = []
                log.info("%d/%d markets, %d rows", i, len(todo), n_rows)
    stats |= {"rows": n_rows, "failed_markets": n_failed}
    return n_failed < len(todo) or not todo

if __name__ == "__main__":
    sys.exit(run_job("backfill", backfill))
