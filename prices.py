#!/usr/bin/env python3
"""
Daily prices: securities (securities.csv plus the BENCHMARKS -> security_prices) and factor
levels (seeds/factors.csv `series` -> factor_prices).

Stocks and Yahoo factor series come from Yahoo Finance's public chart endpoint (free, no API
key). It's unofficial and not licensed for redistribution: fine for research, but swap in a
licensed feed before selling betas. Credit spreads, Treasury yields and the broad dollar come from
FRED's public CSV download (no key). FRED serves ICE BofA spread indices for the last three years
only, so credit sensitivities can only be measured on events since then.

Incremental: first run loads from START; later runs add only dates after the last stored one.
Today's bar is skipped because it may not be final yet.
"""
import csv, io, sys
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from urllib.parse import quote

from collector import get, http, insert, log, now, run_job

START = datetime(2020, 1, 1, tzinfo=timezone.utc)
YAHOO = "https://query1.finance.yahoo.com/v8/finance/chart"
FRED = "https://fred.stlouisfed.org/graph/fredgraph.csv"
UA = {"User-Agent": "Mozilla/5.0"}  # Yahoo rejects the default python-requests agent
BENCHMARKS = ["SPY"]  # backtest.py measures abnormal returns against it

def securities():
    """Tickers to price: securities.csv, plus the benchmarks."""
    with open(Path(__file__).parent / "securities.csv") as f:
        return list(dict.fromkeys(BENCHMARKS + [row["ticker"] for row in csv.DictReader(f)]))

def history(ticker, since):
    d = get(f"{YAHOO}/{quote(ticker)}", dict(period1=int(since.timestamp()), period2=int(now().timestamp()),
                                             interval="1d"), headers=UA)["chart"]["result"][0]
    meta, q, ts = d["meta"], d["indicators"]["quote"][0], d.get("timestamp") or []
    adj = (d["indicators"].get("adjclose") or [{}])[0].get("adjclose") or [None] * len(ts)
    offset = timedelta(seconds=meta.get("gmtoffset") or 0)  # bar timestamps -> exchange-local dates
    today = (now() + offset).date()
    for i, t in enumerate(ts):
        day = (datetime.fromtimestamp(t, timezone.utc) + offset).date()
        if day < today and (q.get("close") or [None] * len(ts))[i] is not None:
            yield dict(ticker=ticker, date=day, close=q["close"][i], adj_close=adj[i],
                       volume=(q.get("volume") or [None] * len(ts))[i], currency=meta.get("currency"))

def fred(series_id, since):
    """(date, value) from FRED's CSV download; '.' marks a holiday with no observation."""
    r = http.get(FRED, params=dict(id=series_id, cosd=since.date().isoformat()), timeout=60)  # no browser UA: FRED drops those
    r.raise_for_status()
    today = now().date()
    for row in csv.DictReader(io.StringIO(r.text)):
        day, v = date.fromisoformat(row["observation_date"]), row[series_id]
        if day < today and v not in ("", "."):
            yield day, float(v)

def factor_series():
    """{factor id: "source:id"} for the factors with a measured series."""
    import factors
    return {f: c["series"] for f, c in factors.CATALOG.items() if c["series"]}

def since_after(last):
    return datetime.combine(last + timedelta(days=1), time(), timezone.utc) if last else START

def security_rows(con, run_id, errors):
    last = dict(con.execute("SELECT ticker, max(date) FROM security_prices GROUP BY 1").fetchall())
    tickers, rows, fetched_at = securities(), [], now()
    for t in tickers:
        try:
            rows += [dict(r, source="yahoo", fetched_at=fetched_at, run_id=run_id)
                     for r in history(t, since_after(last.get(t))) if t not in last or r["date"] > last[t]]
        except Exception as err:
            log.warning("%s failed: %r", t, err); errors.append(f"{t}: {err!r}")
    return tickers, rows

def factor_rows(con, run_id, errors):
    series, rows, fetched_at = factor_series(), [], now()
    # per factor *and series*: a factor switched to a new series loads that series' full history
    last = {f: d for f, s, d in con.execute("SELECT factor_id, series, max(date) FROM factor_prices GROUP BY 1, 2").fetchall()
            if series.get(f) == s}
    for f, s in series.items():
        source, _, sid = s.partition(":")
        since = since_after(last.get(f))
        try:
            got = ([(r["date"], r["close"]) for r in history(sid, since)] if source == "yahoo"
                   else list(fred(sid, since)) if source == "fred" else None)
            if got is None: raise ValueError(f"unknown source {source}")
            rows += [dict(factor_id=f, series=s, date=d, level=v, source=source, fetched_at=fetched_at, run_id=run_id)
                     for d, v in got if f not in last or d > last[f]]
        except Exception as err:
            log.warning("factor %s (%s) failed: %r", f, s, err); errors.append(f"factor {f}: {err!r}")
    return series, rows

def prices(con, run_id, errors, stats):
    tickers, srows = security_rows(con, run_id, errors)
    series, frows = factor_rows(con, run_id, errors)
    con.begin(); insert(con, "security_prices", srows); insert(con, "factor_prices", frows); con.commit()
    stats |= {"tickers": len(tickers), "rows": len(srows), "factors": len(series), "factor_rows": len(frows),
              "failed": len(errors)}
    return len(errors) < len(tickers) + len(series)

if __name__ == "__main__":
    sys.exit(run_job("prices", prices))
