#!/usr/bin/env python3
"""
Daily security prices for the tickers in securities.csv, from Yahoo Finance's public chart
endpoint (free, no API key). It's unofficial and not licensed for redistribution: fine for
research, but swap in a licensed feed before selling betas.

Incremental: first run loads from START; later runs add only dates after the last stored one.
Today's bar is skipped because it may not be final yet.
"""
import csv, sys
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from urllib.parse import quote

from collector import get, insert, log, now, run_job

START = datetime(2020, 1, 1, tzinfo=timezone.utc)
YAHOO = "https://query1.finance.yahoo.com/v8/finance/chart"
UA = {"User-Agent": "Mozilla/5.0"}  # Yahoo rejects the default python-requests agent

def securities():
    with open(Path(__file__).parent / "securities.csv") as f:
        return [row["ticker"] for row in csv.DictReader(f)]

def history(ticker, since):
    d = get(f"{YAHOO}/{quote(ticker)}", dict(period1=int(since.timestamp()), period2=int(now().timestamp()),
                                             interval="1d"), headers=UA)["chart"]["result"][0]
    meta, q, ts = d["meta"], d["indicators"]["quote"][0], d.get("timestamp") or []
    adj = (d["indicators"].get("adjclose") or [{}])[0].get("adjclose") or [None] * len(ts)
    offset = timedelta(seconds=meta.get("gmtoffset") or 0)  # bar timestamps -> exchange-local dates
    today = (now() + offset).date()
    for i, t in enumerate(ts):
        day = (datetime.fromtimestamp(t, timezone.utc) + offset).date()
        if day < today and q["close"][i] is not None:
            yield dict(ticker=ticker, date=day, close=q["close"][i], adj_close=adj[i],
                       volume=q["volume"][i], currency=meta.get("currency"))

def prices(con, run_id, errors, stats):
    last = dict(con.execute("SELECT ticker, max(date) FROM security_prices GROUP BY 1").fetchall())
    tickers, rows, fetched_at = securities(), [], now()
    for t in tickers:
        since = (datetime.combine(last[t] + timedelta(days=1), time(), timezone.utc)
                 if t in last else START)
        try:
            rows += [dict(r, source="yahoo", fetched_at=fetched_at, run_id=run_id)
                     for r in history(t, since) if t not in last or r["date"] > last[t]]
        except Exception as err:
            log.warning("%s failed: %r", t, err); errors.append(f"{t}: {err!r}")
    con.begin(); insert(con, "security_prices", rows); con.commit()
    stats |= {"tickers": len(tickers), "rows": len(rows), "failed": len(errors)}
    return len(errors) < len(tickers)

if __name__ == "__main__":
    sys.exit(run_job("prices", prices))
