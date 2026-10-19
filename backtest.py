#!/usr/bin/env python3
"""
Does the graph predict price reactions? A backtest of the structural exposure priors (api.py:
Graph.exposures_of) against what securities actually did when proposition odds jumped.

  1. Jumps: days on which a proposition's blended daily probability (last price per market per UTC
     day, averaged over its same_as markets) moved by at least --jump and still held at least half
     of that move a day later (thin markets spike and revert), for every proposition with a path
     to some security.
  2. Reaction: each security's abnormal return (return minus SPY's) over the first two trading days
     at or after the day — two days because a jump late in a UTC day lands after the US close —
     divided by that security's own standard deviation of such returns (z), so volatile and quiet
     securities are comparable.
  3. Per (day, security): predicted move = sum over that day's jumps of impact x dp, linked = the
     security's best exposure score among them >= --min-score.

Reported:
  magnitude    within each security: mean |z| on days a linked proposition jumped vs all its other
               days (does a jump close by in the graph mean a bigger move than usual?), and by
               score band. Comparing linked with unlinked securities instead would measure only
               that oil, chip and defense stocks are more volatile than index ETFs.
  direction    share of linked, signed predictions whose sign matches the abnormal return
  calibration  slope k of abnormal return on predicted move (through the origin): converts graph
               impact units into return per probability point; t-stat and correlation

Caveats, printed with the results: jump days are not independent (one news day moves many
propositions, and securities co-move); the graph is today's, not the one known on each jump day
(entity links and exposures are reference facts, but this is not a point-in-time test); security
prices cover only the tickers prices.py has fetched.

  python backtest.py [--jump 0.05] [--min-score 0.1] [--since 2024-01-01] [--csv rows.csv]
"""
import argparse, bisect, csv, math, sys
from collections import defaultdict
from datetime import date, datetime, timezone

import pyarrow as pa

import api

BENCHMARK = "SPY"
BANDS = [(0.1, 0.3), (0.3, 0.6), (0.6, 1.01)]

def daily_probs(cur, members, since):
    """{pid: {day: prob}}: last cleaned price per market per UTC day, averaged over the proposition's markets."""
    keys = [(m.split(":", 2)[1], m.split(":", 2)[2], p) for p, ms in members.items() for m in ms]
    cur.register("mp", pa.table({"venue": [k[0] for k in keys], "market_id": [k[1] for k in keys],
                                 "pid": [k[2] for k in keys]}))
    rows = cur.execute("""
        WITH daily AS (
          SELECT venue, market_id, CAST(timezone('UTC', ts) AS DATE) AS day, arg_max(prob, ts) AS prob
          FROM v_market_prob SEMI JOIN mp USING (venue, market_id)
          WHERE prob IS NOT NULL AND ts >= ? GROUP BY ALL)
        SELECT pid, day::VARCHAR, avg(prob) FROM daily JOIN mp USING (venue, market_id) GROUP BY ALL""",
        [since]).fetchall()
    cur.unregister("mp")
    out = defaultdict(dict)
    for p, d, prob in rows:
        out[p][date.fromisoformat(d)] = prob
    return out

def returns(cur, tickers):
    """{ticker: {trading date: daily return}} from adjusted closes (latest fetch wins)."""
    rows = cur.execute("""
        SELECT ticker, date::VARCHAR, arg_max(coalesce(adj_close, close), fetched_at) FROM security_prices
        WHERE ticker IN (SELECT unnest(?::VARCHAR[])) GROUP BY ALL ORDER BY 1, 2""", [list(tickers)]).fetchall()
    out, prev = defaultdict(dict), {}
    for t, d, px in rows:
        if px and prev.get(t):
            out[t][date.fromisoformat(d)] = px / prev[t] - 1
        prev[t] = px
    return out

def mean(xs): return sum(xs) / len(xs) if xs else float("nan")

def welch(a, b):
    if len(a) < 2 or len(b) < 2: return float("nan")
    va = sum((x - mean(a)) ** 2 for x in a) / (len(a) - 1)
    vb = sum((x - mean(b)) ** 2 for x in b) / (len(b) - 1)
    return (mean(a) - mean(b)) / math.sqrt(va / len(a) + vb / len(b))

def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--jump", type=float, default=0.05, help="min |daily change| in probability (default 0.05)")
    ap.add_argument("--min-score", type=float, default=0.1, help="exposure score that counts as linked (default 0.1)")
    ap.add_argument("--since", default="2021-01-01")
    ap.add_argument("--csv", help="write the (day, security) rows here")
    args = ap.parse_args()

    cur = api.connection().cursor()
    cur.execute("SET TimeZone = 'UTC'")
    now = datetime.now(timezone.utc)
    g = api.Graph(cur, now, now)
    secs = [i for i, n in g.nodes.items() if n["type"] == "security"]
    rets = returns(cur, [i[4:] for i in secs])
    if BENCHMARK not in rets: sys.exit(f"no prices for the benchmark {BENCHMARK}")
    bench = rets[BENCHMARK]
    secs = [s for s in secs if s[4:] in rets and s[4:] != BENCHMARK]
    expo = {s: g.exposures_of(s) for s in secs}
    props = {p for e in expo.values() for p in e}
    members = {p: [m for m, rel, _ in g.markets(p) if rel in api.BLENDED] for p in props}
    probs = daily_probs(cur, members, args.since)

    jumps = defaultdict(dict)  # day -> {pid: dp}
    day = lambda d, k: date.fromordinal(d.toordinal() + k)
    for p, series in probs.items():
        for d, x in series.items():
            prev, nxt = series.get(day(d, -1)), series.get(day(d, 1))
            if prev is None or nxt is None or abs(x - prev) < args.jump: continue
            if (nxt - prev) * (x - prev) > 0 and abs(nxt - prev) >= abs(x - prev) / 2:  # the move held
                jumps[d][p] = x - prev

    # every day's 2-day abnormal return per security, and its standard deviation (for z)
    days = sorted(bench)
    first = min(jumps, default=None)
    windows = {d: days[i:i + 2] for i, d in enumerate(days[:-1]) if first and d >= first}
    ar = {s: {d: sum(rets[s[4:]][t] - bench[t] for t in w) for d, w in windows.items()
              if all(t in rets[s[4:]] for t in w)} for s in secs}
    sd = {s: math.sqrt(mean([x * x for x in v.values()])) for s, v in ar.items() if len(v) > 20}

    rows, jumped = [], defaultdict(set)  # jumped: security -> trading days with a linked jump
    for d, moved in sorted(jumps.items()):
        i = bisect.bisect_left(days, d)
        if i + 1 >= len(days): continue
        t0 = days[i]
        for s in secs:
            if s not in sd or t0 not in ar[s]: continue
            hits = {p: expo[s][p] for p in moved if p in expo[s]}
            score = max((v["score"] for v in hits.values()), default=0.0)
            signed = [v["impact"] * moved[p] for p, v in hits.items() if v["impact"] is not None]
            rows.append(dict(day=d, security=s[4:], n_jumps=len(moved), score=score,
                             predicted=sum(signed) if signed else None, abnormal_return=ar[s][t0],
                             z=ar[s][t0] / sd[s]))
            if score >= args.min_score: jumped[s].add(t0)

    linked = [r for r in rows if r["score"] >= args.min_score]
    print(f"days with a lasting jump {len(jumps)} (median {sorted(map(len, jumps.values()))[len(jumps) // 2] if jumps else 0}"
          f" propositions each) · propositions with a path to a priced security {len(props)} · "
          f"securities with prices {len(sd)} · (day, security) rows {len(rows)} ({len(linked)} linked)")
    if not rows: sys.exit("nothing to test: no jumps overlap the security price history")

    print("\nMagnitude — mean |z| (abnormal return / the security's own SD), each security vs itself")
    on = [abs(r["z"]) for r in linked]
    off = [abs(x) / sd[s] for s in sd for d, x in ar[s].items() if d not in jumped[s] and jumped[s]]
    print(f"  days a linked proposition jumped  {mean(on):.3f}  (n={len(on)})\n"
          f"  that security's other days        {mean(off):.3f}  (n={len(off)})\n"
          f"  ratio {mean(on) / mean(off):.3f}, Welch t = {welch(on, off):.2f}  (1.0 = no effect)")
    for lo, hi in BANDS:
        band = [abs(r["z"]) for r in linked if lo <= r["score"] < hi]
        print(f"  score {lo:.1f}–{min(hi, 1):.1f}  {mean(band):.3f}  (n={len(band)})")

    signed = [r for r in linked if r["predicted"]]
    print("\nDirection — linked rows with a signed prediction")
    if signed:
        hits = sum(1 for r in signed if (r["predicted"] > 0) == (r["abnormal_return"] > 0))
        n = len(signed)
        print(f"  hit rate {hits / n:.1%} (n={n}; 50% = chance; z = {(hits - n / 2) / math.sqrt(n / 4):.2f})")
        sxy = sum(r["predicted"] * r["abnormal_return"] for r in signed)
        sxx = sum(r["predicted"] ** 2 for r in signed)
        k = sxy / sxx
        resid = [r["abnormal_return"] - k * r["predicted"] for r in signed]
        se = math.sqrt(sum(e * e for e in resid) / max(n - 1, 1) / sxx)
        xs, ys = [r["predicted"] for r in signed], [r["abnormal_return"] for r in signed]
        mx, my = mean(xs), mean(ys)
        cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
        corr = cov / math.sqrt(sum((x - mx) ** 2 for x in xs) * sum((y - my) ** 2 for y in ys) or 1)
        print(f"\nCalibration — abnormal return = k x predicted\n  k = {k:.4f} (SE {se:.4f}, t = {k / se:.2f}), "
              f"correlation {corr:.3f}\n  i.e. impact 1.0 and a +10 pt move ≈ {k * 0.1:+.2%} abnormal return")
    else:
        print("  none")

    print("\nCaveats: jump days are not independent (t-stats overstate); today's graph is used for past jumps"
          " (not point-in-time); only securities with fetched prices are tested.")
    if args.csv:
        with open(args.csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0]))
            w.writeheader(); w.writerows(rows)
        print(f"rows written to {args.csv}")

if __name__ == "__main__":
    main()
