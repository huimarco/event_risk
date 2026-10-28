#!/usr/bin/env python3
"""
Quantitative links from factors and questions to securities, measured from prices rather than
written down. The graph only records which issuer issued which security (securities.csv);
everything here is estimated.

1. Loadings (security_loadings): each security's weekly log returns (last adjusted close of each
   week, security_prices) over the last LOOKBACK_WEEKS, regressed on the market (factor
   equity_us) plus one factor at a time. beta = the security's weekly % move per unit weekly move
   of the factor (in the factor's units: % or bp), controlling for the market; for equity_us
   itself, the market beta. Weekly, because Asian, European and US closes don't line up daily.
2. Factor-implied sensitivity to a question: the security's loadings on the question's measured
   factors plus the market, estimated jointly (oil and EM spreads moving together are not counted
   twice), times the question's factor moves (factor_betas shrunk_beta):
     implied = sum over f of b_f x beta_qf     (% security move per +1.0 of probability)
3. Direct: the security's market-adjusted two-day move (r - market beta x r_SPY) on the question's
   lasting jump days (factor_betas.question_events), regressed on the jump through the origin;
   se from the security's own SD of such moves.
4. shrunk = precision-weighted mix of direct and a prior: implied, with variance tau^2 +
   implied^2/4 (tau = spread of direct - implied across pairs with events, DerSimonian-Laird; the
   implied^2/4 term admits the implied value is itself an estimate); or, for pairs without a
   factor-implied value, 0 with variance tau0^2 (spread of direct across those pairs) — one jump
   day of an unrelated news story should not produce a large number. No events: shrunk = implied.

Which pairs are measured:
  factor      the security's loading on one of the question's factors (not the market) has
              |t| >= MIN_T — a statistical link, with or without any entity path
  structural  the question reaches the security's issuer through entities (Graph.exposures_of,
              score >= MIN_STRUCTURAL_SCORE)
  both
Factor pairs are capped at MAX_PER_QUESTION per question by |implied|; structural pairs are kept.

Caveats: loadings are in-sample and recent (3 years); jump days carry other news; the graph and
factor links are today's, not point-in-time. Units are % per +1.0 of probability (per_10pts = /10).

  python security_betas.py [--jump 0.05] [--since 2021-01-01] [--dry-run]

Appends one full set of rows to security_loadings and security_betas per run; the API reads the
latest run. Runs after factor_betas.py (it uses its latest sensitivities).
"""
import argparse, bisect, csv, json, math, sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import api, factors
from backtest import returns
from collector import cursor, insert, log, run_job
from factor_betas import question_events
from prices import BENCHMARKS

LOOKBACK_WEEKS = 156
MIN_WEEKS = 52
MIN_T = 3.0
MAX_PER_QUESTION = 50
MIN_STRUCTURAL_SCORE = 0.3
MARKET = "equity_us"
MAX_EVENTS = 10

def weekly(points, pct):
    """{week start: move} from [(date, level)] sorted: last level per ISO week, then the change
    from the previous week (100 x log return for %, 100 x level change for bp)."""
    last = {}
    for d, v in points:
        if v is None or (pct and v <= 0): continue
        last[d - timedelta(days=d.weekday())] = v
    weeks = sorted(last)
    return {w: (100 * math.log(last[w] / last[p]) if pct else 100 * (last[w] - last[p]))
            for p, w in zip(weeks, weeks[1:]) if (w - p).days == 7}

def load_series(cur, tickers, since):
    fac = defaultdict(list)
    for f, d, v in cur.execute("""SELECT factor_id, date::VARCHAR, arg_max(level, fetched_at) FROM factor_prices
                                  WHERE date >= ? GROUP BY ALL ORDER BY 1, 2""", [since]).fetchall():
        fac[f].append((date.fromisoformat(d), v))
    secs = defaultdict(list)
    for t, d, v in cur.execute("""SELECT ticker, date::VARCHAR, arg_max(coalesce(adj_close, close), fetched_at) FROM security_prices
                                  WHERE date >= ? AND ticker IN (SELECT unnest(?::VARCHAR[])) GROUP BY ALL ORDER BY 1, 2""",
                               [since, list(tickers)]).fetchall():
        secs[t].append((date.fromisoformat(d), v))
    F = {f: weekly(xs, factors.CATALOG[f]["units"] == "%") for f, xs in fac.items() if f in factors.CATALOG}
    S = {t: weekly(xs, True) for t, xs in secs.items()}
    return {f: s for f, s in F.items() if len(s) >= MIN_WEEKS}, {t: s for t, s in S.items() if len(s) >= MIN_WEEKS}

def cov(a, b):
    """(covariance, n) over the weeks both series have."""
    ws = a.keys() & b.keys()
    n = len(ws)
    if n < MIN_WEEKS: return None, n
    ma, mb = sum(a[w] for w in ws) / n, sum(b[w] for w in ws) / n
    return sum((a[w] - ma) * (b[w] - mb) for w in ws) / n, n

def solve(A, y):
    """A x = y by Gaussian elimination with partial pivoting (A small and symmetric)."""
    k = len(y)
    M = [row[:] + [y[i]] for i, row in enumerate(A)]
    for c in range(k):
        p = max(range(c, k), key=lambda r: abs(M[r][c]))
        if abs(M[p][c]) < 1e-12: return None
        M[c], M[p] = M[p], M[c]
        for r in range(k):
            if r != c:
                f = M[r][c] / M[c][c]
                M[r] = [x - f * z for x, z in zip(M[r], M[c])]
    return [M[i][k] / M[i][i] for i in range(k)]

def inverse_diag(A):
    k = len(A)
    return [solve(A, [1.0 if j == i else 0.0 for j in range(k)])[i] for i in range(k)]

class Regressions:
    """Joint OLS of a security on a set of factors, from pairwise covariances (cached)."""
    def __init__(self, F, S):
        self.F, self.S, self._ff, self._fs = F, S, {}, {}
    def ff(self, a, b):
        k = (a, b) if a <= b else (b, a)
        if k not in self._ff: self._ff[k] = cov(self.F[a], self.F[b])[0]
        return self._ff[k]
    def fs(self, f, t):
        if (f, t) not in self._fs: self._fs[(f, t)] = cov(self.F[f], self.S[t])
        return self._fs[(f, t)]
    def fit(self, t, fs):
        """{factor: (beta, se)} for security t on factors fs jointly; None if not estimable."""
        fs = [f for f in fs if f in self.F]
        cs = [self.fs(f, t) for f in fs]
        if not fs or any(c is None for c, _ in cs): return None
        n = min(m for _, m in cs)
        A = [[self.ff(a, b) for b in fs] for a in fs]
        if any(x is None for row in A for x in row): return None
        ridge = 1e-9 * sum(A[i][i] for i in range(len(fs)))
        A = [[x + (ridge if i == j else 0) for j, x in enumerate(row)] for i, row in enumerate(A)]
        y = [c for c, _ in cs]
        b = solve(A, y)
        if b is None: return None
        var_s = cov(self.S[t], self.S[t])[0]
        resid = max(var_s - sum(bi * yi for bi, yi in zip(b, y)), 1e-12) * n / max(n - len(fs) - 1, 1)
        inv = inverse_diag(A)
        return {f: (bi, math.sqrt(max(resid * v / n, 0))) for f, bi, v in zip(fs, b, inv)}

def market_adjusted(rets, bench, beta):
    """{trading day: two-day market-adjusted return starting that day} for one security."""
    days = sorted(rets)
    return {d: sum(rets[t] - beta * bench.get(t, 0.0) for t in days[i:i + 2]) * 100
            for i, d in enumerate(days[:-1]) if all(t in bench for t in days[i:i + 2])}

def dl_tau2(pairs):
    """DerSimonian-Laird between-pair variance of estimates z with sampling variances v; deviations
    are measured from 0 (the estimates are already centred on their prior)."""
    if len(pairs) < 3: return None
    w = [1 / v for _, v in pairs]
    q = sum(wi * z * z for (z, _), wi in zip(pairs, w))
    return max(0.0, (q - len(pairs)) / (sum(w) - sum(wi * wi for wi in w) / sum(w)))

def shrink(rows):
    with_events = [r for r in rows if r["n_events"] and r["se"]]
    tau2 = dl_tau2([(r["beta"] - r["implied"], r["se"] ** 2) for r in with_events if r["implied"] is not None]) or 0.0
    tau0 = dl_tau2([(r["beta"], r["se"] ** 2) for r in with_events if r["implied"] is None])
    tau02 = tau0 if tau0 is not None else tau2
    for r in rows:
        r["tau"] = math.sqrt(tau2 if r["implied"] is not None else tau02)
        prior = (r["implied"], tau2 + r["implied"] ** 2 / 4) if r["implied"] is not None else (0.0, tau02)
        if r["n_events"] and r["se"] and prior and prior[1] > 0:
            prec = 1 / r["se"] ** 2 + 1 / prior[1]
            r["shrunk"], r["shrunk_se"] = (r["beta"] / r["se"] ** 2 + prior[0] / prior[1]) / prec, 1 / math.sqrt(prec)
        elif r["n_events"] and r["se"]:  # prior variance 0: the data say these pairs don't differ from the prior
            r["shrunk"], r["shrunk_se"] = prior[0], 0.0
        elif r["implied"] is not None:
            r["shrunk"], r["shrunk_se"] = prior[0], math.sqrt(prior[1]) if prior[1] > 0 else None
        else:
            r["shrunk"] = r["shrunk_se"] = None
    return [r for r in rows if r["shrunk"] is not None]

def estimate(g, cur, jump, since):
    with open(Path(__file__).parent / "securities.csv") as f:
        secs = {r["ticker"]: r for r in csv.DictReader(f)}
    start = (datetime.now(timezone.utc) - timedelta(weeks=LOOKBACK_WEEKS + 1)).date()
    F, S = load_series(cur, secs, start)
    reg = Regressions(F, S)
    log.info("%d factors and %d securities with >= %d weeks", len(F), len(S), MIN_WEEKS)

    # 1. loadings: market + one factor at a time
    loadings = {}
    for t in S:
        mk = reg.fit(t, [MARKET])
        if mk: loadings[(t, MARKET)] = mk[MARKET]
        for f in F:
            if f != MARKET and (fit := reg.fit(t, [MARKET, f])): loadings[(t, f)] = fit[f]
    sig = defaultdict(set)  # factor -> securities loading on it with |t| >= MIN_T
    for (t, f), (b, se) in loadings.items():
        if f != MARKET and se and abs(b / se) >= MIN_T: sig[f].add(t)

    # 2. candidate pairs and factor-implied sensitivities
    betas = api.load_betas(cur)
    qf = defaultdict(dict)  # question -> {factor: shrunk beta}
    for (q, f), b in betas.items():
        if f in F and b["shrunk_beta"] is not None: qf[q][f] = b["shrunk_beta"]
    structural = defaultdict(set)  # question -> tickers reached through entities
    by_issuer = defaultdict(list)
    for t, r in secs.items():
        if r["issuer_id"] and t in S: by_issuer[f"iss:{r['issuer_id']}"].append(t)
    for iss, ts in by_issuer.items():
        if iss in g.nodes:
            for q, v in g.exposures_of(iss).items():
                if v["score"] >= MIN_STRUCTURAL_SCORE: structural[q].update(ts)
    rows = []
    for q in set(qf) | set(structural):
        fs = qf.get(q, {})
        cands = {t for f in fs if f != MARKET for t in sig.get(f, ())}
        scored = []
        for t in cands | structural.get(q, set()):
            fit = reg.fit(t, sorted(set(fs) | {MARKET})) if fs else None
            implied = sum(fit[f][0] * fs[f] for f in fs if f in fit) if fit else None
            src = "both" if t in cands and t in structural.get(q, ()) else "factor" if t in cands else "structural"
            scored.append(dict(question_id=q, ticker=t, implied=implied, source=src,
                               implied_factors={f: round(fit[f][0], 4) for f in fit} if fit else {}))
        factor_only = sorted((r for r in scored if r["source"] == "factor" and r["implied"] is not None),
                             key=lambda r: -abs(r["implied"]))[:MAX_PER_QUESTION]
        rows += factor_only + [r for r in scored if r["source"] != "factor"]
    log.info("%d (question, security) pairs: %s", len(rows), {k: sum(1 for r in rows if r["source"] == k) for k in ("factor", "structural", "both")})

    # 3. direct event betas
    events = question_events(g, cur, {r["question_id"] for r in rows}, jump, since)
    daily = returns(cur, list(dict.fromkeys(BENCHMARKS + [r["ticker"] for r in rows])))
    bench = daily.get(BENCHMARKS[0], {})
    adj, sd = {}, {}
    for t in {r["ticker"] for r in rows}:
        if t not in daily: continue
        adj[t] = market_adjusted(daily[t], bench, loadings.get((t, MARKET), (1.0, None))[0])
        sd[t] = math.sqrt(sum(x * x for x in adj[t].values()) / len(adj[t])) if len(adj[t]) > 20 else None
    for r in rows:
        t, a = r["ticker"], adj.get(r["ticker"])
        hits = []
        if a and sd.get(t):
            days = sorted(a)
            for d, p, dp in events.get(r["question_id"], ()):
                i = bisect.bisect_left(days, d)
                if i < len(days): hits.append((d, p, dp, a[days[i]]))
        sxx = sum(dp * dp for _, _, dp, _ in hits)
        r.update(n_events=len(hits), beta=sum(dp * y for _, _, dp, y in hits) / sxx if sxx else None,
                 se=sd[t] / math.sqrt(sxx) if sxx else None, units="%",
                 events=[dict(day=str(d), rung=p, dp=round(dp, 4), move=round(y, 3))
                         for d, p, dp, y in sorted(hits, key=lambda h: -abs(h[2]))[:MAX_EVENTS]])
    loading_rows = [dict(ticker=t, factor_id=f, beta=b, se=se, t=b / se if se else None,
                         n_weeks=reg.fs(f, t)[1] if f in F else None) for (t, f), (b, se) in loadings.items()]
    return loading_rows, shrink(rows)

def run(args):
    def job(con, run_id, errors, stats):
        cur = cursor(con)
        cur.execute("SET TimeZone = 'UTC'")
        t = datetime.now(timezone.utc)
        loads, rows = estimate(api.Graph(cur, t, t), cur, args.jump, args.since)
        if not args.dry_run:
            insert(con, "security_loadings", [dict(r, run_id=run_id, computed_at=t) for r in loads])
            insert(con, "security_betas", [dict(r, run_id=run_id, computed_at=t, events=json.dumps(r["events"]),
                                                implied_factors=json.dumps(r["implied_factors"])) for r in rows])
        stats |= {"loadings": len(loads), "significant": sum(1 for r in loads if r["t"] and abs(r["t"]) >= MIN_T and r["factor_id"] != MARKET),
                  "pairs": len(rows), "with_events": sum(1 for r in rows if r["n_events"]), "dry_run": args.dry_run}
        if args.dry_run:
            for r in sorted(rows, key=lambda r: -abs(r["shrunk"]))[:25]:
                print(f"{r['ticker']:10} {r['source']:10} implied {r['implied'] if r['implied'] is not None else float('nan'):+7.2f} "
                      f"direct {r['beta'] if r['beta'] is not None else float('nan'):+7.2f} (n={r['n_events']:3}) shrunk {r['shrunk']:+7.2f}  "
                      f"{g_name(con, r['question_id'])[:60]}")
        return True
    return run_job("security_betas", job)

def g_name(con, qid):
    return (con.execute("SELECT name FROM graph_nodes WHERE node_id = ? ORDER BY recorded_at DESC LIMIT 1", [qid]).fetchone() or [qid])[0]

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--jump", type=float, default=0.05, help="min lasting |daily change| in probability (default 0.05)")
    ap.add_argument("--since", default="2021-01-01")
    ap.add_argument("--dry-run", action="store_true", help="print the largest sensitivities instead of writing")
    sys.exit(run(ap.parse_args()))
