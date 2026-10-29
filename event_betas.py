#!/usr/bin/env python3
"""
Dense event sensitivities: every issuer (its primary security) and every macro factor with a daily
series, measured against every question that has data — not only the pairs a factor loading, an
entity path or a rule links (security_betas.py / factor_betas.py). Feeds the Issuer and Macro
dashboards (/dashboard/issuers/{id}, /dashboard/factors/{id}).

Events: every question in the graph, live or closed; its lasting daily jumps over all its rungs and
all their venue contracts (factor_betas.question_events, --jump). A question has data for a target
when it has jump days, or (securities only) measured factor moves to imply a move from. Questions
with neither (never jumped, no measured factor) can't be regressed and are counted, not guessed.

Securities (issuer_event_betas), as security_betas.py: direct = market-adjusted two-day move on the
question's jump days regressed on the jump (se from the security's own SD of such moves); implied =
the security's joint loadings on the question's measured factors + market x those factor moves;
shrunk toward implied (variance tau^2 + implied^2/4) or, with no implied, toward 0 (tau0^2); both
taus by DerSimonian-Laird over this dense population. % per +1.0 of probability.

Factors (factor_event_betas): own beta as factor_betas.py (factor move from the last close before
the jump day to the second trading day after, through the origin, se from the factor's local
volatility around each jump). Linked pairs (a rule or curated question -> factor link) keep
factor_betas' shrunk estimate, pooled per channel. Unlinked pairs get a spike-and-slab prior per
factor (spike_slab: a share of exact zeros + a normal slab, fitted by EM over its unlinked pairs):
p_effect = P(the event moves the factor at all), shrunk = p_effect x the slab-shrunk estimate.
Only unlinked pairs with jumps in >= MIN_EPISODES independent episodes are estimated: with one or
two news days, an unrelated event that happened to jump during a big factor move (a BBC licence
question during a late-October HY rally) can't be told apart from a real one. A
single normal prior (DerSimonian-Laird) fitted to thousands of mostly-null pairs had a spread of 0
for 11 factors (EUR, JPY, VIX, IG...) and set every unlinked estimate to 0, real ones included
(decision 2026-10-28). Factor units (% or bp) per +1.0 of probability.

Every pair is evaluated each run; the top KEEP live questions per security / factor by |shrunk|
(ties by |own t|) are stored, with n_evaluated / n_live as coverage and q = Benjamini-Hochberg
q-value of the own estimate over the target's pairs. Securities are ranked (and kept) separately
with and without an economic channel (issuer_channels: measured factor moves, an entity path, the
issuer's GICS sector link, a curated link): ranked by size alone, thousands of unrelated questions
put chance associations on top, and q doesn't catch them (one news day moves many questions). Same caveats as the scripts
above: one news day moves many questions, jump days carry other news, the graph is today's.

  python event_betas.py [--jump 0.05] [--since 2021-01-01] [--keep 50] [--dry-run]

Appends one set of rows to issuer_event_betas and factor_event_betas per run; the API reads the
latest. Runs after security_betas.py (it uses the latest factor_betas and security_loadings).
"""
import argparse, bisect, csv, json, math, sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import api, factors
from backtest import returns
from collector import cursor, insert, log, run_job
from factor_betas import factor_moves, fit, question_events, react
from prices import BENCHMARKS
from security_betas import (LOOKBACK_WEEKS, MARKET, MAX_EVENTS, MIN_STRUCTURAL_SCORE, Regressions, dl_tau2, load_series,
                            market_adjusted)

KEEP = 50
MIN_P_EFFECT = 0.5  # an unlinked factor pair is a detectable effect when P(effect) >= this; others are listed as weak
MIN_EPISODES, EPISODE_GAP = 3, 5  # an unlinked factor pair needs jumps in >= 3 episodes (jump days > 5 days apart)

def episodes(h):
    """Independent episodes among jump days [(day, ...)]: days within EPISODE_GAP of the previous
    one belong to the same news story (a week of jumps on one late-October HY move is one piece of
    evidence, not three)."""
    ds = sorted(x[0] for x in h)
    return sum(1 for i, d in enumerate(ds) if i == 0 or (d - ds[i - 1]).days > EPISODE_GAP)

def shrink_to(beta, se, prior, var):
    """(shrunk, shrunk_se): precision-weighted mix of an own estimate (beta, se) and a prior N(prior,
    var); the prior alone without an own estimate; (None, None) with neither."""
    if beta is None or not se:
        return (prior, math.sqrt(var) if var > 0 else None) if prior is not None else (None, None)
    if var <= 0: return prior, 0.0   # the data say pairs don't differ from the prior
    prec = 1 / se ** 2 + 1 / var
    return (beta / se ** 2 + prior / var) / prec, 1 / math.sqrt(prec)

def spike_slab(pairs, iters=300):
    """Empirical-Bayes prior for many estimates (beta, var) of which most are probably zero: a share
    pi0 of exact zeros plus a normal N(0, tau1^2) slab, fitted by EM (latent effect and group).
    Returns (pi0, tau1^2). A single normal prior fitted to thousands of mostly-null pairs gets a
    spread of ~0 and erases the few real effects with them; this one keeps them."""
    if len(pairs) < 20: return 1.0, 0.0
    pi0, t2 = 0.9, sorted(v for _, v in pairs)[len(pairs) // 2]
    for _ in range(iters):
        w, num = [], 0.0
        for b, v in pairs:
            w1 = post_slab(b, v, pi0, t2)
            m, pv = b * t2 / (t2 + v), t2 * v / (t2 + v)
            w.append(w1); num += w1 * (m * m + pv)
        sw = sum(w)
        new_pi0, new_t2 = min(max(1 - sw / len(pairs), 1e-3), 1 - 1e-3), num / sw if sw > 1e-9 else 0.0
        done = abs(new_pi0 - pi0) < 1e-6 and abs(new_t2 - t2) <= 1e-6 * max(t2, 1e-12)
        pi0, t2 = new_pi0, new_t2
        if done or t2 <= 0: break
    return pi0, t2

def post_slab(b, v, pi0, t2):
    """P(the pair has an effect | its estimate) under spike_slab's prior."""
    l0 = math.log(pi0) - 0.5 * (math.log(v) + b * b / v)
    l1 = math.log(1 - pi0) - 0.5 * (math.log(v + t2) + b * b / (v + t2))
    return 1 / (1 + math.exp(min(l0 - l1, 700)))

def slab_shrink(b, se, pi0, t2):
    """(posterior mean, posterior sd, P(effect)) of one estimate under the spike-and-slab prior."""
    if b is None or not se or t2 <= 0: return (0.0, None, 0.0) if b is not None else (None, None, None)
    v = se * se
    w = post_slab(b, v, pi0, t2)
    m, pv = b * t2 / (t2 + v), t2 * v / (t2 + v)
    return w * m, math.sqrt(max(w * (m * m + pv) - (w * m) ** 2, 0.0)), w

def audit(h):
    """The largest jump days of one pair, for the record: [dict(day, rung, dp, move)]."""
    return [dict(day=str(d), rung=p, dp=round(dp, 4), move=round(y, 3)) for d, p, dp, y in sorted(h, key=lambda x: -abs(x[2]))[:MAX_EVENTS]]

def bh(rows):
    """Benjamini-Hochberg q-value of each row's own (jump-day) estimate, over all of one target's
    rows that have one (two-sided normal p of beta / se); None without jump days."""
    ps = sorted(((math.erfc(abs(r["beta"] / r["se"]) / math.sqrt(2)), r) for r in rows if r["beta"] is not None and r["se"]),
                key=lambda x: x[0])
    for r in rows: r["q"] = None
    m, q = len(ps), 1.0
    for k in range(m, 0, -1):
        q = min(q, ps[k - 1][0] * m / k); ps[k - 1][1]["q"] = q

def ranked(rows, live, keep, n_evaluated=None):
    """Rows for live questions, by |shrunk| then |own t|, the top `keep` with rank and coverage."""
    t = lambda r: abs(r["beta"] / r["se"]) if r["beta"] is not None and r["se"] else 0
    lv = sorted((r for r in rows if r["question_id"] in live), key=lambda r: (-abs(r["shrunk"]), -t(r)))
    return [dict(r, rank=i + 1, n_evaluated=n_evaluated or len(rows), n_live=len(lv)) for i, r in enumerate(lv[:keep])]

def issuer_channels(g):
    """{issuer id: {question: [channel]}}: the economic reasons a question could move an issuer, other
    than measured factor moves (added per pair): an entity path (Graph.exposures_of, score >=
    MIN_STRUCTURAL_SCORE), the issuer's GICS sector in the LLM sector links, a curated issuer link."""
    code = {s["name"]: c for c, s in factors.SECTORS.items()}
    with open(Path(__file__).parent / "issuers.csv") as f:
        sector = {r["issuer_id"]: code.get(r["sector"]) for r in csv.DictReader(f)}
    by_sector = defaultdict(list)
    for q, cs in factors.SECTOR_LINKS.items():
        for c in cs: by_sector[c].append(q)
    out = defaultdict(lambda: defaultdict(list))
    for iid, c in sector.items():
        for q in by_sector.get(c, ()): out[iid][q].append("sector")
        if f"iss:{iid}" in g.nodes:
            for q, v in g.exposures_of(f"iss:{iid}").items():
                if v["score"] >= MIN_STRUCTURAL_SCORE: out[iid][q].append("entity")
    for r in factors.QUESTION_LINKS:
        if r["target_type"] == "issuer": out[r["target_id"]][r["question_id"]].append("analyst")
    return out

def factor_part(g, cur, events, live, keep):
    series = factor_moves(cur)
    linked = api.load_betas(cur)
    out, cover = [], {}
    for f, c in factors.CATALOG.items():
        if c["layer"] != "macro" or f not in series: continue
        own = {}
        for q, evs in events.items():
            h = [(d, p, dp, *r) for d, p, dp in evs if (r := react(series[f], d))]
            b, se = fit(h)
            if b is not None: own[q] = ([x[:4] for x in h], b, se)
        # unlinked pairs: estimated only with repeated, independent evidence (MIN_EPISODES); the rest
        # are counted as evaluated but can't be told apart from whatever else moved the factor those days
        unl = {q: v for q, v in own.items() if (q, f) not in linked and v[2] and episodes(v[0]) >= MIN_EPISODES}
        pi0, t2 = spike_slab([(b, se * se) for _, b, se in unl.values()])
        rows, n_data = [], len(own) + sum(1 for (q, ff) in linked if ff == f and q not in own)
        for q in set(own) | {q for (q, ff) in linked if ff == f}:
            if q not in g.nodes: continue
            h, b, se = own.get(q, ([], None, None))
            L = linked.get((q, f))
            if L and L["shrunk_beta"] is not None:
                s, sse, tau, ch, pe = L["shrunk_beta"], L["shrunk_se"], L["tau"], L["channel"], None
            elif q not in unl:
                continue
            else:
                (s, sse, pe), tau, ch = slab_shrink(b, se, pi0, t2), math.sqrt(t2), None
            if s is None: continue
            rows.append(dict(factor_id=f, question_id=q, linked=bool(L), channel=ch, n_events=len(h), beta=b, se=se,
                             tau=tau, shrunk=s, shrunk_se=sse, p_effect=pe, units=c["units"], events=json.dumps(audit(h))))
        bh(rows)
        # detectable (linked, or P(effect) >= MIN_P_EFFECT) and weak associations are ranked and kept separately
        det = [r for r in rows if r["linked"] or (r["p_effect"] or 0) >= MIN_P_EFFECT]
        out += ranked(det, live, keep, n_data) + ranked([r for r in rows if r not in det], live, keep, n_data)
        cover[f] = dict(evaluated=n_data, estimated=len(rows), null_share=round(pi0, 3), tau1=round(math.sqrt(t2), 4))
    return out, cover

def issuer_part(g, cur, events, live, keep):
    with open(Path(__file__).parent / "securities.csv") as f:
        prim = {r["ticker"]: r["issuer_id"] for r in csv.DictReader(f) if r["issuer_id"] and r["primary"]}
    start = (datetime.now(timezone.utc) - timedelta(weeks=LOOKBACK_WEEKS + 1)).date()
    F, S = load_series(cur, prim, start)
    reg = Regressions(F, S)
    loadings, _ = api.load_loadings(cur)
    qf = defaultdict(dict)  # question -> {factor: shrunk beta}: the factors the question is measured to move
    for (q, f), b in api.load_betas(cur).items():
        if f in F and b["shrunk_beta"] is not None and q in g.nodes: qf[q][f] = b["shrunk_beta"]
    daily = returns(cur, list(dict.fromkeys(BENCHMARKS + sorted(prim))))
    bench = daily.get(BENCHMARKS[0], {})
    qs = sorted(q for q in set(events) | set(qf) if q in g.nodes)
    log.info("%d securities, %d questions with jump days or measured factors", len(prim), len(qs))

    acc, adj, fits = [], {}, {}  # acc: (ticker, question, implied, beta, se) for every pair with data
    for i, t in enumerate(sorted(prim)):
        if t not in daily: continue
        a = market_adjusted(daily[t], bench, ((loadings.get(t) or {}).get(MARKET) or {}).get("beta", 1.0))
        if len(a) <= 20: continue
        sd, days = math.sqrt(sum(x * x for x in a.values()) / len(a)), sorted(a)
        adj[t] = (a, days, sd)
        for q in qs:
            implied, fs = None, qf.get(q)
            if fs and t in S:
                k = (t, tuple(sorted(fs)))
                if k not in fits: fits[k] = reg.fit(t, sorted(set(fs) | {MARKET}), with_se=False)
                if fits[k]: implied = sum(fits[k][f][0] * fs[f] for f in fs if f in fits[k])
            sxx = sxy = 0.0
            for d, _, dp in events.get(q, ()):
                j = bisect.bisect_left(days, d)
                if j < len(days): sxx += dp * dp; sxy += dp * a[days[j]]
            b, se = (sxy / sxx, sd / math.sqrt(sxx)) if sxx else (None, None)
            if b is not None or implied is not None: acc.append((t, q, implied, b, se))
        if i % 100 == 0: log.info("  %d / %d securities, %d pairs", i, len(prim), len(acc))
    tau2 = dl_tau2([(b - im, se * se) for _, _, im, b, se in acc if im is not None and b is not None]) or 0.0
    tau0 = dl_tau2([(b, se * se) for _, _, im, b, se in acc if im is None and b is not None])
    tau02 = tau0 if tau0 is not None else tau2
    log.info("%d pairs; tau (vs implied) %.3f, tau0 (vs 0) %.3f", len(acc), math.sqrt(tau2), math.sqrt(tau02))

    by = defaultdict(list)
    for t, q, im, b, se in acc:
        s, sse = shrink_to(b, se, *((im, tau2 + im * im / 4) if im is not None else (0.0, tau02)))
        if s is not None:
            by[t].append(dict(ticker=t, issuer_id=prim[t], question_id=q, implied=im, beta=b, se=se,
                              tau=math.sqrt(tau2 if im is not None else tau02), shrunk=s, shrunk_se=sse, units="%"))
    chans, out = issuer_channels(g), []
    for t, rows in by.items():
        bh(rows)
        a, days, _ = adj[t]
        ch = chans.get(prim[t], {})
        for r in rows:
            r["channels"] = ";".join((["factors"] if r["implied"] is not None else []) + ch.get(r["question_id"], []))
        # pairs with an economic channel and statistical-only ones are ranked (and kept) separately
        kept = (ranked([r for r in rows if r["channels"]], live, keep, len(rows))
                + ranked([r for r in rows if not r["channels"]], live, keep, len(rows)))
        for r in kept:  # audit trail only for the rows kept
            q = r["question_id"]
            h = [(d, p, dp, a[days[j]]) for d, p, dp in events.get(q, ()) if (j := bisect.bisect_left(days, d)) < len(days)]
            fit = fits.get((t, tuple(sorted(qf.get(q, {})))))
            out.append(dict(r, n_events=len(h), events=json.dumps(audit(h)),
                            implied_factors=json.dumps({f: round(v[0], 4) for f, v in fit.items()} if fit else {})))
    return out, dict(securities=len(adj), pairs=len(acc), tau=round(math.sqrt(tau2), 4), tau0=round(math.sqrt(tau02), 4))

def run(args):
    def job(con, run_id, errors, stats):
        cur = cursor(con)
        cur.execute("SET TimeZone = 'UTC'")
        t = datetime.now(timezone.utc)
        g = api.Graph(cur, t, t)
        qs = [q for q, n in g.nodes.items() if n["type"] == "question"]
        events = question_events(g, cur, qs, args.jump, args.since)
        qp = api.question_probs(cur, g, qs, t)
        live = {q for q in qs if qp[q]["status"] == "live"}
        frows, fcover = factor_part(g, cur, events, live, args.keep)
        irows, icover = issuer_part(g, cur, events, live, args.keep)
        stats |= {"questions": len(qs), "live": len(live), "with_jumps": sum(1 for q in qs if events.get(q)),
                  "live_with_jumps": sum(1 for q in live if events.get(q)), "factors": len(fcover),
                  "factor_rows": len(frows), "issuer_rows": len(irows), **{f"issuers_{k}": v for k, v in icover.items()},
                  "jump": args.jump, "since": args.since, "dry_run": args.dry_run}
        log.info("%s", stats)
        if args.dry_run:
            for f, c in sorted(fcover.items()):
                rs = [r for r in frows if r["factor_id"] == f]
                print(f"{f:28} evaluated {c['evaluated']:5}  estimated {c['estimated']:5}  null share {c['null_share']:.3f}  tau1 {c['tau1']:7.3f}  top20 nonzero "
                      f"{sum(1 for r in rs[:20] if abs(r['shrunk']) > 1e-9):2}  linked {sum(1 for r in rs[:20] if r['linked']):2}")
            for r in [r for r in irows if r["ticker"] in ("AAPL", "XOM", "LMT") and r["rank"] <= 12] + [r for r in frows if r["factor_id"] in ("brent", "rates_us_10y") and r["rank"] <= 12]:
                print(r.get("ticker") or r["factor_id"], r["rank"], f"{r['shrunk'] / 10:+.3f}/10pts n={r['n_events']} q={r['q'] if r['q'] is None else round(r['q'], 3)} implied={r.get('implied') is not None}", g.nodes[r["question_id"]]["name"][:60])
        else:
            insert(con, "factor_event_betas", [dict(r, run_id=run_id, computed_at=t) for r in frows])
            insert(con, "issuer_event_betas", [dict(r, run_id=run_id, computed_at=t) for r in irows])
        return True
    return run_job("event_betas", job)

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--jump", type=float, default=0.05, help="min lasting |daily change| in probability (default 0.05)")
    ap.add_argument("--since", default="2021-01-01")
    ap.add_argument("--keep", type=int, default=KEEP, help=f"live questions stored per security / factor (default {KEEP})")
    ap.add_argument("--dry-run", action="store_true", help="print coverage and a few top rows instead of writing")
    sys.exit(run(ap.parse_args()))
