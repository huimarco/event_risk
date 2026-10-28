#!/usr/bin/env python3
"""
Event -> factor sensitivities: how much each factor moved when a question's probability jumped.
Turns the qualitative question -> factor links (factors.py) into numbers a risk system can use:
the factor move per +1.0 of probability (100 points), in the factor's units.

For every question -> factor `relevant_to` edge in the current graph:
  1. Events: lasting daily jumps of the question's rungs (backtest.lasting_jumps: |dp| >= --jump and
     at least half the move still there a day later); per day, the rung that moved most speaks for
     the question.
  2. Factor move: from the factor's last close before the jump day to its close on the second
     trading day at or after it (a jump late in a UTC day lands after many closes), in the
     factor's units (seeds/factors.csv): % = log return x 100, bp = change in level x 100 (FRED
     quotes spreads and yields in percent).
  3. Own estimate: beta = sum(dp * move) / sum(dp^2) (regression through the origin), se = sigma /
     sqrt(sum(dp^2)) with sigma = the factor's own standard deviation of two-day moves over all
     days — with one to three events per question a residual variance would mean nothing.
  4. Pooling: questions sharing a factor and channel (matching.EFFECTS: conflict, sanctions, credit,
     ...) are oriented so that +dp is bad news for the entity that links them (orientation = minus
     the `involves` sign: an invasion and a ceasefire then point the same way) and pooled:
     inverse-variance mean (pooled_beta, per +1.0 of adverse probability) and the spread between
     questions, tau (DerSimonian-Laird; with fewer than MIN_POOL questions tau^2 = pooled_beta^2 +
     pooled_se^2, a deliberately weak prior).
  5. Shrinkage (empirical Bayes): shrunk_beta mixes the question's own beta with its prior,
     orientation x pooled_beta with variance tau^2 + pooled_se^2, by precision. A question with no
     events gets the prior; one whose orientation is unknown shrinks toward 0 with variance
     tau^2 + pooled_beta^2. shrunk_beta is per +1.0 of the question's own YES probability.

Caveats (also in the API docs): jump days aren't independent (one news day moves many questions
and every factor); a jump day's factor move includes everything else that happened that day; each
question's own events are also in its pool; the graph is today's, not point-in-time; ICE spread
indices on FRED start in late 2023, so credit factors see only events since then.

  python factor_betas.py [--jump 0.05] [--since 2021-01-01] [--dry-run]

Appends one full set of rows to factor_betas per run; the API reads the latest run.
"""
import argparse, bisect, json, math, sys
from collections import Counter, defaultdict
from datetime import date, datetime, timezone

import api, factors
from backtest import daily_probs, lasting_jumps
from collector import cursor, insert, log, now, run_job

MIN_POOL = 3     # questions needed to estimate the between-question spread tau
MAX_EVENTS = 10  # events kept per row for audit (largest moves)

def factor_moves(cur):
    """{factor id: (trading days, {day: two-day move starting that day}, sigma)} in factor units."""
    levels = defaultdict(list)
    for f, d, v in cur.execute("""SELECT factor_id, date::VARCHAR, arg_max(level, fetched_at) FROM factor_prices
                                  GROUP BY ALL ORDER BY 1, 2""").fetchall():
        if v is not None: levels[f].append((date.fromisoformat(d), v))
    out = {}
    for f, xs in levels.items():
        if f not in factors.CATALOG: continue
        pct = factors.CATALOG[f]["units"] == "%"
        move = (lambda a, b: 100 * math.log(b / a)) if pct else (lambda a, b: 100 * (b - a))
        days, vals = [d for d, _ in xs], [v for _, v in xs]
        if pct and min(vals) <= 0: continue
        moves = {days[i]: move(vals[i - 1], vals[i + 1]) for i in range(1, len(days) - 1)}
        if len(moves) < 20: continue
        sigma = math.sqrt(sum(m * m for m in moves.values()) / len(moves))
        out[f] = (days, moves, sigma)
    return out

def reaction(series, day):
    """The factor's move for a jump on `day`: the window starting at its first trading day >= day."""
    days, moves, _ = series
    i = bisect.bisect_left(days, day)
    return moves.get(days[i]) if i < len(days) else None

def orientation(g, qid, paths):
    """+1 if YES is adverse for every entity the link goes through, -1 if favourable for all,
    None if unknown or mixed. From the question's `involves` signs."""
    sign = {e["dst"]: e["sign"] for e in g.out[qid] if e["type"] == "involves"}
    # curated paths carry their own sign (what YES means for the factor); stored negated like an entity
    # sign, so a factor expected to rise on YES gets orientation +1
    signs = {(-p["sign"] if p.get("sign") else None) if p.get("channel") == "curated" else sign.get(p["nodes"][0]) for p in paths}
    return -signs.pop() if len(signs) == 1 and None not in signs else None

def question_events(g, cur, qids, jump, since):
    """{question: [(day, rung, dp)]}: lasting daily jumps of each question's rungs, the rung that
    moved most speaking for the question on each day."""
    rungs = {q: [p for p, _, _ in g.rungs(q)] for q in qids}
    members = {p: [m for m, rel, _ in g.markets(p) if rel in api.BLENDED] for ps in rungs.values() for p in ps}
    log.info("%d questions, %d rungs", len(rungs), len(members))
    jumps = lasting_jumps(daily_probs(cur, members, since), jump)
    events = defaultdict(list)
    question_of = {p: q for q, ps in rungs.items() for p in ps}
    for d, moved in jumps.items():
        best = {}
        for p, dp in moved.items():
            q = question_of[p]
            if q not in best or abs(dp) > abs(best[q][1]): best[q] = (p, dp)
        for q, (p, dp) in best.items():
            events[q].append((d, p, dp))
    return events

def estimate(g, cur, jump, since):
    links = [(q, e["dst"][7:], e["attrs"].get("paths") or []) for q, n in g.nodes.items() if n["type"] == "question"
             for e in g.out[q] if e["type"] == "relevant_to"]
    log.info("%d question -> factor links", len(links))
    events = question_events(g, cur, {q for q, _, _ in links}, jump, since)
    series = factor_moves(cur)

    rows = []
    for q, f, paths in links:
        if f not in series: continue
        channel = Counter(p.get("channel") for p in paths).most_common(1)[0][0] if paths else None
        hits = [(d, p, dp, y) for d, p, dp in events[q] if (y := reaction(series[f], d)) is not None]
        sxx = sum(dp * dp for _, _, dp, _ in hits)
        beta = sum(dp * y for _, _, dp, y in hits) / sxx if sxx else None
        se = series[f][2] / math.sqrt(sxx) if sxx else None
        rows.append(dict(question_id=q, factor_id=f, channel=channel, orientation=orientation(g, q, paths),
                         n_events=len(hits), beta=beta, se=se, units=factors.CATALOG[f]["units"],
                         events=[dict(day=str(d), rung=p, dp=round(dp, 4), move=round(y, 3))
                                 for d, p, dp, y in sorted(hits, key=lambda h: -abs(h[2]))[:MAX_EVENTS]]))
    return shrink(rows)

def shrink(rows):
    """Adds pooled_beta / pooled_se / tau per (factor, channel) and shrunk_beta / shrunk_se per row."""
    pools = defaultdict(list)
    for r in rows:
        if r["n_events"] and r["orientation"] is not None and r["se"]:
            pools[(r["factor_id"], r["channel"])].append((r["orientation"] * r["beta"], r["se"] ** 2))
    pooled = {}
    for k, xs in pools.items():
        w = [1 / v for _, v in xs]
        mu = sum(z * wi for (z, _), wi in zip(xs, w)) / sum(w)
        mu_se = 1 / math.sqrt(sum(w))
        if len(xs) >= MIN_POOL:
            q = sum(wi * (z - mu) ** 2 for (z, _), wi in zip(xs, w))
            tau2 = max(0.0, (q - (len(xs) - 1)) / (sum(w) - sum(wi * wi for wi in w) / sum(w)))
        else:
            tau2 = mu * mu + mu_se * mu_se
        pooled[k] = (mu, mu_se, math.sqrt(tau2))
    out = []
    for r in rows:
        mu, mu_se, tau = pooled.get((r["factor_id"], r["channel"]), (None, None, None))
        if mu is None:
            prior = None
        elif r["orientation"] is not None:
            prior = (r["orientation"] * mu, tau * tau + mu_se * mu_se)
        else:
            prior = (0.0, tau * tau + mu * mu)
        if prior and prior[1] > 0 and r["se"]:
            prec = 1 / r["se"] ** 2 + 1 / prior[1]
            shrunk, shrunk_se = (r["beta"] / r["se"] ** 2 + prior[0] / prior[1]) / prec, 1 / math.sqrt(prec)
        elif prior and prior[1] > 0:
            shrunk, shrunk_se = prior[0], math.sqrt(prior[1])
        elif r["se"]:
            shrunk, shrunk_se = r["beta"], r["se"]
        else:
            continue  # no events and no pool: nothing to say
        out.append(dict(r, pooled_beta=mu, pooled_se=mu_se, tau=tau, shrunk_beta=shrunk, shrunk_se=shrunk_se))
    return out

def summary(rows):
    by = defaultdict(list)
    for r in rows: by[(r["factor_id"], r["channel"])].append(r)
    print(f"{'factor':18} {'channel':12} {'questions':>9} {'w/ events':>9} {'events':>7} {'pooled/+1.0 adverse':>20} {'tau':>8}  units")
    for (f, ch), rs in sorted(by.items(), key=lambda kv: -sum(r["n_events"] for r in kv[1])):
        r0 = rs[0]
        pooled = "" if r0["pooled_beta"] is None else f"{r0['pooled_beta']:+.2f} ± {r0['pooled_se']:.2f}"
        print(f"{f:18} {str(ch):12} {len(rs):9} {sum(1 for r in rs if r['n_events']):9} {sum(r['n_events'] for r in rs):7} "
              f"{pooled:>20} {r0['tau'] if r0['tau'] is not None else float('nan'):8.2f}  {r0['units']}")

def run(args):
    def job(con, run_id, errors, stats):
        cur = cursor(con)
        cur.execute("SET TimeZone = 'UTC'")
        t = datetime.now(timezone.utc)
        rows = estimate(api.Graph(cur, t, t), cur, args.jump, args.since)
        if args.dry_run:
            summary(rows)
        else:
            insert(con, "factor_betas", [dict(r, run_id=run_id, computed_at=t, events=json.dumps(r["events"])) for r in rows])
        stats |= {"rows": len(rows), "with_events": sum(1 for r in rows if r["n_events"]),
                  "events": sum(r["n_events"] for r in rows), "jump": args.jump, "since": args.since, "dry_run": args.dry_run}
        return True
    return run_job("factor_betas", job)

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--jump", type=float, default=0.05, help="min lasting |daily change| in probability (default 0.05)")
    ap.add_argument("--since", default="2021-01-01")
    ap.add_argument("--dry-run", action="store_true", help="print a summary instead of writing factor_betas")
    sys.exit(run(ap.parse_args()))
