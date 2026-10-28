"""
Question ladders as probability curves (used by api.py).

A deadline ladder ("China invades Taiwan by Jun 2026 / Dec 2026 / Dec 2027") prices points on one
cumulative curve F(T) = P(event by T). Differencing it gives what credit investors think in:
the probability of the event in each period, conditional on it not having happened before
(the per-period default probability, in credit terms), and the annualized hazard rate — so it can
be set against a bond's maturity or a CDS curve.

  coherence()  monotonicity check. Deadlines: P(by later) >= P(by earlier). Thresholds within one
               period: P(> higher) <= P(> lower), P(< higher) >= P(< lower). A violation larger
               than TOLERANCE (quote noise) means a wrong grouping or a mispricing.
  curve()      the deadline curve, made monotone by weighted isotonic regression (pool adjacent
               violators; weight = 1 + ln(1 + open interest or 24h volume)), with per-period
               marginal and conditional probabilities and the annualized hazard
               -ln(S(T_i) / S(T_i-1)) / years, S = 1 - F.
  prob_by()    P(event by any date) from the fitted curve: hazard constant within each period
               (survival log-linear between deadlines), S = 1 at the as-of date. No extrapolation
               past the last deadline unless asked (it then uses the last period's hazard).

Only live rungs with a deadline after the as-of date take part: an expired or unpriced rung says
nothing about the future.

Some deadline ladders price a state, not an event: "US x Iran ceasefire continues?" asks whether it
still holds at each date, so its prices fall with the deadline. survival() spots them by wording;
for those the curve is of the opposite event (the ceasefire ends: F = 1 - price), so the hazard is
the rate at which the state breaks, and the coherence check runs the other way.
"""
import math, re
from datetime import date

TOLERANCE = 0.02   # probability points of disagreement tolerated before flagging a violation
YEAR = 365.25

SURVIVAL = re.compile(r"\b(?:continues?|remains?|still|stays?|in (?:effect|place|power|office))\b", re.I)

def survival(name):
    """True when a deadline question asks whether a state persists until each date."""
    return bool(SURVIVAL.search(name or ""))

def _day(x):
    return x if isinstance(x, date) else date.fromisoformat(str(x)[:10]) if x else None

def weight(p):
    return 1 + math.log1p(max(p.get("open_interest") or 0, p.get("volume_24h") or 0))

def pava(ys, ws):
    """Weighted isotonic (non-decreasing) regression: pool adjacent violators."""
    blocks = []  # [mean, weight, count]
    for y, w in zip(ys, ws):
        blocks.append([y, w, 1])
        while len(blocks) > 1 and blocks[-2][0] > blocks[-1][0]:
            (m1, w1, c1), (m2, w2, c2) = blocks[-2], blocks.pop()
            blocks[-1] = [(m1 * w1 + m2 * w2) / (w1 + w2), w1 + w2, c1 + c2]
    return [m for m, _, c in blocks for _ in range(c)]

def deadline_points(rungs, pp, as_of, invert=False):
    """Live rungs with a deadline after `as_of`, one point per deadline (rungs sharing a deadline
    — e.g. different start dates — averaged by weight), earliest first. rungs = [(pid, window,
    threshold)], pp = {pid: dict(prob, open_interest, volume_24h, ...)}. invert: prob = 1 - price
    (survival questions)."""
    by_day = {}
    for pid, window, _ in rungs:
        p, end = pp.get(pid) or {}, _day((window or [None, None])[1])
        if p.get("prob") is None or not end or end <= as_of: continue
        by_day.setdefault(end, []).append((pid, p))
    out = []
    for end, xs in sorted(by_day.items()):
        ws = [weight(p) for _, p in xs]
        prob = sum(p["prob"] * w for (_, p), w in zip(xs, ws)) / sum(ws)
        out.append(dict(deadline=end, rungs=[pid for pid, _ in xs], weight=sum(ws), prob=1 - prob if invert else prob))
    return out

def coherence(kind, rungs, pp, as_of, invert=False):
    """{ok, violations}: pairs of live rungs whose prices contradict the ladder's logic by more than
    TOLERANCE. Each violation names the rung that should be lower and the one that should be higher.
    invert: a survival question, whose prices must fall (not rise) with the deadline."""
    bad = []
    if kind == "deadlines":
        pts = deadline_points(rungs, pp, as_of, invert)
        for i, a in enumerate(pts):
            for b in pts[i + 1:]:
                if round(a["prob"] - b["prob"], 4) > TOLERANCE:
                    pa, pb = (1 - a["prob"], 1 - b["prob"]) if invert else (a["prob"], b["prob"])
                    lo, hi = (b, a) if invert else (a, b)
                    bad.append(dict(rule="P(still true at a later date) <= P(at an earlier one)" if invert
                                    else "P(by later deadline) >= P(by earlier)", should_be_lower=lo["rungs"][0],
                                    should_be_higher=hi["rungs"][0], gap=round(a["prob"] - b["prob"], 4),
                                    detail=f"{a['deadline']}: {pa:.3f} vs {b['deadline']}: {pb:.3f}"))
    elif kind == "thresholds":
        groups = {}
        for pid, window, t in rungs:
            p = pp.get(pid) or {}
            if p.get("prob") is None or not t or t[0] not in (">", ">=", "<", "<=") or isinstance(t[1], list): continue
            groups.setdefault((tuple(window or ()), t[0][0]), []).append((t[1], pid, p["prob"]))
        for (_, op), xs in groups.items():
            xs.sort()
            for i, (ta, a, pa) in enumerate(xs):
                for tb, b, pb in xs[i + 1:]:
                    if tb == ta: continue
                    # ">": the higher bar is harder -> P(b) <= P(a); "<": the higher bar is easier -> P(b) >= P(a)
                    lo, hi, gap = ((b, a, pb - pa) if op == ">" else (a, b, pa - pb))
                    if round(gap, 4) > TOLERANCE:
                        bad.append(dict(rule="P(> higher) <= P(> lower)" if op == ">" else "P(< higher) >= P(< lower)",
                                        should_be_lower=lo, should_be_higher=hi, gap=round(gap, 4),
                                        detail=f"{op} {ta}: {pa:.3f} vs {op} {tb}: {pb:.3f}"))
    return dict(ok=not bad, violations=sorted(bad, key=lambda v: -v["gap"]))

def curve(points, as_of):
    """Rows per deadline: prob (blended market price), fitted (monotone), period_start,
    period_prob = F(T_i) - F(T_i-1), conditional_prob = period_prob / S(T_i-1),
    hazard_annual = -ln(S(T_i) / S(T_i-1)) / years (None where undefined)."""
    fitted = pava([p["prob"] for p in points], [p["weight"] for p in points])
    rows, prev_t, prev_f = [], as_of, 0.0
    for p, f in zip(points, fitted):
        f = min(max(f, 0.0), 1.0)
        s_prev, s = 1 - prev_f, 1 - f
        years = (p["deadline"] - prev_t).days / YEAR
        rows.append(dict(deadline=p["deadline"], rungs=p["rungs"], prob=round(p["prob"], 4), fitted=round(f, 4),
                         period_start=prev_t, period_prob=round(f - prev_f, 4),
                         conditional_prob=round((f - prev_f) / s_prev, 4) if s_prev > 0 else None,
                         hazard_annual=round(-math.log(s / s_prev) / years, 4) + 0.0 if s > 0 and s_prev > 0 and years > 0 else None))
        prev_t, prev_f = p["deadline"], f
    return rows

def prob_by(rows, as_of, when, extrapolate=False):
    """P(event by `when`) from curve() rows; None past the last deadline unless `extrapolate`."""
    when = _day(when)
    if when <= as_of: return 0.0
    prev_t, prev_f = as_of, 0.0
    for r in rows:
        if when <= r["deadline"]:
            span = (r["deadline"] - prev_t).days
            frac = (when - prev_t).days / span if span else 1.0
            s_prev, s = 1 - prev_f, 1 - r["fitted"]
            if s_prev <= 0: return 1.0
            if s <= 0: return round(prev_f + (r["fitted"] - prev_f) * frac, 4)  # certain by the deadline: linear
            return round(1 - s_prev * (s / s_prev) ** frac, 4)
        prev_t, prev_f = r["deadline"], r["fitted"]
    if not extrapolate or not rows or rows[-1]["hazard_annual"] is None: return None
    last = rows[-1]
    return round(1 - (1 - last["fitted"]) * math.exp(-last["hazard_annual"] * (when - last["deadline"]).days / YEAR), 4)
