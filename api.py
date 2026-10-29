#!/usr/bin/env python3
"""
Knowledge-graph API for geopolitical event risk.

  uvicorn api:app            (docs at /docs)

Auth: set API_KEYS="key1,key2" to require an `X-API-Key` header; unset = open (local dev).

Every read serves the current graph (refreshed every GRAPH_TTL seconds) and the latest prices;
`as_of` in a response is when it was computed. The stored graph is bitemporal, but point-in-time
reads (as_of / known_as_of) are not exposed yet.

Market -> proposition -> question. A proposition is one bet (one deadline / threshold), possibly
listed on several venues; its probability = blend of its markets' latest cleaned prices (within
the last 7 days), weighted by open interest when every market reports it, else
equally. Markets join a proposition only when their contract signatures match (matching.py);
propositions with materially different terms are linked as `near_equivalent` and never blended.
A question groups the propositions that differ only in deadline ("China invades Taiwan by Jun
2026 / Dec 2026 / Dec 2027") or, for one measurement period, in threshold: its rungs. Its
headline probability is its most-traded live rung (24h volume, then open interest). A
proposition or question is `live` when it has a price in the last 7 days, else `closed`.
Entities (and through them issuers) attach to questions.

Entities are discovered from market titles via Wikipedia; entity links come from Wikidata statements
and Wikipedia co-occurrence.

Securities (securities.csv) hang off their issuer; how much each one moves with a question or a
factor is measured from prices (security_betas.py): factor loadings from weekly returns, and per
question a factor-implied move (its factor moves x the security's joint loadings) and a direct
market-adjusted move on the question's jump days, shrunk toward the implied one.
Issuer exposure to a question, summed over every path question -> entity [-> linked entity] ->
issuer. Each path's strength = involves weight (severity x role) x pass-through (1, or
LINK_DAMPING x link weight for a linked entity) x exposure weight; its sign = what YES means for
the entity x how the issuer is affected on that channel (seeds/channel_signs.csv).
  score   relevance in [0, 1]: noisy-OR of path strengths
  impact  signed structural sensitivity: sum of signed path strengths (> 0: the issuer is
          expected to benefit if the event happens). A prior in graph units, not a measured beta —
          backtest.py measures how it maps to the issuer's stock returns.
  risk    |impact| x sqrt(p (1 - p)): how much the event can still move the issuer now
Weights are analyst priors (seeds/exposures.csv) until empirical event betas exist.

Factors (equity indices, FX, commodities, rates, credit spreads; seeds/factors.csv) are where the
graph meets a risk system: questions link to factors by rule (factors.py), and factor_betas.py
measures each link — the factor's move per +1.0 of the question's probability, in the factor's
units (% or bp), shrunk toward questions sharing the factor and channel. `move_if_yes` /
`move_if_no` = the factor move if the question resolves now, given today's probability.

Factors come in two layers (factors.py): macro (the market factors above plus economic
indicators: CPI, GDP, payrolls, PMIs — monthly series, not measured) and niche (an industry's
revenue / cost drivers: load factor, jet fuel cost, NIM, wafer demand). Factors drive each other
(`drives`, signed: macro -> niche), niche factors drive GICS industries (`driver_of`, signed by
the effect on earnings) and issuers are classified into industries (`classified_as`) — the
transmission path question -> macro factor -> niche factor -> industry <- issuer. Hand-written
analyst priors, not measured; `/industries`, `/industries/{code}`.

Deadline ladders are also a term structure (ladders.py): `/questions/{id}/curve` gives the
probability per period, conditional on not before (a hazard rate), and P(event by any date).
Resolved markets (market_resolutions) give `/calibration`: how often a price of p came true.
"""
import json, math, os, threading, time
from contextlib import asynccontextmanager
from collections import defaultdict, deque
from datetime import date, datetime, timedelta, timezone

from pathlib import Path

import duckdb
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles

import factors, ladders
from collector import DB

LOOKBACK = timedelta(days=7)
MUTEX_SHOWN = 3  # per node in /graph neighbourhoods
GRAPH_TTL = 300  # seconds the current graph is reused; it only changes when graph_build runs
BLENDED, LOGICAL = {"same_as"}, {"mutex", "implies", "near_equivalent"}
API_KEYS = {k for k in os.getenv("API_KEYS", "").split(",") if k}
ISO = "strftime({} AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ')"
INTERVALS = {"1h": "1 hour", "1d": "1 day"}
LINK_DAMPING = 0.5  # share of an entity's shock passed to a linked entity, x the link's weight
MATCH_STATUSES = ("unmatched", "unparsed", "linked", "merged")

def require_key(request: Request, x_api_key: str | None = Header(None)):
    if request.url.path == "/" or request.url.path.startswith("/excel/"):
        return  # the web page and Excel add-in files are public; their data calls are not
    if API_KEYS and x_api_key not in API_KEYS:
        raise HTTPException(401, "missing or invalid X-API-Key")

@asynccontextmanager
async def lifespan(app):
    # load the current graph while the server starts, so the first visitor doesn't wait
    threading.Thread(target=lambda: _current["graph"] is None and _load_current(), daemon=True).start()
    yield

app = FastAPI(title="Event Risk Knowledge Graph", version="0.1.0", lifespan=lifespan,
              description=__doc__, dependencies=[Depends(require_key)])

_con, _con_lock = None, threading.Lock()

def connection():
    """One shared DuckDB/MotherDuck connection, opened on first use."""
    global _con
    with _con_lock:
        if _con is None:
            _con = duckdb.connect(DB) if DB.startswith("md:") else duckdb.connect(DB, read_only=True)
    return _con

def db():
    cur = connection().cursor()  # one cursor per request
    cur.execute("SET TimeZone = 'UTC'")
    try: yield cur
    finally: cur.close()

IncludeClosed = Query(False, description="Include closed items: no price in the last 7 days (resolved / delisted). Default: live only.")

# ── graph + prices ────────────────────────────────────────────────────────────
class Graph:
    """The graph at valid time `as_of` as recorded by `known` (nodes_asof / edges_asof)."""
    def __init__(self, cur, as_of, known):
        self.nodes = {r[0]: dict(id=r[0], type=r[1], name=r[2], attrs=json.loads(r[3] or "{}"), valid_from=r[4])
                      for r in cur.execute(f"""
            SELECT node_id, node_type, name, attrs::VARCHAR, {ISO.format('valid_from')}
            FROM nodes_asof(?, ?)""", [as_of, known]).fetchall()}
        self.edges = [dict(src=r[0], dst=r[1], type=r[2], weight=r[3], confidence=r[4],
                           provenance=r[5], note=r[6], attrs=json.loads(r[7] or "{}"), sign=r[8])
                      for r in cur.execute("""
            SELECT src_id, dst_id, edge_type, weight, confidence, provenance, note, attrs::VARCHAR, sign
            FROM edges_asof(?, ?)""", [as_of, known]).fetchall()]
        self.latest = None  # on the shared "now" graph: every market's price now and 24h ago
        self.betas, self.levels = {}, {}  # on the shared graph: factor sensitivities and latest factor levels
        self.loadings, self.sec_betas = {}, {}  # on the shared graph: security loadings, question -> security sensitivities
        self.loadings_run, self.level_info = None, {}  # latest security_betas run; factor history coverage
        self.out, self.inc = defaultdict(list), defaultdict(list)
        for e in self.edges:
            self.out[e["src"]].append(e); self.inc[e["dst"]].append(e)

    def get(self, node_id, node_type):
        n = self.nodes.get(node_id)
        if not n or n["type"] != node_type:
            raise HTTPException(404, f"{node_type} {node_id} not found")
        return n

    def ref(self, node_id):
        n = self.nodes[node_id]
        return dict(id=n["id"], type=n["type"], name=n["name"])

    def linked(self, ent):
        """Entities linked to `ent` (Wikidata statements and co-occurrence), either direction:
        [(edge, other entity id)]."""
        return [(e, e[k]) for es, k in ((self.out[ent], "dst"), (self.inc[ent], "src")) for e in es
                if self.nodes[e[k]]["type"] == "entity" and self.nodes[ent]["type"] == "entity"]

    def markets(self, pid):
        return [(e["src"], e["type"], e["note"]) for e in self.inc[pid] if self.nodes[e["src"]]["type"] == "market"]

    def rungs(self, qid):
        """A question's propositions with their rung terms: [(pid, window, threshold)]."""
        return [(e["src"], (e["attrs"] or {}).get("window"), (e["attrs"] or {}).get("threshold"))
                for e in self.inc[qid] if e["type"] == "rung_of"]

    def entity(self, key):
        """An entity id, or an entity name (case-insensitive; the most-discussed one if several
        share it) -> entity id; None if neither."""
        n = self.nodes.get(key)
        if n and n["type"] == "entity": return key
        k = key.strip().lower()
        hits = [i for i, n in self.nodes.items() if n["type"] == "entity" and n["name"].lower() == k]
        return max(hits, key=lambda i: len(self.inc[i]), default=None)

    def question_of(self, pid):
        return next((e["dst"] for e in self.out[pid] if e["type"] == "rung_of"), None)

    def issuer(self, key):
        """An issuer node id from "iss:Q713418", "Q713418", "local:acme" or a name (case-insensitive,
        exact); None if none matches."""
        k = key.strip()
        for i in (k, f"iss:{k}"):
            if (self.nodes.get(i) or {}).get("type") == "issuer": return i
        return next((i for i, n in self.nodes.items() if n["type"] == "issuer" and n["name"].lower() == k.lower()), None)

    def exposures_of(self, sec):
        """{question_id: dict(score, impact, paths)} for one issuer, walking back from it over
        every path issuer <- exposure - entity [<- link -> entity] <- involves - question.
        A path = dict(strength, sign, channel, role, nodes); sign None = direction unknown."""
        paths = defaultdict(list)
        for x in self.inc[sec]:
            if x["type"] != "exposure": continue
            held, responds = x["src"], (x["attrs"] or {}).get("channel_signs") or {}
            hops = [(held, 1.0, [held])]
            hops += [(other, LINK_DAMPING * (l["weight"] or 0), [other, held]) for l, other in self.linked(held)]
            for ent, passed, via in hops:
                if passed <= 0: continue
                for inv in self.inc[ent]:
                    if inv["type"] != "involves": continue
                    a = inv["attrs"] or {}
                    strength = (inv["weight"] or 0) * passed * (x["weight"] or 0)
                    if strength <= 0: continue
                    sign = None if inv["sign"] is None else inv["sign"] * responds.get(a.get("channel"), x["sign"] or 1.0)
                    paths[inv["src"]].append(dict(strength=strength, sign=sign, channel=a.get("channel"),
                                                  role=a.get("role"), nodes=[inv["src"], *via, sec]))
        out = {}
        for p, ps in paths.items():
            ps.sort(key=lambda q: -q["strength"])
            signed = [q["strength"] * q["sign"] for q in ps if q["sign"] is not None]
            out[p] = dict(score=round(1 - math.prod(1 - min(q["strength"], 1.0) for q in ps), 4),
                          impact=round(sum(signed), 4) if signed else None, paths=ps)
        return out

_current = {"graph": None, "loaded": 0.0}   # the graph "as of now", shared by requests
_reloading = threading.Lock()

def _load_current():
    cur = connection().cursor()
    cur.execute("SET TimeZone = 'UTC'")
    now = datetime.now(timezone.utc)
    g = Graph(cur, now, now)
    g.latest = dict(at=now, now=all_market_probs(cur, now), ago=all_market_probs(cur, now - timedelta(hours=24)))
    g.betas, g.levels = load_betas(cur), load_levels(cur)
    (g.loadings, g.loadings_run), g.sec_betas = load_loadings(cur), load_sec_betas(cur)
    g.level_info = load_level_info(cur)
    _current.update(graph=g, loaded=time.monotonic())

BETA_COLS = ("question_id", "factor_id", "channel", "orientation", "n_events", "beta", "se", "pooled_beta", "pooled_se",
             "tau", "shrunk_beta", "shrunk_se", "units", "events", "computed_at")

def load_betas(cur):
    """{(question id, factor id): row} from the latest factor_betas run; empty before the first
    run (or before the table exists)."""
    try:
        rows = cur.execute(f"""SELECT {', '.join(BETA_COLS[:-2])}, events::VARCHAR, {ISO.format('computed_at')}
            FROM factor_betas WHERE run_id = (SELECT arg_max(run_id, computed_at) FROM factor_betas)""").fetchall()
    except duckdb.Error:
        return {}
    return {(r[0], r[1]): dict(zip(BETA_COLS, r)) | dict(events=json.loads(r[13] or "[]")) for r in rows}

def latest_run(table):
    return f"run_id = (SELECT arg_max(run_id, computed_at) FROM {table})"

def load_loadings(cur):
    """({ticker: {factor id: dict(beta, se, t, n_weeks, p, q)}}, run info) from the latest
    security_betas run. p = two-sided normal p-value of t; q = Benjamini-Hochberg adjusted p over
    every non-market loading in the run (the market beta is not a tested hypothesis)."""
    out, run = defaultdict(dict), None
    try:
        rows = cur.execute(f"""SELECT ticker, factor_id, beta, se, t, n_weeks, run_id, {ISO.format('computed_at')}
                               FROM security_loadings WHERE {latest_run('security_loadings')}""").fetchall()
    except duckdb.Error:
        return {}, None
    for t, f, b, se, tt, n, rid, at in rows:
        out[t][f] = dict(beta=b, se=se, t=tt, n_weeks=n, p=math.erfc(abs(tt) / math.sqrt(2)) if tt is not None else None, q=None)
        run = dict(run_id=rid, computed_at=at)
    tested = sorted(((l["p"], l) for fs in out.values() for f, l in fs.items() if f != "equity_us" and l["p"] is not None),
                    key=lambda x: x[0])
    m, q = len(tested), 1.0
    for k in range(m, 0, -1):   # BH step-up: q_(k) = min over j >= k of p_(j) m / j
        q = min(q, tested[k - 1][0] * m / k); tested[k - 1][1]["q"] = q
    if run: run["n_tests"] = m
    return dict(out), run

SEC_BETA_COLS = ("question_id", "ticker", "source", "implied", "implied_factors", "n_events", "beta", "se", "tau", "shrunk", "shrunk_se", "units", "events")

def load_sec_betas(cur):
    """{(question id, ticker): row} from the latest security_betas run."""
    try:
        rows = cur.execute(f"""SELECT question_id, ticker, source, implied, implied_factors::VARCHAR, n_events, beta, se, tau, shrunk,
                                      shrunk_se, units, events::VARCHAR FROM security_betas WHERE {latest_run('security_betas')}""").fetchall()
    except duckdb.Error:
        return {}
    return {(r[0], r[1]): dict(zip(SEC_BETA_COLS, r)) | dict(implied_factors=json.loads(r[4] or "{}"), events=json.loads(r[12] or "[]"))
            for r in rows}

def load_levels(cur):
    """{factor id: (date, level)}: each factor's latest daily level (factor_prices)."""
    try:
        return {f: (d, v) for f, d, v in cur.execute("""
            SELECT factor_id, max(date)::VARCHAR, arg_max(level, (date, fetched_at)) FROM factor_prices GROUP BY 1""").fetchall()}
    except duckdb.Error:
        return {}

def load_level_info(cur):
    """{factor id: dict(first_date, last_date, n_obs, last_fetched_at, source, series)}: how much
    observed history factor_prices holds for each factor."""
    try:
        return {f: dict(first_date=a, last_date=b, n_obs=n, last_fetched_at=fa, source=src, series=ser)
                for f, a, b, n, fa, src, ser in cur.execute(f"""
            SELECT factor_id, min(date)::VARCHAR, max(date)::VARCHAR, count(DISTINCT date), {ISO.format('max(fetched_at)')},
                   arg_max(source, fetched_at), arg_max(series, fetched_at) FROM factor_prices GROUP BY 1""").fetchall()}
    except duckdb.Error:
        return {}

def _reload_in_background():
    try: _load_current()
    finally: _reloading.release()

def graph(cur):
    """The current graph, shared by all requests and refreshed in the background every
    GRAPH_TTL seconds (callers never wait for a reload)."""
    if _current["graph"] is None:
        _load_current()
    elif time.monotonic() - _current["loaded"] > GRAPH_TTL and _reloading.acquire(blocking=False):
        threading.Thread(target=_reload_in_background, daemon=True).start()
    return _current["graph"]

def all_market_probs(cur, at):
    """Latest price of every market at `at` (within 7 days), in one query."""
    return {m: dict(prob=p, open_interest=oi, volume_24h=v, observed_at=ts) for m, p, oi, v, ts in cur.execute(f"""
        SELECT 'mkt:' || venue || ':' || market_id, arg_max(prob, ts), arg_max(open_interest, ts),
               arg_max(volume_24h, ts), {ISO.format('max(ts)')}
        FROM v_market_prob WHERE ts <= ? AND ts > ? AND prob IS NOT NULL GROUP BY 1""",
        [at, at - LOOKBACK]).fetchall()}

def market_probs(cur, market_ids, at, g=None):
    if g is not None and g.latest:  # shared "now" graph: answer from memory
        snap = g.latest["now"] if at > g.latest["at"] - timedelta(hours=12) else g.latest["ago"]
        return {m: snap[m] for m in market_ids if m in snap}
    if not market_ids: return {}
    rows = cur.execute(f"""
        SELECT 'mkt:' || venue || ':' || market_id, arg_max(prob, ts), arg_max(open_interest, ts),
               arg_max(volume_24h, ts), {ISO.format('max(ts)')}
        FROM v_market_prob
        WHERE ts <= ? AND ts > ? AND prob IS NOT NULL
          AND 'mkt:' || venue || ':' || market_id IN (SELECT unnest(?::VARCHAR[]))
        GROUP BY 1""", [at, at - LOOKBACK, list(market_ids)]).fetchall()
    return {m: dict(prob=p, open_interest=oi, volume_24h=v, observed_at=ts) for m, p, oi, v, ts in rows}

def blend(vals):
    vals = [v for v in vals if v and v["prob"] is not None]
    if not vals: return None
    w = [v["open_interest"] for v in vals]
    if not all(w): w = [1.0] * len(vals)
    return round(sum(v["prob"] * x for v, x in zip(vals, w)) / sum(w), 4)

def prop_probs(cur, g, pids, at):
    """{pid: {prob, prob_24h_ago, change_24h, status, volume_24h, open_interest}} from blended
    same_as markets; volume and open interest are summed over the proposition's markets."""
    members = {p: [m for m, rel, _ in g.markets(p) if rel in BLENDED] for p in pids}
    ids = {m for ms in members.values() for m in ms}
    now_, ago = market_probs(cur, ids, at, g), market_probs(cur, ids, at - timedelta(hours=24), g)
    total = lambda vals, k: sum(v[k] for v in vals if v and v.get(k)) or None
    out = {}
    for p, ms in members.items():
        a, b = blend([now_.get(m) for m in ms]), blend([ago.get(m) for m in ms])
        out[p] = dict(prob=a, prob_24h_ago=b, status="live" if a is not None else "closed",
                      change_24h=round(a - b, 4) if a is not None and b is not None else None,
                      volume_24h=total([now_.get(m) for m in ms], "volume_24h"),
                      open_interest=total([now_.get(m) for m in ms], "open_interest"),
                      updated=max((now_[m]["observed_at"] for m in ms if m in now_), default=None))
    return out

def contract_rows(cur, g, pids, at):
    """The venue contracts (markets) behind each proposition, in order: how each joined it
    (same_as = blended, near_equivalent = linked only), its price, liquidity and rules."""
    mkts = [(p, m, rel, note) for p in pids for m, rel, note in g.markets(p)]
    mprobs = market_probs(cur, [m for _, m, _, _ in mkts], at, g)
    return [dict(id=m, rung=g.ref(p), relation=rel, note=note, venue=g.nodes[m]["attrs"]["venue"],
                 name=g.nodes[m]["name"], close_time=g.nodes[m]["attrs"].get("close_time"),
                 rules=g.nodes[m]["attrs"].get("rules"), **(mprobs.get(m) or {}),
                 updated=(mprobs.get(m) or {}).get("observed_at"))
            for p, m, rel, note in mkts]

def proposition_summary(g, pid, probs):
    n = g.nodes[pid]
    return dict(id=pid, name=n["name"], close_time=n["attrs"].get("close_time"),
                venues=n["attrs"].get("venues"), **probs.get(pid, {}))

def question_probs(cur, g, qids, at):
    """{qid: {prob, prob_24h_ago, change_24h, status, headline, n_live}}: the headline is the
    question's most-traded live rung (24h volume, then open interest, then latest deadline)."""
    rungs = {q: g.rungs(q) for q in qids}
    pp = prop_probs(cur, g, [p for rs in rungs.values() for p, _, _ in rs], at)
    out = {}
    for q, rs in rungs.items():
        live = [(p, w) for p, w, _ in rs if pp[p]["status"] == "live"]
        head = max(live, key=lambda x: (pp[x[0]]["volume_24h"] or 0, pp[x[0]]["open_interest"] or 0,
                                        (x[1] or [None, ""])[1] or ""), default=(None, None))[0]
        h = pp.get(head, {})
        out[q] = dict(prob=h.get("prob"), prob_24h_ago=h.get("prob_24h_ago"), change_24h=h.get("change_24h"),
                      status="live" if live else "closed", headline=head, n_live=len(live), rung_probs=pp,
                      volume_24h=sum(pp[p]["volume_24h"] or 0 for p, _ in live) or None,
                      open_interest=sum(pp[p]["open_interest"] or 0 for p, _ in live) or None,
                      updated=h.get("updated"))
    return out

def question_summary(g, qid, qp):
    n, v = g.nodes[qid], qp.get(qid, {})
    a = n["attrs"]
    return dict(id=qid, name=n["name"], kind=a.get("kind"), n_rungs=a.get("n_rungs"), n_live=v.get("n_live"),
                status=v.get("status"), venues=a.get("venues"), close_time=a.get("close_time"),
                prob=v.get("prob"), prob_24h_ago=v.get("prob_24h_ago"), change_24h=v.get("change_24h"),
                volume_24h=v.get("volume_24h"), open_interest=v.get("open_interest"), updated=v.get("updated"),
                headline=g.ref(v["headline"]) if v.get("headline") else None)

def sensitivity(g, qid, fid, prob):
    """The measured question -> factor sensitivity (factor_betas.py), or None if not measured.
    beta = factor move per +1.0 of the question's probability (shrunk estimate); move_if_yes /
    move_if_no = the move if the question resolved now from probability `prob`."""
    b = g.betas.get((qid, fid))
    if not b: return None
    beta, r = b["shrunk_beta"], lambda x: None if x is None else round(x, 4)
    return dict(units=b["units"], beta=r(beta), se=r(b["shrunk_se"]), per_10pts=r(beta / 10),
                move_if_yes=r(beta * (1 - prob)) if prob is not None else None,
                move_if_no=r(-beta * prob) if prob is not None else None,
                n_events=b["n_events"], own_beta=r(b["beta"]), own_se=r(b["se"]), channel=b["channel"],
                orientation=b["orientation"], pooled_beta=r(b["pooled_beta"]), pooled_se=r(b["pooled_se"]),
                tau=r(b["tau"]), events=b["events"], computed_at=b["computed_at"])

def factor_view(g, qid, prob):
    """factors.links for a question, each with its latest level and measured sensitivity."""
    return [dict(f, level=(g.levels.get(f["id"]) or (None, None))[1], level_date=(g.levels.get(f["id"]) or (None, None))[0],
                 sensitivity=sensitivity(g, qid, f["id"], prob)) for f in factors.links(g, qid)] if qid else []

def ladder_coherence(g, qid, pp):
    kind = g.nodes[qid]["attrs"].get("kind")
    if kind not in ("deadlines", "thresholds"): return None
    return ladders.coherence(kind, g.rungs(qid), pp, datetime.now(timezone.utc).date(), ladders.survival(g.nodes[qid]["name"]))

SORTS = {"move": lambda r: -abs(r["change_24h"] or 0), "prob": lambda r: -(r["prob"] or 0),
         "name": lambda r: r["name"]}

# ── endpoints ─────────────────────────────────────────────────────────────────
@app.get("/health")
def health(cur=Depends(db)):
    last = cur.execute(f"SELECT {ISO.format('max(finished_at)')} FROM collection_runs WHERE status != 'failed'").fetchone()[0]
    return dict(status="ok", last_successful_run=last)

@app.get("/questions")
def list_questions(q: str | None = Query(None, description="Substring of the question name"),
                   entity: str | None = Query(None, description="Only questions involving this entity (id or name, e.g. Iran)"),
                   factor: str | None = Query(None, description="Only questions linked to this factor (id, e.g. brent)"),
                   industry: str | None = Query(None, description="Only questions reaching this GICS industry via its niche factors (6-digit code or gics:… id)"),
                   sort: str = Query("move", pattern="^(move|prob|name)$"),
                   limit: int = Query(50, le=500), include_closed: bool = IncludeClosed,
                   cur=Depends(db)):
    """Questions with their headline probability (most-traded live rung) and its 24h change.
    sort=move ranks by |24h change|."""
    at = datetime.now(timezone.utc)
    g = graph(cur)
    if entity:
        entity = g.entity(entity) or g.get(entity, "entity")["id"]  # unknown -> 404
    factor_node = f"factor:{factor.removeprefix('factor:')}" if factor else None
    if factor_node and factor_node not in g.nodes:
        raise HTTPException(404, f"factor {factor} not found")
    # industry filter: questions linked to niche factors that drive the industry, or curated affects edges
    industry_qids = None
    if industry:
        ind_code = industry.removeprefix("gics:")
        nid = f"gics:{ind_code if len(ind_code) != 8 else ind_code[:6]}"
        if nid not in g.nodes or g.nodes[nid]["type"] != "industry":
            raise HTTPException(404, f"industry {industry} not found")
        industry_qids = set()
        for d in g.inc[nid]:
            if d["type"] == "driver_of":
                for e in g.inc[d["src"]]:
                    if e["type"] == "relevant_to": industry_qids.add(e["src"])
            elif d["type"] == "affects":
                industry_qids.add(d["src"])
    qids = [i for i, n in g.nodes.items() if n["type"] == "question"
            and (not q or q.lower() in n["name"].lower())
            and (not entity or any(e["dst"] == entity for e in g.out[i] if e["type"] == "involves"))
            and (not factor_node or any(e["dst"] == factor_node for e in g.out[i] if e["type"] == "relevant_to"))
            and (industry_qids is None or i in industry_qids)]
    qp = question_probs(cur, g, qids, at)
    rows = [question_summary(g, x, qp) for x in qids if include_closed or qp[x]["status"] == "live"]
    return dict(as_of=at, count=len(rows), questions=sorted(rows, key=SORTS[sort])[:limit])

def entity_links(g, qid):
    return [dict(**g.ref(e["dst"]), matched=(e["attrs"] or {}).get("matched"), confidence=e["confidence"],
                 role=(e["attrs"] or {}).get("role"), channel=(e["attrs"] or {}).get("channel"),
                 weight=e["weight"], sign=e["sign"])
            for e in g.out[qid] if e["type"] == "involves"] if qid else []

@app.get("/questions/{question_id}")
def get_question(question_id: str, cur=Depends(db)):
    """One question: headline, every rung (deadline / threshold, probability, 24h change, status,
    volume), latest deadline first (lowest threshold first within a deadline), the entities
    it involves, the factors it is relevant to (factors.py) with their measured sensitivity, and
    `coherence`: rungs whose prices break the ladder's logic (P(by later) >= P(by earlier);
    P(> higher) <= P(> lower)) by more than 2 points — a wrong grouping or a mispricing."""
    at = datetime.now(timezone.utc)
    g = graph(cur)
    g.get(question_id, "question")
    qp = question_probs(cur, g, [question_id], at)
    pp = qp[question_id].pop("rung_probs")
    level = lambda t: (t[1][0] if isinstance(t[1], list) else t[1]) if t else 0   # ["between", [lo, hi]] -> lo
    # latest deadline first; within one deadline (threshold questions), lowest threshold first
    by_threshold = sorted(g.rungs(question_id), key=lambda r: level(r[2]))
    ordered = sorted(by_threshold, key=lambda r: (r[1] or [None, ""])[1] or "", reverse=True)
    rungs = [dict(proposition_summary(g, p, pp), window=w, threshold=t,
                  n_markets=g.nodes[p]["attrs"].get("n_markets"))
             for p, w, t in ordered]
    prob = qp[question_id]["prob"]
    secs = sorted(((t, b) for (q, t), b in g.sec_betas.items() if q == question_id and f"sec:{t}" in g.nodes),
                  key=lambda tb: -abs(tb[1]["shrunk"]))[:15]
    return dict(as_of=at, **question_summary(g, question_id, qp), rungs=rungs, coherence=ladder_coherence(g, question_id, pp),
                entities=entity_links(g, question_id), factors=factor_view(g, question_id, prob),
                industries=question_industries(g, question_id),
                affects=[dict(**g.ref(e["dst"]), sign=e["sign"], rationale=e["note"]) for e in g.out[question_id] if e["type"] == "affects"],
                securities=[dict(**g.ref(f"sec:{t}"), ticker=t, issuer=issuer_of(g, f"sec:{t}"), sensitivity=sec_sensitivity(b, prob))
                            for t, b in secs])

@app.get("/questions/{question_id}/curve")
def question_curve(question_id: str, date_: date | None = Query(None, alias="date", description="Also return P(event by this date), e.g. a bond's maturity"),
                   extrapolate: bool = Query(False, description="Past the last deadline, extend the last period's hazard"),
                   cur=Depends(db)):
    """A deadline question as a term structure: per live deadline after today, the blended
    probability, a monotone fit (isotonic regression), the probability in each period, the same
    conditional on the event not having happened before (per-period hazard, like a default
    probability) and the annualized hazard rate. `prob_by` = P(event by `date`), with the hazard
    constant within each period. `coherence` lists rungs that break P(by later) >= P(by earlier).
    `event` = occurs, or `ends` for questions about a state persisting ("ceasefire continues?"):
    their curve is of the state ending (1 - price), so the hazard is the rate it breaks."""
    at = datetime.now(timezone.utc)
    g = graph(cur)
    g.get(question_id, "question")
    if g.nodes[question_id]["attrs"].get("kind") != "deadlines":
        raise HTTPException(422, "only deadline questions (kind=deadlines) have a curve")
    pp = question_probs(cur, g, [question_id], at)[question_id]["rung_probs"]
    today, invert = at.date(), ladders.survival(g.nodes[question_id]["name"])
    rows = ladders.curve(ladders.deadline_points(g.rungs(question_id), pp, today, invert), today)
    return dict(as_of=at, question=g.ref(question_id), event="ends" if invert else "occurs",
                coherence=ladders.coherence("deadlines", g.rungs(question_id), pp, today, invert),
                points=[dict(r, rungs=[g.ref(p) for p in r["rungs"]]) for r in rows],
                prob_by=None if date_ is None else dict(date=date_, prob=ladders.prob_by(rows, today, date_, extrapolate),
                                                         extrapolated=bool(rows) and date_ > rows[-1]["deadline"]))

@app.get("/questions/{question_id}/contracts")
def question_contracts(question_id: str, include_closed: bool = IncludeClosed, cur=Depends(db)):
    """Every venue contract behind a question's rungs (latest deadline first, as in
    /questions/{id}), with price, liquidity and rules. Live rungs only unless include_closed."""
    at = datetime.now(timezone.utc)
    g = graph(cur)
    g.get(question_id, "question")
    ordered = sorted(g.rungs(question_id), key=lambda r: (r[1] or [None, ""])[1] or "", reverse=True)
    pids = [p for p, _, _ in ordered]
    if not include_closed:
        pp = prop_probs(cur, g, pids, at)
        pids = [p for p in pids if pp[p]["status"] == "live"]
    return dict(as_of=at, question=g.ref(question_id), contracts=contract_rows(cur, g, pids, at))

@app.get("/propositions")
def list_propositions(q: str | None = Query(None, description="Substring of the proposition name"),
                      entity: str | None = Query(None, description="Only propositions involving this entity (id or name)"),
                      sort: str = Query("move", pattern="^(move|prob|name)$"),
                      limit: int = Query(50, le=500), include_closed: bool = IncludeClosed,
                      cur=Depends(db)):
    """Propositions with blended probability and 24h change. sort=move ranks by |24h change|."""
    at = datetime.now(timezone.utc)
    g = graph(cur)
    if entity:
        entity = g.entity(entity) or g.get(entity, "entity")["id"]  # unknown -> 404
    pids = [i for i, n in g.nodes.items() if n["type"] == "proposition"
            and (not q or q.lower() in n["name"].lower())
            and (not entity or any(e["dst"] == entity for e in g.out[g.question_of(i)] if e["type"] == "involves"))]
    probs = prop_probs(cur, g, pids, at)
    rows = [proposition_summary(g, p, probs) for p in pids if include_closed or probs[p]["prob"] is not None]
    return dict(as_of=at, count=len(rows), propositions=sorted(rows, key=SORTS[sort])[:limit])

@app.get("/propositions/{proposition_id}")
def get_proposition(proposition_id: str, cur=Depends(db)):
    """Proposition (one rung of a question) with its markets, its question, the question's
    entities and factors, and logical relations."""
    at = datetime.now(timezone.utc)
    g = graph(cur)
    n = g.get(proposition_id, "proposition")
    logical = [dict(relation=e["type"], direction="out", proposition=g.ref(e["dst"]), note=e["note"],
                    provenance=e["provenance"], **(e["attrs"] or {}))
               for e in g.out[proposition_id] if e["type"] in LOGICAL]
    logical += [dict(relation=e["type"], direction="in", proposition=g.ref(e["src"]), note=e["note"],
                     provenance=e["provenance"], **(e["attrs"] or {}))
                for e in g.inc[proposition_id] if e["type"] in LOGICAL]
    return dict(
        as_of=at,
        **proposition_summary(g, proposition_id, prop_probs(cur, g, [proposition_id], at)),
        valid_from=n["valid_from"],
        question=g.ref(g.question_of(proposition_id)) if g.question_of(proposition_id) else None,
        markets=[{k: v for k, v in r.items() if k != "rung"} for r in contract_rows(cur, g, [proposition_id], at)],
        entities=entity_links(g, g.question_of(proposition_id)),
        factors=factor_view(g, g.question_of(proposition_id),
                            question_probs(cur, g, [g.question_of(proposition_id)], at)[g.question_of(proposition_id)]["prob"]
                            if g.question_of(proposition_id) else None),
        logical=logical)

@app.get("/propositions/{proposition_id}/history")
def proposition_history(proposition_id: str, start: datetime | None = None, end: datetime | None = None,
                        interval: str = Query("1h", pattern="^(1h|1d)$"), cur=Depends(db)):
    """Blended probability series (default: last 30 days, hourly). Each point uses the markets
    observed in that bucket; `n_markets` says how many. Uses the proposition's current markets
    for the whole series."""
    end = end or datetime.now(timezone.utc)
    start = start or end - timedelta(days=30)
    g = graph(cur)
    g.get(proposition_id, "proposition")
    ids = [m for m, rel, _ in g.markets(proposition_id) if rel in BLENDED]
    rows = cur.execute(f"""
        SELECT {ISO.format(f"time_bucket(INTERVAL '{INTERVALS[interval]}', ts)")} AS t,
               'mkt:' || venue || ':' || market_id, arg_max(prob, ts), arg_max(open_interest, ts)
        FROM v_market_prob
        WHERE ts BETWEEN ? AND ? AND prob IS NOT NULL
          AND 'mkt:' || venue || ':' || market_id IN (SELECT unnest(?::VARCHAR[]))
        GROUP BY 1, 2 ORDER BY 1""", [start, end, ids]).fetchall()
    buckets = defaultdict(list)
    for t, _, p, oi in rows:
        buckets[t].append(dict(prob=p, open_interest=oi))
    return dict(id=proposition_id, start=start, end=end, interval=interval,
                points=[dict(t=t, prob=blend(v), n_markets=len(v)) for t, v in sorted(buckets.items())])

def neighbourhood(g, root, depth, fanout, rank=None):
    """Nodes within `depth` hops of `root` (edges in both directions; markets only as the root's own
    neighbours: a proposition's venue markets, or a selected market's proposition). Each node
    keeps its `fanout` top links *per kind of neighbour* (questions, propositions, entities,
    issuers; at most MUTEX_SHOWN mutex), so a hub's 600 questions don't crowd out its links
    to other entities. Links are ranked by `rank(edge, other)` (smaller first; default: strongest
    link). Returns (seen, edges, totals): totals = {kind: links available} for the root."""
    rank = rank or (lambda e, other: -(e["weight"] or 0))
    seen, frontier, edges, totals = {root: 0}, deque([root]), {}, {}
    while frontier:
        n = frontier.popleft()
        if seen[n] == depth: continue
        by_kind = defaultdict(list)
        for e in sorted(g.out[n] + g.inc[n], key=lambda e: rank(e, e["dst"] if e["src"] == n else e["src"])):
            other = e["dst"] if e["src"] == n else e["src"]
            if g.nodes[other]["type"] != "market" or n == root:
                by_kind[g.nodes[other]["type"]].append((e, other))
        if n == root:
            totals = {k: len(v) for k, v in by_kind.items()}
        for links in by_kind.values():
            mutex = [e for e, _ in links if e["type"] == "mutex"][:MUTEX_SHOWN]  # mutex sets are cliques
            for e, other in [(e, o) for e, o in links if e["type"] != "mutex" or e in mutex][:fanout]:
                edges[(e["src"], e["type"], e["dst"])] = e
                if other not in seen:
                    seen[other] = seen[n] + 1; frontier.append(other)
    return seen, [e for e in edges.values() if e["src"] in seen and e["dst"] in seen], totals

@app.get("/graph/{node_id}")
def node_graph(node_id: str, depth: int = Query(1, ge=1, le=3), fanout: int = Query(30, ge=1, le=200),
               cur=Depends(db)):
    """Neighbourhood of any question, proposition, market, entity, issuer, factor
    (`factor:<id>`, macro or niche) or GICS industry (`gics:<code>`), for visualisation. Questions are picked live first, then by 24h trading
    volume; other links by strength. `totals` = how many links of each kind the root has (the
    graph shows at most `fanout`)."""
    at = datetime.now(timezone.utc)
    g = graph(cur)
    if node_id not in g.nodes: raise HTTPException(404, f"{node_id} not found")
    qp = {}
    def rank(e, other):
        if g.nodes[other]["type"] != "question": return (0, 0, -(e["weight"] or 0))
        if other not in qp: qp.update(question_probs(cur, g, [other], at))
        v = qp[other]
        return (v["status"] != "live", -(v["volume_24h"] or 0), -(e["weight"] or 0))
    seen, edges, totals = neighbourhood(g, node_id, depth, fanout, rank)
    probs = prop_probs(cur, g, [i for i in seen if g.nodes[i]["type"] == "proposition"], at)
    probs |= question_probs(cur, g, [i for i in seen if g.nodes[i]["type"] == "question"], at)
    return dict(as_of=at, root=node_id, totals=totals,
                nodes=[g.ref(i) | dict(depth=d, entity_type=g.nodes[i]["attrs"].get("entity_type"),
                                       venue=g.nodes[i]["attrs"].get("venue"),
                                       family=g.nodes[i]["attrs"].get("family"), layer=g.nodes[i]["attrs"].get("layer"),
                                       **{k: probs[i][k] for k in ("prob", "change_24h", "status")} if i in probs else {})
                       for i, d in seen.items()],
                edges=[dict(src=e["src"], dst=e["dst"], type=e["type"], weight=e["weight"], sign=e["sign"],
                            confidence=e["confidence"], provenance=e["provenance"], note=e["note"])
                       for e in edges])

@app.get("/propositions/{proposition_id}/graph")
def proposition_graph(proposition_id: str, depth: int = Query(2, ge=1, le=3), fanout: int = Query(30, ge=1, le=200),
                      cur=Depends(db)):
    """Neighbourhood of a proposition up to `depth` hops (edges followed in both directions)."""
    graph(cur).get(proposition_id, "proposition")
    return node_graph(proposition_id, depth, fanout, cur)

def market_row(g, mid):
    n = g.nodes[mid]
    a = n["attrs"]
    return dict(**g.ref(mid), venue=a.get("venue"), match=a.get("match"), issues=a.get("match_issues") or [],
                proposition=next((e["dst"] for e in g.out[mid] if e["type"] == "same_as"), None),
                close_time=a.get("close_time"))

@app.get("/markets")
def list_markets(status: list[str] = Query(["unmatched", "unparsed"], description="merged | linked | unmatched | unparsed"),
                 q: str | None = Query(None, description="Substring of the market name"),
                 venue: str | None = Query(None, pattern="^(kalshi|polymarket)$"),
                 limit: int = Query(200, le=2000), include_closed: bool = IncludeClosed,
                 cur=Depends(db)):
    """Venue markets by how they fared in cross-venue matching: `merged` (shares a proposition with
    another market), `linked` (near_equivalent / implies only), `unmatched` (parsed, no partner),
    `unparsed` (the contract parser couldn't read it; `issues` says why). Counts cover all statuses."""
    at = datetime.now(timezone.utc)
    g = graph(cur)
    mids = [i for i, n in g.nodes.items() if n["type"] == "market"
            and (not venue or n["attrs"].get("venue") == venue)
            and (not q or q.lower() in n["name"].lower())]
    live = market_probs(cur, mids, at, g)
    mids = [i for i in mids if include_closed or i in live]
    match = lambda i: g.nodes[i]["attrs"].get("match") or "unmatched"   # graphs built before match status
    counts = {s: sum(1 for i in mids if match(i) == s) for s in MATCH_STATUSES}
    rows = [dict(market_row(g, i), prob=(live.get(i) or {}).get("prob")) for i in mids if match(i) in status]
    rows.sort(key=lambda r: (MATCH_STATUSES.index(r["match"]) if r["match"] in MATCH_STATUSES else 9, r["name"]))
    return dict(as_of=at, counts=counts, count=len(rows), markets=rows[:limit])

@app.get("/markets/{market_id}")
def get_market(market_id: str, cur=Depends(db)):
    """One market (venue contract): its terms (title, rules, resolution sources, outcomes; latest
    version), price and liquidity, match status, parsed contract signature, parser issues and the
    nearest rejected candidates (with the reason each was rejected)."""
    at = datetime.now(timezone.utc)
    g = graph(cur)
    n = g.get(market_id, "market")
    a = n["attrs"]
    cands = [dict(c, name=g.nodes[c["market"]]["name"] if c["market"] in g.nodes else None,
                  venue=c["market"].split(":")[1]) for c in a.get("candidates") or []]
    terms = cur.execute("""
        SELECT title, subtitle, event_title, rules_primary, rules_secondary,
               resolution_sources::VARCHAR, outcomes::VARCHAR
        FROM market_versions WHERE venue = ? AND market_id = ?
        ORDER BY version_no DESC LIMIT 1""", [a.get("venue"), a.get("market_id")]).fetchone()
    t = dict(zip(("title", "subtitle", "event_title", "rules", "rules_secondary", "resolution_sources", "outcomes"),
                 terms or [None] * 7))
    for k in ("resolution_sources", "outcomes"):
        t[k] = json.loads(t[k]) if t[k] else None
    t["rules"] = t["rules"] or a.get("rules")
    px = market_probs(cur, [market_id], at, g).get(market_id) or {}
    try:
        res = cur.execute(f"""SELECT arg_max(result, observed_at), arg_max(settlement_value, observed_at),
                                     {ISO.format('arg_max(resolved_at, observed_at)')}
                              FROM market_resolutions WHERE venue = ? AND market_id = ?""", [a.get("venue"), a.get("market_id")]).fetchone()
    except duckdb.Error:
        res = None
    t["resolution"] = dict(result=res[0], settlement_value=res[1], resolved_at=res[2]) if res and res[0] else None
    return dict(as_of=at, **market_row(g, market_id), **t,
                prob=px.get("prob"), volume_24h=px.get("volume_24h"), open_interest=px.get("open_interest"),
                updated=px.get("observed_at"), status="live" if px.get("prob") is not None else "closed",
                signature=a.get("signature"), candidates=cands)

ISSUER_FIELDS = ("qid", "country", "sector", "sub_industry", "headquarters", "lei", "index")
SECURITY_FIELDS = ("ticker", "asset_class", "country", "index", "primary")

def issuer_of(g, sid):
    return next((g.ref(e["src"]) for e in g.inc[sid] if e["type"] == "issued"), None)

def securities_of(g, iid):
    return [dict(**g.ref(e["dst"]), **{k: g.nodes[e["dst"]]["attrs"].get(k) for k in SECURITY_FIELDS})
            for e in g.out[iid] if e["type"] == "issued"]

def sec_sensitivity(b, prob):
    """A measured question -> security sensitivity (security_betas.py): % move per +1.0 of
    probability (shrunk), per 10 points, and the move if the question resolved now."""
    r = lambda x: None if x is None else round(x, 4)
    x = b["shrunk"]
    return dict(units="%", beta=r(x), se=r(b["shrunk_se"]), per_10pts=r(x / 10),
                move_if_yes=r(x * (1 - prob)) if prob is not None else None, move_if_no=r(-x * prob) if prob is not None else None,
                implied=r(b["implied"]), implied_factors=b["implied_factors"], direct=r(b["beta"]), direct_se=r(b["se"]),
                n_events=b["n_events"], found_by=b["source"], events=b["events"])

@app.get("/securities")
def list_securities(q: str | None = Query(None, description="Substring of the ticker or name"), cur=Depends(db)):
    """Securities (securities.csv) with their issuer (none for funds), and how many questions have a
    measured sensitivity for each (security_betas.py)."""
    g = graph(cur)
    n = defaultdict(int)
    for (_, t) in g.sec_betas: n[t] += 1
    return [dict(**g.ref(i), **{k: x["attrs"].get(k) for k in SECURITY_FIELDS}, issuer=issuer_of(g, i), n_questions=n[x["attrs"]["ticker"]])
            for i, x in g.nodes.items() if x["type"] == "security"
            and (not q or q.lower() in x["name"].lower() or q.lower() in x["attrs"]["ticker"].lower())]

@app.get("/securities/{ticker}")
def get_security(ticker: str, cur=Depends(db)):
    """One security: its issuer and its factor loadings (weekly % move per unit factor move,
    controlling for the market; |t| >= 3 first), from security_betas.py: beta, se, 95% CI, t,
    p (two-sided, normal), q (Benjamini-Hochberg across the run), weeks; `spec` and `run`."""
    g = graph(cur)
    sid = f"sec:{ticker}"
    x = g.get(sid, "security")
    loads = [loading_row(f, l) for f, l in (g.loadings.get(ticker) or {}).items() if f in factors.CATALOG]
    loads.sort(key=lambda l: (not l["significant"], -abs(l["t"] or 0)))
    return dict(**g.ref(sid), **{k: x["attrs"].get(k) for k in SECURITY_FIELDS}, issuer=issuer_of(g, sid), loadings=loads,
                spec=STORED_SPEC, run=g.loadings_run)

@app.get("/issuers")
def list_issuers(q: str | None = Query(None, description="Substring of the issuer name"), cur=Depends(db)):
    """Issuers (issuers.csv: companies, and any borrower added by hand) with how many entities
    they are exposed to. The graph stops at issuers: map them to your own bonds, loans or shares."""
    g = graph(cur)
    return [dict(**g.ref(i), **{k: n["attrs"].get(k) for k in ISSUER_FIELDS},
                 n_exposures=sum(1 for e in g.inc[i] if e["type"] == "exposure"))
            for i, n in g.nodes.items() if n["type"] == "issuer" and (not q or q.lower() in n["name"].lower())]

def get_issuer_id(g, key):
    i = g.issuer(key)
    if not i: raise HTTPException(404, f"issuer {key} not found")
    return i

@app.get("/issuers/{issuer_id}")
def get_issuer(issuer_id: str, cur=Depends(db)):
    """One issuer — by id (iss:Q713418, Q713418, local:…) or exact name — with its reference data,
    its GICS `industry` (and sub-industry), the niche factors that drive its sub-industry's
    earnings (`drivers`: side, earnings sign, the macro factors behind each), `industry_questions`
    (questions relevant to one of those drivers or the macro factors behind them, live first; `via`)
    and the entities it is exposed to (weight, rationale, per-channel signs)."""
    g = graph(cur)
    i = get_issuer_id(g, issuer_id)
    n = g.nodes[i]
    ind = issuer_industry(g, i)
    fids = {f for d in ind["drivers"] for f in [d["factor"]["id"], *(x["factor"]["id"] for x in d["driven_by"])]}
    return dict(**g.ref(i), **{k: n["attrs"].get(k) for k in ISSUER_FIELDS}, securities=securities_of(g, i), **ind,
                industry_questions=questions_via(cur, g, fids, datetime.now(timezone.utc), limit=25,
                                                 direct=[i] + ([ind["industry"]["id"]] if ind["industry"] else [])),
                exposed_to=sorted((dict(entity=g.ref(e["src"]), weight=e["weight"], rationale=e["note"],
                                        provenance=e["provenance"], channel_signs=(e["attrs"] or {}).get("channel_signs") or {})
                                   for e in g.inc[i] if e["type"] == "exposure"), key=lambda x: -(x["weight"] or 0)))

def live_questions(cur, g, at):
    """{question id: live}: a question is live when any of its rungs has a market priced in the
    last 7 days."""
    priced = market_probs(cur, [i for i, n in g.nodes.items() if n["type"] == "market"], at, g)
    return {q: any(m in priced for p, _, _ in g.rungs(q) for m, _, _ in g.markets(p))
            for q, n in g.nodes.items() if n["type"] == "question"}

@app.get("/entities")
def list_entities(type: str | None = Query(None, description="person | country | organisation_or_place | concept"),
                  cur=Depends(db)):
    at = datetime.now(timezone.utc)
    g = graph(cur)
    live = live_questions(cur, g, at)
    return [dict(**g.ref(i), entity_type=n["attrs"].get("entity_type"), description=n["attrs"].get("description"),
                 n_questions=sum(1 for e in g.inc[i] if e["type"] == "involves"),
                 n_live_questions=sum(1 for e in g.inc[i] if e["type"] == "involves" and live.get(e["src"])))
            for i, n in g.nodes.items() if n["type"] == "entity" and (not type or n["attrs"].get("entity_type") == type)]

@app.get("/entities/{entity_id}")
def get_entity(entity_id: str, cur=Depends(db)):
    """Entity (id, or a name such as Iran) with every question that involves it (live first, then
    by 24h trading volume), its links, and issuers exposed to it."""
    at = datetime.now(timezone.utc)
    g = graph(cur)
    entity_id = g.entity(entity_id) or entity_id
    n = g.get(entity_id, "entity")
    qids = [e["src"] for e in g.inc[entity_id] if e["type"] == "involves"]
    qp = question_probs(cur, g, qids, at)
    return dict(as_of=at, **g.ref(entity_id), **n["attrs"],
                questions=sorted((question_summary(g, x, qp) for x in qids),
                                 key=lambda r: (r["status"] != "live", -(r["volume_24h"] or 0), r["name"])),
                links=sorted((dict(relation=e["type"], entity=g.ref(other), weight=e["weight"],
                                   provenance=e["provenance"]) for e, other in g.linked(entity_id)),
                             key=lambda l: -(l["weight"] or 0)),
                exposures=[dict(issuer=g.ref(e["dst"]), weight=e["weight"], rationale=e["note"])
                           for e in g.out[entity_id] if e["type"] == "exposure"])

def factor_links(g, factor_id=None):
    """{factor id: {question id: paths}} over every question (or for one factor)."""
    out = defaultdict(dict)
    for q, n in g.nodes.items():
        if n["type"] == "question":
            for f in factors.links(g, q):
                if not factor_id or f["id"] == factor_id:
                    out[f["id"]][q] = f["paths"]
    return out

@app.get("/factors")
def list_factors(family: str | None = Query(None, description="macro: country_equity | fx | commodity | rates | credit | industry | inflation | growth | labour | consumer | housing | trade | fiscal | volatility; niche: revenue | cost | investment"),
                 layer: str | None = Query(None, description="macro | niche"), cur=Depends(db)):
    """The factor catalog — macro (seeds/factors.csv: series, units) and niche (seeds/niche_factors.csv:
    side, earnings_sign, GICS codes) — with its latest level, how many questions are relevant to
    each directly, how many of those links have a measured sensitivity, and how many factors it
    drives / is driven by and industries it reaches."""
    at = datetime.now(timezone.utc)
    g = graph(cur)
    live, hits = live_questions(cur, g, at), factor_links(g)
    return [dict(f, level=(g.levels.get(f["id"]) or (None, None))[1], level_date=(g.levels.get(f["id"]) or (None, None))[0],
                 n_questions=len(hits[f["id"]]), n_live_questions=sum(live[q] for q in hits[f["id"]]),
                 n_measured=sum(1 for q in hits[f["id"]] if (g.betas.get((q, f["id"])) or {}).get("n_events")),
                 history=g.level_info.get(f["id"]),
                 n_drives=len(factor_drives(g, f["id"], "out")), n_driven_by=len(factor_drives(g, f["id"], "in")),
                 n_industries=len({x["industry"]["id"] for x in industries_driven(g, f["id"])}))
            for f in factors.CATALOG.values() if (not family or f["family"] == family) and (not layer or f["layer"] == layer)]

@app.get("/factors/{factor_id}")
def get_factor(factor_id: str, limit: int | None = Query(None, ge=0, description="Max questions returned (default all)"),
               cur=Depends(db)):
    """Factor with its latest level, every question relevant to it (live first, then by 24h
    trading volume) with the paths that link each one (question -> entity [-> its country] ->
    factor) and its measured `sensitivity` (factor move per +1.0 probability; move_if_yes /
    move_if_no), and `pooled`: per channel, the move per +1.0 of adverse probability across
    questions (factor_betas.py). Also its transmission: `drives` / `driven_by` (other factors,
    signed), `industries` (GICS industries whose earnings it moves: directly for a niche factor,
    through the niche factors it drives for a macro one; sign = effect on earnings if it rises) and
    `indirect_questions`: questions relevant to a factor that drives this one (`via`), top 50."""
    if factor_id not in factors.CATALOG: raise HTTPException(404, f"factor {factor_id} not found")
    at = datetime.now(timezone.utc)
    g = graph(cur)
    links = factor_links(g, factor_id)[factor_id]
    qp = question_probs(cur, g, list(links), at)
    rows = sorted((dict(question_summary(g, x, qp), paths=links[x], sensitivity=sensitivity(g, x, factor_id, qp[x]["prob"]))
                   for x in links), key=lambda r: (r["status"] != "live", -(r["volume_24h"] or 0), r["name"]))
    pooled = {}
    for (q, f), b in g.betas.items():
        if f == factor_id and b["pooled_beta"] is not None:
            pooled[b["channel"]] = dict(channel=b["channel"], beta_adverse=round(b["pooled_beta"], 4),
                                        se=round(b["pooled_se"], 4), tau=round(b["tau"], 4), units=b["units"])
    level = g.levels.get(factor_id) or (None, None)
    return dict(as_of=at, **factors.CATALOG[factor_id], level=level[1], level_date=level[0],
                pooled=sorted(pooled.values(), key=lambda p: p["channel"] or ""),
                drives=factor_drives(g, factor_id, "out"), driven_by=factor_drives(g, factor_id, "in"),
                industries=industries_driven(g, factor_id),
                indirect_questions=questions_via(cur, g, [x["factor"]["id"] for x in factor_drives(g, factor_id, "in")], at, exclude=links),
                questions=rows[:limit] if limit is not None else rows)

@app.get("/factors/{factor_id}/history")
def factor_history(factor_id: str, start: date | None = None, end: date | None = None, cur=Depends(db)):
    """Daily levels of a factor (default: the last year), as stored by prices.py: index / FX /
    futures closes, or spreads and yields in percent (FRED). Per point: `fetched_at` = when the
    level used was downloaded (the latest vintage), `first_fetched_at`, `n_vintages`, `source`.
    Fetch times are when this project captured the value — mostly a retrospective backfill, not
    evidence of what investors saw at the time. Missing dates are not filled."""
    if factor_id not in factors.CATALOG: raise HTTPException(404, f"factor {factor_id} not found")
    end = end or datetime.now(timezone.utc).date()
    start = start or end - timedelta(days=365)
    try:
        rows = cur.execute(f"""SELECT date::VARCHAR, arg_max(level, fetched_at), {ISO.format('max(fetched_at)')},
                                      {ISO.format('min(fetched_at)')}, count(*), arg_max(source, fetched_at)
                               FROM factor_prices WHERE factor_id = ? AND date BETWEEN ? AND ? GROUP BY 1 ORDER BY 1""",
                           [factor_id, start, end]).fetchall()
    except duckdb.Error:
        rows = []
    f = factors.CATALOG[factor_id]
    return dict(id=factor_id, series=f["series"], units=f["units"], start=start, end=end, evidence="observed" if rows else None,
                note=None if f["series"] else "Definition only: this factor has no observed series in the project.",
                points=[dict(date=d, level=v, fetched_at=fa, first_fetched_at=ff, n_vintages=n, source=src)
                        for d, v, fa, ff, n, src in rows])

# ── transmission: factor -> factor -> GICS industry <- issuer ───────────────
def factor_ref(fid):
    f = factors.CATALOG[fid.removeprefix("factor:")]
    return dict(id=f["id"], name=f["name"], layer=f["layer"], family=f["family"])

def industry_ref(g, nid):
    a = g.nodes[nid]["attrs"]
    return dict(id=nid, name=g.nodes[nid]["name"], code=a.get("code"), sector=a.get("sector"))

def factor_drives(g, fid, direction):
    """Factors `fid` drives ("out") or is driven by ("in"): [dict(factor, sign, rationale)]."""
    nid, k = f"factor:{fid}", "dst" if direction == "out" else "src"
    es = g.out[nid] if direction == "out" else g.inc[nid]
    return sorted((dict(factor=factor_ref(e[k]), sign=e["sign"], rationale=e["note"])
                   for e in es if e["type"] == "drives" and e[k] in g.nodes),
                  key=lambda x: (x["factor"]["layer"], x["factor"]["name"]))

def industries_driven(g, fid):
    """GICS industries a factor moves: a niche factor through its `driver_of` edges, a macro factor
    through the niche factors it drives (one hop). [dict(industry, sign, side, sub_industries, via)]:
    sign = effect on the industry's earnings if the factor rises (drives sign x earnings sign);
    sub_industries = the GICS sub-industry codes it applies to (None = the whole industry)."""
    nid, out = f"factor:{fid}", []
    def add(e, sign, via):
        a = e["attrs"] or {}
        out.append(dict(industry=industry_ref(g, e["dst"]), sign=sign, side=a.get("side"),
                        sub_industries=a.get("sub_industries"), via=via))
    for e in g.out[nid]:
        if e["type"] == "driver_of": add(e, e["sign"], None)
        elif e["type"] == "drives" and g.nodes.get(e["dst"], {}).get("attrs", {}).get("layer") == "niche":
            for d in g.out[e["dst"]]:
                if d["type"] == "driver_of": add(d, (e["sign"] or 0) * (d["sign"] or 0), factor_ref(e["dst"]))
    return sorted(out, key=lambda x: (x["industry"]["code"], (x["via"] or {}).get("name") or ""))

def question_industries(g, qid, limit=20):
    """GICS industries a question reaches through its factors: question -> factor [-> niche factor]
    -> industry. Per industry the paths; `if_yes` = the earnings direction if the question resolves
    YES, where the question -> factor sensitivity has been measured (sign of the shrunk beta)."""
    by = defaultdict(list)
    for f in factors.links(g, qid):
        b = (g.betas.get((qid, f["id"])) or {}).get("shrunk_beta")
        for x in industries_driven(g, f["id"]):
            by[x["industry"]["id"]].append(dict(factor=factor_ref(f["id"]), via=x["via"], side=x["side"], sign=x["sign"],
                                                sub_industries=x["sub_industries"],
                                                if_yes=(1 if b * x["sign"] > 0 else -1) if b and x["sign"] else None))
    rows = [dict(industry=industry_ref(g, i), n_paths=len(ps), paths=ps) for i, ps in by.items()]
    return sorted(rows, key=lambda r: (-r["n_paths"], r["industry"]["code"]))[:limit]

CURATED_VIA = dict(id=None, name="analyst assessment", layer=None, family=None)

def questions_via(cur, g, fids, at, exclude=(), limit=50, direct=()):
    """Live-first question summaries for questions relevant to any of `fids` (bare factor ids) or with
    a curated `affects` edge to a node in `direct`, each with `via` = the factors that connect it
    (CURATED_VIA for an affects edge, with its `sign` and `rationale`); questions in `exclude` are
    left out."""
    via, hit = defaultdict(set), {}
    for fid in fids:
        for e in g.inc[f"factor:{fid}"]:
            if e["type"] == "relevant_to" and e["src"] not in exclude: via[e["src"]].add(fid)
    for nid in direct:
        for e in g.inc[nid]:
            if e["type"] == "affects" and e["src"] not in exclude:
                via[e["src"]]; hit[e["src"]] = dict(sign=e["sign"], rationale=e["note"])
    qp = question_probs(cur, g, list(via), at)
    rows = sorted((dict(question_summary(g, q, qp), via=[factor_ref(f) for f in sorted(fs)] + ([CURATED_VIA] if q in hit else []),
                        curated=hit.get(q)) for q, fs in via.items()),
                  key=lambda r: (r["status"] != "live", -(r["volume_24h"] or 0), r["name"]))
    return rows[:limit]

def applies(x, sub_code):
    return x["sub_industries"] is None or sub_code in x["sub_industries"]

def issuer_industry(g, iid):
    """An issuer's GICS industry (with sub-industry) and the niche factors driving its sub-industry,
    each with the macro factors that drive it."""
    e = next((e for e in g.out[iid] if e["type"] == "classified_as" and e["dst"] in g.nodes), None)
    if not e: return dict(industry=None, drivers=[])
    sub_code = (e["attrs"] or {}).get("sub_industry_code")
    drivers = [dict(factor=factor_ref(d["src"]), side=(d["attrs"] or {}).get("side"), sign=d["sign"],
                    driven_by=[x for x in factor_drives(g, d["src"][7:], "in") if x["factor"]["layer"] == "macro"])
               for d in g.inc[e["dst"]] if d["type"] == "driver_of"
               and applies(dict(sub_industries=(d["attrs"] or {}).get("sub_industries")), sub_code)]
    return dict(industry=dict(industry_ref(g, e["dst"]), sub_industry=(e["attrs"] or {}).get("sub_industry"),
                              sub_industry_code=sub_code),
                drivers=sorted(drivers, key=lambda d: ({"revenue": 0, "cost": 1}.get(d["side"], 2), d["factor"]["name"])))

@app.get("/industries")
def list_industries(sector: str | None = Query(None, description="GICS sector name, e.g. Energy"), cur=Depends(db)):
    """GICS industries (seeds/gics.csv) with how many issuers are classified in each and how many
    niche factors drive it."""
    g = graph(cur)
    return [dict(industry_ref(g, i), industry_group=n["attrs"].get("industry_group"),
                 n_issuers=sum(1 for e in g.inc[i] if e["type"] == "classified_as"),
                 n_factors=sum(1 for e in g.inc[i] if e["type"] == "driver_of"))
            for i, n in g.nodes.items() if n["type"] == "industry" and (not sector or n["attrs"].get("sector") == sector)]

@app.get("/industries/{code}")
def get_industry(code: str, cur=Depends(db)):
    """One GICS industry (`453010` or `gics:453010`; a sub-industry code gives its industry): its
    sub-industries, issuers, the niche factors driving its earnings (revenue / cost / investment,
    with the macro factors behind each), and the live questions that reach it through those factors
    (directly relevant to one of its niche factors, or to a macro factor driving one), live first,
    then by 24h volume."""
    at = datetime.now(timezone.utc)
    g = graph(cur)
    c = code.removeprefix("gics:")
    nid = f"gics:{c if len(c) != 8 else c[:6]}"
    n = g.get(nid, "industry")
    drivers = [dict(factor=factor_ref(d["src"]), side=(d["attrs"] or {}).get("side"), sign=d["sign"],
                    sub_industries=(d["attrs"] or {}).get("sub_industries"),
                    driven_by=[x for x in factor_drives(g, d["src"][7:], "in")])
               for d in g.inc[nid] if d["type"] == "driver_of"]
    qs = questions_via(cur, g, {f for d in drivers for f in [d["factor"]["id"], *(x["factor"]["id"] for x in d["driven_by"])]},
                       at, limit=None, direct=[nid])
    issuers = [dict(**g.ref(e["src"]), sub_industry=(e["attrs"] or {}).get("sub_industry"))
               for e in g.inc[nid] if e["type"] == "classified_as"]
    return dict(as_of=at, **industry_ref(g, nid), industry_group=n["attrs"].get("industry_group"),
                sub_industries=n["attrs"].get("sub_industries"), issuers=sorted(issuers, key=lambda x: x["name"]),
                drivers=sorted(drivers, key=lambda d: ({"revenue": 0, "cost": 1}.get(d["side"], 2), d["factor"]["name"])),
                questions=qs[:100], n_questions=len(qs))

# ── factor research: factor -> securities, regressions, contract history ─────────────────
# Results carry `evidence`: definition (reference data) · observed (captured prices / levels) ·
# qualitative (rule, transmission or curated link; not measured) · estimated (regression; an
# in-sample association, not a causal impact) · validated (none exist: VALIDATED_NOTE).
VALIDATED_NOTE = ("No validated event-impact estimates exist in this project: backtest.py (2026-10-18) found no "
                  "gradient between structural scores and abnormal returns, and nothing is evaluated out of sample.")
STORED_SPEC = dict(
    id="market_plus_factor", name="Market + one factor (stored)", source="security_betas.py -> security_loadings",
    dependent="weekly log return of the security's adjusted close, x 100 (last close of each ISO week)",
    regressors=["equity_us (S&P 500) weekly move", "the factor's weekly move (% = log return x 100; bp = level change x 100)"],
    estimator="OLS with intercept from pairwise-complete weekly covariances; se from the residual variance",
    window=f"last 156 weeks before the run; at least 52 weeks", out_of_sample="none: in-sample only",
    validation="not validated (no out-of-sample test or backtest of loadings in this project)")
JOINT_SPEC = dict(STORED_SPEC, id="joint", name="Market + selected factors (joint, on request)",
                  source="security_betas.Regressions.fit, computed now from stored prices (not stored)",
                  regressors=["equity_us (S&P 500) weekly move", "each selected factor's weekly move, estimated jointly"])

def loading_row(fid, l):
    r = lambda v: None if v is None else round(v, 4)
    ci = (l["beta"] - 1.96 * l["se"], l["beta"] + 1.96 * l["se"]) if l["beta"] is not None and l["se"] is not None else (None, None)
    return dict(factor=factor_ref(fid) | dict(units=factors.CATALOG[fid]["units"]), beta=r(l["beta"]), se=r(l["se"]),
                ci_low=r(ci[0]), ci_high=r(ci[1]), t=r(l["t"]), p=r(l.get("p")), q=r(l.get("q")), n_weeks=l["n_weeks"],
                significant=bool(l["t"] and abs(l["t"]) >= 3), bh_5pct=bool(l.get("q") is not None and l["q"] < 0.05),
                evidence="estimated")

@app.get("/factors/{factor_id}/securities")
def factor_securities(factor_id: str, min_abs_t: float = Query(0, ge=0), limit: int = Query(200, ge=1, le=1000), cur=Depends(db)):
    """Securities with a measured loading on a factor (latest security_betas run), |t| first: beta
    (weekly % move of the security per unit weekly move of the factor, controlling for the market),
    se, 95% CI, t, p, Benjamini-Hochberg q, weeks. `spec` describes the regression; `run` its vintage.
    Empty `securities` = the factor has no measured loadings (no price series, or not run yet)."""
    if factor_id not in factors.CATALOG: raise HTTPException(404, f"factor {factor_id} not found")
    g = graph(cur)
    rows = []
    for t, fs in g.loadings.items():
        if factor_id in fs and fs[factor_id]["t"] is not None and abs(fs[factor_id]["t"]) >= min_abs_t:
            sid = f"sec:{t}"
            sec = g.ref(sid) | {k: g.nodes[sid]["attrs"].get(k) for k in SECURITY_FIELDS} if sid in g.nodes else dict(id=sid, name=t, ticker=t)
            rows.append(dict(security=sec, issuer=issuer_of(g, sid) if sid in g.nodes else None, **loading_row(factor_id, fs[factor_id])))
    rows.sort(key=lambda x: -abs(x["t"] or 0))
    f = factors.CATALOG[factor_id]
    return dict(factor=factor_ref(factor_id) | dict(units=f["units"], series=f["series"]), spec=STORED_SPEC, run=g.loadings_run,
                measured=bool(f["series"]), count=len(rows), securities=rows[:limit], validated_impacts=VALIDATED_NOTE,
                note=None if f["series"] else "This factor has no price series (definition only), so no security can be regressed on it.")

_weekly_cache = {"at": 0.0, "F": None, "start": None}

@app.get("/securities/{ticker}/regression")
def security_regression(ticker: str, factor: list[str] = Query(..., description="Factor ids, e.g. factor=rates_us_10y&factor=brent"),
                        cur=Depends(db)):
    """A joint regression of a security's weekly returns on the market plus the chosen factors,
    with the project's estimator (security_betas.Regressions: pairwise-complete weekly covariances,
    last 156 weeks), computed now from stored prices and not stored. Coefficients with se, 95% CI,
    t, p; R², adjusted R², residual SD, weeks and the sample window. In-sample statistical
    associations only — not causal event impacts."""
    import security_betas as sb
    bad = [f for f in factor if f not in factors.CATALOG]
    if bad: raise HTTPException(404, f"unknown factor(s): {', '.join(bad)}")
    if _weekly_cache["F"] is None or time.monotonic() - _weekly_cache["at"] > 3600:
        start = (datetime.now(timezone.utc) - timedelta(weeks=sb.LOOKBACK_WEEKS + 1)).date()
        F, _ = sb.load_series(cur, [], start)
        _weekly_cache.update(at=time.monotonic(), F=F, start=start)
    F, start = _weekly_cache["F"], _weekly_cache["start"]
    pts = cur.execute("""SELECT date::VARCHAR, arg_max(coalesce(adj_close, close), fetched_at) FROM security_prices
                         WHERE ticker = ? AND date >= ? GROUP BY 1 ORDER BY 1""", [ticker, start]).fetchall()
    S = sb.weekly([(date.fromisoformat(d), v) for d, v in pts], True)
    regressors = list(dict.fromkeys(["equity_us", *factor]))
    missing = [f for f in regressors if f not in F]
    out = dict(ticker=ticker, spec=JOINT_SPEC, regressors=regressors, evidence="estimated",
               missing=[dict(factor=factor_ref(f), reason="no price series (definition only)" if not factors.CATALOG[f]["series"]
                             else f"fewer than {sb.MIN_WEEKS} weeks of observed levels") for f in missing])
    if len(S) < sb.MIN_WEEKS:
        return out | dict(fit=None, note=f"{ticker}: {len(S)} weeks of prices; at least {sb.MIN_WEEKS} are needed")
    use = [f for f in regressors if f in F]
    reg = sb.Regressions(F, {ticker: S})
    fit = reg.fit(ticker, use)
    if not fit:
        return out | dict(fit=None, note="not estimable (too few overlapping weeks or collinear regressors)")
    ys = {f: reg.fs(f, ticker)[0] for f in use}
    var_s = sb.cov(S, S)[0]
    weeks = sorted(S.keys() & set.intersection(*(set(F[f]) for f in use)))
    n, k = len(weeks), len(use)
    r2 = max(0.0, min(1.0, sum(fit[f][0] * ys[f] for f in use) / var_s)) if var_s else None
    r = lambda v: None if v is None else round(v, 4)
    coefs = [dict(factor=factor_ref(f) | dict(units=factors.CATALOG[f]["units"]), beta=r(b), se=r(se),
                  ci_low=r(b - 1.96 * se), ci_high=r(b + 1.96 * se), t=r(b / se) if se else None,
                  p=r(math.erfc(abs(b / se) / math.sqrt(2))) if se else None,
                  stored=loading_row(f, g_l) if (g_l := (graph(cur).loadings.get(ticker) or {}).get(f)) else None)
             for f, (b, se) in fit.items()]
    return out | dict(fit=dict(coefficients=coefs, r2=r(r2), adj_r2=r(1 - (1 - r2) * (n - 1) / max(n - k - 1, 1)) if r2 is not None else None,
                               resid_sd=r(math.sqrt(var_s * (1 - r2) * n / max(n - k - 1, 1))) if r2 is not None else None,
                               n_weeks=n, window=[str(weeks[0]), str(weeks[-1])] if weeks else None))

@app.get("/markets/{market_id}/history")
def market_history(market_id: str, start: datetime | None = None, end: datetime | None = None,
                   limit: int = Query(5000, ge=1, le=20000), cur=Depends(db)):
    """Captured snapshots of one market (default: the last 30 days), oldest first: ts, price (mid if
    the spread is <= 10c, else last trade), bid, ask, last, volume_24h, open_interest, tier
    (`realtime` / `hourly` / `daily` collector runs; `backfill` = rebuilt from venue history after
    the fact, not captured live). Gaps are real: nothing is interpolated."""
    g = graph(cur)
    a = g.get(market_id, "market")["attrs"]
    end = end or datetime.now(timezone.utc)
    start = start or end - timedelta(days=30)
    rows = cur.execute(f"""
        SELECT {ISO.format('ts')}, CASE WHEN yes_ask - yes_bid BETWEEN 0 AND 0.10 THEN round((yes_bid + yes_ask) / 2, 4) ELSE last END,
               CASE WHEN yes_ask - yes_bid BETWEEN 0 AND 0.10 THEN 'mid' ELSE 'last trade (spread > 10c or no quote)' END,
               yes_bid, yes_ask, last, volume_24h, open_interest, source, status
        FROM market_snapshots WHERE venue = ? AND market_id = ? AND ts BETWEEN ? AND ? ORDER BY ts DESC LIMIT ?""",
        [a["venue"], a["market_id"], start, end, limit]).fetchall()
    cols = ("ts", "price", "price_rule", "yes_bid", "yes_ask", "last", "volume_24h", "open_interest", "tier", "status")
    return dict(id=market_id, venue=a["venue"], native_id=a["market_id"], start=start, end=end, evidence="observed",
                truncated=len(rows) == limit, points=[dict(zip(cols, r)) for r in reversed(rows)])

@app.get("/review/ladders")
def review_ladders(cur=Depends(db)):
    """Live multi-rung questions whose rung prices contradict the ladder (ladders.coherence):
    largest gap first. Each is a wrong grouping (fix with seeds/match_overrides.csv) or a mispricing."""
    at = datetime.now(timezone.utc)
    g = graph(cur)
    qids = [q for q, n in g.nodes.items() if n["type"] == "question" and n["attrs"].get("kind") in ("deadlines", "thresholds")]
    qp = question_probs(cur, g, qids, at)
    rows = []
    for q in qids:
        c = ladder_coherence(g, q, qp[q]["rung_probs"])
        if c and not c["ok"]:
            rows.append(dict(question_summary(g, q, qp), violations=[dict(v, should_be_lower=g.ref(v["should_be_lower"]),
                                                                          should_be_higher=g.ref(v["should_be_higher"]))
                                                                     for v in c["violations"]]))
    rows.sort(key=lambda r: -r["violations"][0]["gap"])
    return dict(as_of=at, count=len(rows), questions=rows)

CALIBRATION_TTL = 3600  # seconds the price-at-horizon table is reused
HORIZONS = (1, 7, 30, 90)  # days before resolution
LIQUIDITY = ((1e5, ">=100k"), (1e4, "10k-100k"), (1e3, "1k-10k"), (0, "<1k"))
_calibration = {"rows": None, "loaded": 0.0}

def calibration_rows(cur):
    """[(venue, market id, outcome 0/1, horizon, price, liquidity)]: each binary resolved market's
    last cleaned price on the day `horizon` days before it resolved (or within the week before),
    with its open interest or 24h volume then (None if the venue's history has neither)."""
    if _calibration["rows"] is None or time.monotonic() - _calibration["loaded"] > CALIBRATION_TTL:
        try:
            rows = cur.execute(f"""
                WITH res AS (SELECT venue, market_id, arg_max(result, observed_at) AS result,
                                    CAST(arg_max(resolved_at, observed_at) AS DATE) AS day
                             FROM market_resolutions GROUP BY ALL HAVING arg_max(result, observed_at) IN ('yes', 'no')),
                daily AS (SELECT venue, market_id, CAST(ts AS DATE) AS day, arg_max(prob, ts) AS prob,
                                 arg_max(greatest(coalesce(open_interest, 0), coalesce(volume_24h, 0)), ts) AS liq,
                                 bool_or(open_interest IS NOT NULL OR volume_24h IS NOT NULL) AS has_liq
                          FROM v_market_prob SEMI JOIN res USING (venue, market_id) WHERE prob IS NOT NULL GROUP BY ALL)
                SELECT r.venue, r.market_id, (r.result = 'yes')::INT, h.horizon,
                       arg_max(d.prob, d.day), arg_max(CASE WHEN d.has_liq THEN d.liq END, d.day)
                FROM res r CROSS JOIN (SELECT unnest({list(HORIZONS)}) AS horizon) h
                JOIN daily d ON d.venue = r.venue AND d.market_id = r.market_id
                 AND d.day <= r.day - h.horizon AND d.day > r.day - h.horizon - 7
                GROUP BY ALL""").fetchall()
        except duckdb.Error:
            rows = []
        _calibration.update(rows=rows, loaded=time.monotonic())
    return _calibration["rows"]

def calibration_summary(rows, bins):
    if not rows: return dict(n=0)
    n = len(rows)
    brier = sum((p - y) ** 2 for *_, y, _, p, _ in rows) / n
    base = sum(y for *_, y, _, _, _ in rows) / n
    ref = base * (1 - base)
    cells = defaultdict(list)
    for *_, y, _, p, _ in rows:
        cells[min(int(p * bins), bins - 1)].append((p, y))
    return dict(n=n, brier=round(brier, 4), base_rate=round(base, 4), brier_skill=round(1 - brier / ref, 4) if ref else None,
                bins=[dict(lo=round(i / bins, 4), hi=round((i + 1) / bins, 4), n=len(c), mean_prob=round(sum(p for p, _ in c) / len(c), 4),
                           frequency=round(sum(y for _, y in c) / len(c), 4)) for i, c in sorted(cells.items())])

@app.get("/calibration")
def calibration(horizon: int = Query(7, description="Days before resolution: 1, 7, 30 or 90"),
                venue: str | None = Query(None, pattern="^(kalshi|polymarket)$"),
                scope: str = Query("graph", pattern="^(graph|all)$", description="graph = markets in the knowledge graph; all = every resolved market collected"),
                bins: int = Query(10, ge=2, le=20), cur=Depends(db)):
    """How often a price came true: binary markets that resolved yes/no (market_resolutions),
    priced `horizon` days before resolution. Per probability bin: mean price vs share that
    resolved YES; Brier score and skill vs always predicting the base rate; the same split by
    venue and by liquidity (open interest or 24h volume then). Only markets the collector saw
    resolve are included, so the sample starts with resolution capture."""
    if horizon not in HORIZONS: raise HTTPException(422, f"horizon must be one of {HORIZONS}")
    g = graph(cur)
    rows = [r for r in calibration_rows(cur) if r[3] == horizon and (not venue or r[0] == venue)
            and (scope == "all" or f"mkt:{r[0]}:{r[1]}" in g.nodes)]
    bucket = lambda liq: "unknown" if liq is None else next(name for lo, name in LIQUIDITY if liq >= lo)
    by = lambda key: {k: calibration_summary([r for r in rows if key(r) == k], bins) for k in sorted({key(r) for r in rows})}
    return dict(as_of=datetime.now(timezone.utc), horizon_days=horizon, venue=venue, scope=scope,
                **calibration_summary(rows, bins), by_venue=by(lambda r: r[0]), by_liquidity=by(lambda r: bucket(r[5])))

def direction(impact):
    return "unknown" if impact is None else "up" if impact > 0 else "down" if impact < 0 else "neutral"

def risk(impact, prob):
    return None if impact is None or prob is None else round(abs(impact) * math.sqrt(prob * (1 - prob)), 4)

def path_out(g, q):
    return dict(strength=round(q["strength"], 4), sign=q["sign"], channel=q["channel"], role=q["role"],
                nodes=[g.ref(x) for x in q["nodes"]])

EXPOSURE_SORTS = {"score": lambda r: -r["score"], "impact": lambda r: -abs(r["impact"] or 0),
                  "risk": lambda r: -(r["risk"] or 0)}

@app.get("/issuers/{issuer_id}/exposures")
def issuer_exposures(issuer_id: str, min_score: float = Query(0.1, ge=0, le=1),
                       sort: str = Query("score", pattern="^(score|impact|risk)$"),
                       limit: int = Query(50, le=500), include_closed: bool = IncludeClosed,
                       cur=Depends(db)):
    """Questions this issuer is exposed to (issuer by id or exact name), summed over every path
    question -> entity [-> entity] -> issuer: `score` (relevance 0-1), `impact` (signed: > 0 =
    expected to benefit if the event happens), `direction`, `risk` (|impact| x sqrt(p(1-p))) and
    the strongest `paths`."""
    at = datetime.now(timezone.utc)
    g = graph(cur)
    sid = get_issuer_id(g, issuer_id)
    hits = {p: v for p, v in g.exposures_of(sid).items() if v["score"] >= min_score}
    qp = question_probs(cur, g, list(hits), at)
    rows = [dict(question_summary(g, p, qp), score=v["score"], impact=v["impact"],
                 direction=direction(v["impact"]), risk=risk(v["impact"], qp[p]["prob"]),
                 path=[g.ref(x) for x in v["paths"][0]["nodes"]],
                 paths=[path_out(g, q) for q in v["paths"][:3]], n_paths=len(v["paths"]))
            for p, v in hits.items() if include_closed or qp[p]["status"] == "live"]
    rows.sort(key=lambda r: (EXPOSURE_SORTS[sort](r), -abs(r["change_24h"] or 0)))
    return dict(as_of=at, issuer=g.ref(sid), count=len(rows), exposures=rows[:limit])

SEC_SORTS = {"risk": lambda r: -(r["risk"] or 0), "move": lambda r: -abs(r["sensitivity"]["beta"] or 0),
             "evidence": lambda r: -(r["sensitivity"]["n_events"] or 0)}

@app.get("/securities/{ticker}/exposures")
def security_exposures(ticker: str, sort: str = Query("risk", pattern="^(risk|move|evidence)$"),
                       min_events: int = Query(0, ge=0, description="Only pairs measured on at least this many jump days"),
                       limit: int = Query(50, le=500), include_closed: bool = IncludeClosed, cur=Depends(db)):
    """Questions this security moves with, measured (security_betas.py): `sensitivity` = % move per
    +1.0 of the question's probability (factor-implied, direct on jump days, shrunk), move_if_yes /
    move_if_no, `found_by` (factor = a significant factor loading; structural = an entity path to
    its issuer; both). risk = |beta| x sqrt(p(1-p)). Structural `path` when there is one."""
    at = datetime.now(timezone.utc)
    g = graph(cur)
    sid = f"sec:{ticker}"
    g.get(sid, "security")
    hits = {q: b for (q, t), b in g.sec_betas.items() if t == ticker and q in g.nodes and b["n_events"] >= min_events}
    qp = question_probs(cur, g, list(hits), at)
    iss = issuer_of(g, sid)
    paths = g.exposures_of(iss["id"]) if iss else {}
    rows = []
    for q, b in hits.items():
        if not include_closed and qp[q]["status"] != "live": continue
        sens = sec_sensitivity(b, qp[q]["prob"])
        rows.append(dict(question_summary(g, q, qp), sensitivity=sens, risk=risk(sens["beta"], qp[q]["prob"]),
                         path=[g.ref(x) for x in paths[q]["paths"][0]["nodes"]] + [g.ref(sid)] if q in paths else None))
    rows.sort(key=lambda r: (SEC_SORTS[sort](r), -abs(r["change_24h"] or 0)))
    return dict(as_of=at, security=g.ref(sid), issuer=iss, count=len(rows), exposures=rows[:limit])

@app.get("/", include_in_schema=False)
def web():
    """Interactive graph explorer (web/index.html)."""
    return FileResponse(Path(__file__).parent / "web" / "index.html")

EXCEL = Path(__file__).parent / "excel"

ADDIN_ID, LOCAL_ADDIN_ID = "eeb823d5-3150-4ee3-a983-57ef26e13f0b", "5c0e7a3e-8f2b-4d61-9a47-1b6e2d0c9f14"

def excel_template(request: Request, name: str) -> str:
    """An excel/ template with {BASE} = this server's origin (Office requires HTTPS; Railway terminates
    TLS), {ID} = the add-in id and {FILE} = the manifest's file name on the user's machine. Served
    from localhost it is a separate add-in, "Event Risk (local)" with functions ERDEV.* instead of ER.*,
    so it can sit beside the deployed one."""
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme).split(",")[0]
    base = f"{scheme}://{request.headers.get('host', request.url.netloc)}"
    local = request.url.hostname in ("localhost", "127.0.0.1")
    text = (EXCEL / name).read_text().replace("{BASE}", base).replace("{ID}", LOCAL_ADDIN_ID if local else ADDIN_ID) \
                                     .replace("{FILE}", "event-risk-local.xml" if local else "event-risk-manifest.xml")
    return text.replace(ADDIN_ID, LOCAL_ADDIN_ID).replace(
        '<DisplayName DefaultValue="Event Risk"/>', '<DisplayName DefaultValue="Event Risk (local)"/>').replace(
        'id="Functions.Namespace" DefaultValue="ER"', 'id="Functions.Namespace" DefaultValue="ERDEV"').replace(
        "type =ER. in a cell", "type =ERDEV. in a cell") if local else text

@app.get("/excel/manifest.xml", include_in_schema=False)
def excel_manifest(request: Request):
    """Excel add-in manifest pointing at this server."""
    return Response(excel_template(request, "manifest.xml"), media_type="application/xml",
                    headers={"Content-Disposition": 'attachment; filename="event-risk-manifest.xml"'})

@app.get("/excel/install.cmd", include_in_schema=False)
@app.get("/excel/uninstall.cmd", include_in_schema=False)
def excel_installer(request: Request):
    """Double-click installer / uninstaller for Windows (no admin rights): fetches the manifest and
    registers it as a developer add-in for the current user. Windows batch files need CRLF."""
    name = request.url.path.rsplit("/", 1)[1]
    text = excel_template(request, name).replace("\r\n", "\n").replace("\n", "\r\n")
    return Response(text, media_type="application/octet-stream",
                    headers={"Content-Disposition": f'attachment; filename="{name.replace(".", "-event-risk.")}"'})

app.mount("/excel", StaticFiles(directory=EXCEL), name="excel")  # functions.js/json, taskpane.html, icons
