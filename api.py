#!/usr/bin/env python3
"""
Knowledge-graph API for geopolitical event risk.

  uvicorn api:app            (docs at /docs)

Auth: set API_KEYS="key1,key2" to require an `X-API-Key` header; unset = open (local dev).

Time travel: every read endpoint takes
  as_of        valid time: the state of the world at this moment (default now)
  known_as_of  transaction time: only what the graph had recorded by then (default now).
               Use both = T for a backtest-safe, point-in-time view.

Market -> proposition -> question. A proposition is one bet (one deadline / threshold), possibly
listed on several venues; its probability = blend of its markets' latest cleaned prices at or
before as_of (within 7 days), weighted by open interest when every market reports it, else
equally. Markets join a proposition only when their contract signatures match (matching.py);
propositions with materially different terms are linked as `near_equivalent` and never blended.
A question groups the propositions that differ only in deadline ("China invades Taiwan by Jun
2026 / Dec 2026 / Dec 2027") or, for one measurement period, in threshold: its rungs. Its
headline probability is its most-traded live rung (24h volume, then open interest). A
proposition or question is `live` when it has a price in the 7 days before as_of, else `closed`.
Entities (and through them securities) attach to questions.

Entities are discovered from market titles via Wikipedia; entity links come from Wikidata statements
and Wikipedia co-occurrence.

Security exposure to a question, summed over every path question -> entity [-> linked
entity] -> security. Each path's strength = involves weight (severity x role) x pass-through
(1, or LINK_DAMPING x link weight for a linked entity) x exposure weight; its sign = what YES
means for the entity x how the security responds on that channel (seeds/channel_signs.csv).
  score   relevance in [0, 1]: noisy-OR of path strengths
  impact  signed structural sensitivity: sum of signed path strengths (> 0: the security is
          expected to rise if the event happens). A prior in graph units, not a measured beta —
          backtest.py measures how it maps to returns.
  risk    |impact| x sqrt(p (1 - p)): how much the event can still move the security now
Weights are analyst priors (seeds/exposures.csv) until empirical event betas exist.
"""
import json, math, os, threading, time
from contextlib import asynccontextmanager
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone

from pathlib import Path

import duckdb
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

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

def times(as_of, known_as_of):
    t = as_of or datetime.now(timezone.utc)
    return t, known_as_of or datetime.now(timezone.utc)

AsOf = Query(None, description="Valid time (ISO 8601). Default: now.")
KnownAsOf = Query(None, description="Transaction time (ISO 8601). Default: now.")
IncludeClosed = Query(False, description="Include closed items: no price in the 7 days before as_of (resolved / delisted). Default: live only.")

# ── graph + prices ────────────────────────────────────────────────────────────
class Graph:
    """The graph at (as_of, known_as_of), loaded once per request."""
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
        self.out, self.inc = defaultdict(list), defaultdict(list)
        for e in self.edges:
            self.out[e["src"]].append(e); self.inc[e["dst"]].append(e)

    def get(self, node_id, node_type):
        n = self.nodes.get(node_id)
        if not n or n["type"] != node_type:
            raise HTTPException(404, f"{node_type} {node_id} not found at this as_of / known_as_of")
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

    def question_of(self, pid):
        return next((e["dst"] for e in self.out[pid] if e["type"] == "rung_of"), None)

    def exposures_of(self, sec):
        """{question_id: dict(score, impact, paths)} for one security, walking back from it over
        every path security <- exposure - entity [<- link -> entity] <- involves - question.
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
    now, known = times(None, None)
    g = Graph(cur, now, known)
    g.latest = dict(at=now, now=all_market_probs(cur, now), ago=all_market_probs(cur, now - timedelta(hours=24)))
    _current.update(graph=g, loaded=time.monotonic())

def _reload_in_background():
    try: _load_current()
    finally: _reloading.release()

def graph(cur, as_of, known_as_of):
    """The graph for a request. Requests about now share one copy, refreshed in the background
    every GRAPH_TTL seconds (callers never wait for a reload); time-travel requests (explicit
    as_of / known_as_of) load their own."""
    if as_of or known_as_of:
        return Graph(cur, *times(as_of, known_as_of))
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
                      open_interest=total([now_.get(m) for m in ms], "open_interest"))
    return out

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
                      volume_24h=sum(pp[p]["volume_24h"] or 0 for p, _ in live) or None)
    return out

def question_summary(g, qid, qp):
    n, v = g.nodes[qid], qp.get(qid, {})
    a = n["attrs"]
    return dict(id=qid, name=n["name"], kind=a.get("kind"), n_rungs=a.get("n_rungs"), n_live=v.get("n_live"),
                status=v.get("status"), venues=a.get("venues"), close_time=a.get("close_time"),
                prob=v.get("prob"), prob_24h_ago=v.get("prob_24h_ago"), change_24h=v.get("change_24h"),
                volume_24h=v.get("volume_24h"), headline=g.ref(v["headline"]) if v.get("headline") else None)

SORTS = {"move": lambda r: -abs(r["change_24h"] or 0), "prob": lambda r: -(r["prob"] or 0),
         "name": lambda r: r["name"]}

# ── endpoints ─────────────────────────────────────────────────────────────────
@app.get("/health")
def health(cur=Depends(db)):
    last = cur.execute(f"SELECT {ISO.format('max(finished_at)')} FROM collection_runs WHERE status != 'failed'").fetchone()[0]
    return dict(status="ok", last_successful_run=last)

@app.get("/questions")
def list_questions(q: str | None = Query(None, description="Substring of the question name"),
                   entity: str | None = Query(None, description="Only questions involving this entity id"),
                   sort: str = Query("move", pattern="^(move|prob|name)$"),
                   limit: int = Query(50, le=500), include_closed: bool = IncludeClosed,
                   as_of: datetime | None = AsOf, known_as_of: datetime | None = KnownAsOf,
                   cur=Depends(db)):
    """Questions with their headline probability (most-traded live rung) and its 24h change.
    sort=move ranks by |24h change|."""
    at, known = times(as_of, known_as_of)
    g = graph(cur, as_of, known_as_of)
    qids = [i for i, n in g.nodes.items() if n["type"] == "question"
            and (not q or q.lower() in n["name"].lower())
            and (not entity or any(e["dst"] == entity for e in g.out[i] if e["type"] == "involves"))]
    qp = question_probs(cur, g, qids, at)
    rows = [question_summary(g, x, qp) for x in qids if include_closed or qp[x]["status"] == "live"]
    return dict(as_of=at, known_as_of=known, count=len(rows), questions=sorted(rows, key=SORTS[sort])[:limit])

def entity_links(g, qid):
    return [dict(**g.ref(e["dst"]), matched=(e["attrs"] or {}).get("matched"), confidence=e["confidence"],
                 role=(e["attrs"] or {}).get("role"), channel=(e["attrs"] or {}).get("channel"),
                 weight=e["weight"], sign=e["sign"])
            for e in g.out[qid] if e["type"] == "involves"] if qid else []

@app.get("/questions/{question_id}")
def get_question(question_id: str, as_of: datetime | None = AsOf, known_as_of: datetime | None = KnownAsOf,
                 cur=Depends(db)):
    """One question: headline, every rung (deadline / threshold, probability, 24h change, status,
    volume) in deadline then threshold order, and the entities it involves."""
    at, known = times(as_of, known_as_of)
    g = graph(cur, as_of, known_as_of)
    g.get(question_id, "question")
    qp = question_probs(cur, g, [question_id], at)
    pp = qp[question_id].pop("rung_probs")
    level = lambda t: (t[1][0] if isinstance(t[1], list) else t[1]) if t else 0   # ["between", [lo, hi]] -> lo
    order = lambda r: ((r[1] or [None, ""])[1] or "", level(r[2]))
    rungs = [dict(proposition_summary(g, p, pp), window=w, threshold=t,
                  n_markets=g.nodes[p]["attrs"].get("n_markets"))
             for p, w, t in sorted(g.rungs(question_id), key=order)]
    return dict(as_of=at, known_as_of=known, **question_summary(g, question_id, qp), rungs=rungs,
                entities=entity_links(g, question_id))

@app.get("/propositions")
def list_propositions(q: str | None = Query(None, description="Substring of the proposition name"),
                      entity: str | None = Query(None, description="Only propositions involving this entity id"),
                      sort: str = Query("move", pattern="^(move|prob|name)$"),
                      limit: int = Query(50, le=500), include_closed: bool = IncludeClosed,
                      as_of: datetime | None = AsOf, known_as_of: datetime | None = KnownAsOf,
                      cur=Depends(db)):
    """Propositions with blended probability and 24h change. sort=move ranks by |24h change|."""
    at, known = times(as_of, known_as_of)
    g = graph(cur, as_of, known_as_of)
    pids = [i for i, n in g.nodes.items() if n["type"] == "proposition"
            and (not q or q.lower() in n["name"].lower())
            and (not entity or any(e["dst"] == entity for e in g.out[g.question_of(i)] if e["type"] == "involves"))]
    probs = prop_probs(cur, g, pids, at)
    rows = [proposition_summary(g, p, probs) for p in pids if include_closed or probs[p]["prob"] is not None]
    return dict(as_of=at, known_as_of=known, count=len(rows), propositions=sorted(rows, key=SORTS[sort])[:limit])

@app.get("/propositions/{proposition_id}")
def get_proposition(proposition_id: str, as_of: datetime | None = AsOf,
                    known_as_of: datetime | None = KnownAsOf, cur=Depends(db)):
    """Proposition (one rung of a question) with its markets, its question, the question's
    entities and logical relations."""
    at, known = times(as_of, known_as_of)
    g = graph(cur, as_of, known_as_of)
    n = g.get(proposition_id, "proposition")
    mkts = g.markets(proposition_id)
    mprobs = market_probs(cur, [m for m, _, _ in mkts], at, g)
    logical = [dict(relation=e["type"], direction="out", proposition=g.ref(e["dst"]), note=e["note"],
                    provenance=e["provenance"], **(e["attrs"] or {}))
               for e in g.out[proposition_id] if e["type"] in LOGICAL]
    logical += [dict(relation=e["type"], direction="in", proposition=g.ref(e["src"]), note=e["note"],
                     provenance=e["provenance"], **(e["attrs"] or {}))
                for e in g.inc[proposition_id] if e["type"] in LOGICAL]
    return dict(
        as_of=at, known_as_of=known,
        **proposition_summary(g, proposition_id, prop_probs(cur, g, [proposition_id], at)),
        valid_from=n["valid_from"],
        question=g.ref(g.question_of(proposition_id)) if g.question_of(proposition_id) else None,
        markets=[dict(id=m, relation=rel, note=note, venue=g.nodes[m]["attrs"]["venue"],
                      name=g.nodes[m]["name"], rules=g.nodes[m]["attrs"].get("rules"), **(mprobs.get(m) or {}))
                 for m, rel, note in mkts],
        entities=entity_links(g, g.question_of(proposition_id)),
        logical=logical)

@app.get("/propositions/{proposition_id}/history")
def proposition_history(proposition_id: str, start: datetime | None = None, end: datetime | None = None,
                        interval: str = Query("1h", pattern="^(1h|1d)$"),
                        known_as_of: datetime | None = KnownAsOf, cur=Depends(db)):
    """Blended probability series (default: last 30 days, hourly). Each point uses the markets
    observed in that bucket; `n_markets` says how many."""
    end = end or datetime.now(timezone.utc)
    start = start or end - timedelta(days=30)
    g = Graph(cur, end, known_as_of or datetime.now(timezone.utc))
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
    securities; at most MUTEX_SHOWN mutex), so a hub's 600 questions don't crowd out its links
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
               as_of: datetime | None = AsOf, known_as_of: datetime | None = KnownAsOf, cur=Depends(db)):
    """Neighbourhood of any question, proposition, market, entity or security, for visualisation.
    Questions are picked live first, then by 24h trading volume; other links by strength.
    `totals` = how many links of each kind the root has (the graph shows at most `fanout`)."""
    at, known = times(as_of, known_as_of)
    g = graph(cur, as_of, known_as_of)
    if node_id not in g.nodes: raise HTTPException(404, f"{node_id} not found at this as_of / known_as_of")
    qp = {}
    def rank(e, other):
        if g.nodes[other]["type"] != "question": return (0, 0, -(e["weight"] or 0))
        if other not in qp: qp.update(question_probs(cur, g, [other], at))
        v = qp[other]
        return (v["status"] != "live", -(v["volume_24h"] or 0), -(e["weight"] or 0))
    seen, edges, totals = neighbourhood(g, node_id, depth, fanout, rank)
    probs = prop_probs(cur, g, [i for i in seen if g.nodes[i]["type"] == "proposition"], at)
    probs |= question_probs(cur, g, [i for i in seen if g.nodes[i]["type"] == "question"], at)
    return dict(as_of=at, known_as_of=known, root=node_id, totals=totals,
                nodes=[g.ref(i) | dict(depth=d, entity_type=g.nodes[i]["attrs"].get("entity_type"),
                                       venue=g.nodes[i]["attrs"].get("venue"),
                                       **{k: probs[i][k] for k in ("prob", "change_24h", "status")} if i in probs else {})
                       for i, d in seen.items()],
                edges=[dict(src=e["src"], dst=e["dst"], type=e["type"], weight=e["weight"],
                            confidence=e["confidence"], provenance=e["provenance"], note=e["note"])
                       for e in edges])

@app.get("/propositions/{proposition_id}/graph")
def proposition_graph(proposition_id: str, depth: int = Query(2, ge=1, le=3), fanout: int = Query(30, ge=1, le=200),
                      as_of: datetime | None = AsOf, known_as_of: datetime | None = KnownAsOf,
                      cur=Depends(db)):
    """Neighbourhood of a proposition up to `depth` hops (edges followed in both directions)."""
    graph(cur, as_of, known_as_of).get(proposition_id, "proposition")
    return node_graph(proposition_id, depth, fanout, as_of, known_as_of, cur)

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
                 as_of: datetime | None = AsOf, known_as_of: datetime | None = KnownAsOf, cur=Depends(db)):
    """Venue markets by how they fared in cross-venue matching: `merged` (shares a proposition with
    another market), `linked` (near_equivalent / implies only), `unmatched` (parsed, no partner),
    `unparsed` (the contract parser couldn't read it; `issues` says why). Counts cover all statuses."""
    at, known = times(as_of, known_as_of)
    g = graph(cur, as_of, known_as_of)
    mids = [i for i, n in g.nodes.items() if n["type"] == "market"
            and (not venue or n["attrs"].get("venue") == venue)
            and (not q or q.lower() in n["name"].lower())]
    live = market_probs(cur, mids, at, g)
    mids = [i for i in mids if include_closed or i in live]
    match = lambda i: g.nodes[i]["attrs"].get("match") or "unmatched"   # graphs built before match status
    counts = {s: sum(1 for i in mids if match(i) == s) for s in MATCH_STATUSES}
    rows = [dict(market_row(g, i), prob=(live.get(i) or {}).get("prob")) for i in mids if match(i) in status]
    rows.sort(key=lambda r: (MATCH_STATUSES.index(r["match"]) if r["match"] in MATCH_STATUSES else 9, r["name"]))
    return dict(as_of=at, known_as_of=known, counts=counts, count=len(rows), markets=rows[:limit])

@app.get("/markets/{market_id}")
def get_market(market_id: str, as_of: datetime | None = AsOf, known_as_of: datetime | None = KnownAsOf,
               cur=Depends(db)):
    """One market: match status, parsed contract signature, parser issues, the nearest rejected
    candidates (with the reason each was rejected) and its rules text."""
    at, known = times(as_of, known_as_of)
    g = graph(cur, as_of, known_as_of)
    n = g.get(market_id, "market")
    a = n["attrs"]
    cands = [dict(c, name=g.nodes[c["market"]]["name"] if c["market"] in g.nodes else None,
                  venue=c["market"].split(":")[1]) for c in a.get("candidates") or []]
    return dict(as_of=at, known_as_of=known, **market_row(g, market_id),
                prob=(market_probs(cur, [market_id], at, g).get(market_id) or {}).get("prob"),
                signature=a.get("signature"), candidates=cands, rules=a.get("rules"))

@app.get("/securities")
def list_securities(as_of: datetime | None = AsOf, known_as_of: datetime | None = KnownAsOf, cur=Depends(db)):
    at, known = times(as_of, known_as_of)
    g = graph(cur, as_of, known_as_of)
    return [dict(**g.ref(i), **{k: n["attrs"].get(k) for k in ("ticker", "asset_class", "country", "sector", "index")},
                 n_exposures=sum(1 for e in g.inc[i] if e["type"] == "exposure"))
            for i, n in g.nodes.items() if n["type"] == "security"]

@app.get("/entities")
def list_entities(type: str | None = Query(None, description="person | country | organisation_or_place | concept"),
                  as_of: datetime | None = AsOf, known_as_of: datetime | None = KnownAsOf, cur=Depends(db)):
    at, known = times(as_of, known_as_of)
    g = graph(cur, as_of, known_as_of)
    # a question is live when any of its rungs has a market priced in the last 7 days
    priced = market_probs(cur, [i for i, n in g.nodes.items() if n["type"] == "market"], at, g)
    live = {q: any(m in priced for p, _, _ in g.rungs(q) for m, _, _ in g.markets(p))
            for q, n in g.nodes.items() if n["type"] == "question"}
    return [dict(**g.ref(i), entity_type=n["attrs"].get("entity_type"), description=n["attrs"].get("description"),
                 n_questions=sum(1 for e in g.inc[i] if e["type"] == "involves"),
                 n_live_questions=sum(1 for e in g.inc[i] if e["type"] == "involves" and live.get(e["src"])))
            for i, n in g.nodes.items() if n["type"] == "entity" and (not type or n["attrs"].get("entity_type") == type)]

@app.get("/entities/{entity_id}")
def get_entity(entity_id: str, as_of: datetime | None = AsOf, known_as_of: datetime | None = KnownAsOf,
               cur=Depends(db)):
    """Entity with every question that involves it (live first, then by 24h trading volume), its
    links, and securities exposed to it."""
    at, known = times(as_of, known_as_of)
    g = graph(cur, as_of, known_as_of)
    n = g.get(entity_id, "entity")
    qids = [e["src"] for e in g.inc[entity_id] if e["type"] == "involves"]
    qp = question_probs(cur, g, qids, at)
    return dict(as_of=at, known_as_of=known, **g.ref(entity_id), **n["attrs"],
                questions=sorted((question_summary(g, x, qp) for x in qids),
                                 key=lambda r: (r["status"] != "live", -(r["volume_24h"] or 0), r["name"])),
                links=sorted((dict(relation=e["type"], entity=g.ref(other), weight=e["weight"],
                                   provenance=e["provenance"]) for e, other in g.linked(entity_id)),
                             key=lambda l: -(l["weight"] or 0)),
                exposures=[dict(security=g.ref(e["dst"]), weight=e["weight"], rationale=e["note"])
                           for e in g.out[entity_id] if e["type"] == "exposure"])

def direction(impact):
    return "unknown" if impact is None else "up" if impact > 0 else "down" if impact < 0 else "neutral"

def risk(impact, prob):
    return None if impact is None or prob is None else round(abs(impact) * math.sqrt(prob * (1 - prob)), 4)

def path_out(g, q):
    return dict(strength=round(q["strength"], 4), sign=q["sign"], channel=q["channel"], role=q["role"],
                nodes=[g.ref(x) for x in q["nodes"]])

EXPOSURE_SORTS = {"score": lambda r: -r["score"], "impact": lambda r: -abs(r["impact"] or 0),
                  "risk": lambda r: -(r["risk"] or 0)}

@app.get("/securities/{ticker}/exposures")
def security_exposures(ticker: str, min_score: float = Query(0.1, ge=0, le=1),
                       sort: str = Query("score", pattern="^(score|impact|risk)$"),
                       limit: int = Query(50, le=500), include_closed: bool = IncludeClosed,
                       as_of: datetime | None = AsOf, known_as_of: datetime | None = KnownAsOf,
                       cur=Depends(db)):
    """Questions this security is exposed to, summed over every path question -> entity
    [-> entity] -> security: `score` (relevance 0-1), `impact` (signed: > 0 = expected to rise if
    the event happens), `direction`, `risk` (|impact| x sqrt(p(1-p))) and the strongest `paths`."""
    at, known = times(as_of, known_as_of)
    g = graph(cur, as_of, known_as_of)
    sid = f"sec:{ticker}"
    g.get(sid, "security")
    hits = {p: v for p, v in g.exposures_of(sid).items() if v["score"] >= min_score}
    qp = question_probs(cur, g, list(hits), at)
    rows = [dict(question_summary(g, p, qp), score=v["score"], impact=v["impact"],
                 direction=direction(v["impact"]), risk=risk(v["impact"], qp[p]["prob"]),
                 path=[g.ref(x) for x in v["paths"][0]["nodes"]],
                 paths=[path_out(g, q) for q in v["paths"][:3]], n_paths=len(v["paths"]))
            for p, v in hits.items() if include_closed or qp[p]["status"] == "live"]
    rows.sort(key=lambda r: (EXPOSURE_SORTS[sort](r), -abs(r["change_24h"] or 0)))
    return dict(as_of=at, known_as_of=known, security=g.ref(sid), count=len(rows), exposures=rows[:limit])

class Holding(BaseModel):
    ticker: str
    weight: float = Field(description="Portfolio weight, e.g. 0.05 for 5%")

class ScanRequest(BaseModel):
    holdings: list[Holding]
    as_of: datetime | None = None
    known_as_of: datetime | None = None
    min_score: float = 0.1
    move_threshold: float = Field(0.05, description="Flag questions whose headline |24h change| >= this")
    limit: int = 25
    include_closed: bool = False

@app.post("/portfolio/scan")
def portfolio_scan(req: ScanRequest, cur=Depends(db)):
    """Rank questions by portfolio exposure = sum over holdings of weight x exposure score.
    `impact` = sum of weight x signed impact (> 0: the portfolio is expected to gain if the event
    happens; holdings on opposite sides offset), `risk` = |impact| x sqrt(p(1-p)).
    `moved` flags questions whose headline probability moved >= move_threshold in 24h."""
    at, known = times(req.as_of, req.known_as_of)
    g = graph(cur, req.as_of, req.known_as_of)
    agg, net, contrib, unknown = defaultdict(float), {}, defaultdict(list), []
    for h in req.holdings:
        sid = f"sec:{h.ticker}"
        if sid not in g.nodes:
            unknown.append(h.ticker); continue
        for p, v in g.exposures_of(sid).items():
            if v["score"] >= req.min_score:
                agg[p] += h.weight * v["score"]
                if v["impact"] is not None:
                    net[p] = net.get(p, 0.0) + h.weight * v["impact"]
                contrib[p].append(dict(ticker=h.ticker, weight=h.weight, score=v["score"], impact=v["impact"],
                                       via=[g.nodes[x]["name"] for x in v["paths"][0]["nodes"][1:-1]]))
    qp = question_probs(cur, g, list(agg), at)
    live = [p for p in agg if req.include_closed or qp[p]["status"] == "live"]
    top = sorted(live, key=lambda p: -agg[p])[:req.limit]
    rows = []
    for p in top:
        r = question_summary(g, p, qp)
        impact = round(net[p], 4) if p in net else None
        rows.append(dict(r, exposure=round(agg[p], 4), impact=impact, direction=direction(impact),
                         risk=risk(impact, r["prob"]),
                         moved=r["change_24h"] is not None and abs(r["change_24h"]) >= req.move_threshold,
                         holdings=sorted(contrib[p], key=lambda c: -c["weight"] * c["score"])[:5]))
    return dict(as_of=at, known_as_of=known, unknown_tickers=unknown, count=len(live), questions=rows)

@app.get("/", include_in_schema=False)
def web():
    """Interactive graph explorer (web/index.html)."""
    return FileResponse(Path(__file__).parent / "web" / "index.html")

EXCEL = Path(__file__).parent / "excel"

@app.get("/excel/manifest.xml", include_in_schema=False)
def excel_manifest(request: Request):
    """Excel add-in manifest pointing at this server (Office requires HTTPS; Railway terminates TLS)."""
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme).split(",")[0]
    base = f"{scheme}://{request.headers.get('host', request.url.netloc)}"
    xml = (EXCEL / "manifest.xml").read_text().replace("{BASE}", base)
    return Response(xml, media_type="application/xml",
                    headers={"Content-Disposition": 'attachment; filename="event-risk-manifest.xml"'})

app.mount("/excel", StaticFiles(directory=EXCEL), name="excel")  # functions.js/json, taskpane.html, icons
