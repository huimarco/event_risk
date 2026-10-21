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

import factors
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
    now = datetime.now(timezone.utc)
    g = Graph(cur, now, now)
    g.latest = dict(at=now, now=all_market_probs(cur, now), ago=all_market_probs(cur, now - timedelta(hours=24)))
    _current.update(graph=g, loaded=time.monotonic())

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
                   sort: str = Query("move", pattern="^(move|prob|name)$"),
                   limit: int = Query(50, le=500), include_closed: bool = IncludeClosed,
                   cur=Depends(db)):
    """Questions with their headline probability (most-traded live rung) and its 24h change.
    sort=move ranks by |24h change|."""
    at = datetime.now(timezone.utc)
    g = graph(cur)
    if entity:
        entity = g.entity(entity) or g.get(entity, "entity")["id"]  # unknown -> 404
    qids = [i for i, n in g.nodes.items() if n["type"] == "question"
            and (not q or q.lower() in n["name"].lower())
            and (not entity or any(e["dst"] == entity for e in g.out[i] if e["type"] == "involves"))]
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
    it involves and the factors it is relevant to (factors.py)."""
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
    return dict(as_of=at, **question_summary(g, question_id, qp), rungs=rungs,
                entities=entity_links(g, question_id), factors=factors.links(g, question_id))

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
        factors=factors.links(g, g.question_of(proposition_id)),
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
               cur=Depends(db)):
    """Neighbourhood of any question, proposition, market, entity, security or factor
    (`factor:<id>`), for visualisation. Questions are picked live first, then by 24h trading
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
                                       family=g.nodes[i]["attrs"].get("family"),
                                       **{k: probs[i][k] for k in ("prob", "change_24h", "status")} if i in probs else {})
                       for i, d in seen.items()],
                edges=[dict(src=e["src"], dst=e["dst"], type=e["type"], weight=e["weight"],
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
    return dict(as_of=at, **market_row(g, market_id), **t,
                prob=px.get("prob"), volume_24h=px.get("volume_24h"), open_interest=px.get("open_interest"),
                updated=px.get("observed_at"), status="live" if px.get("prob") is not None else "closed",
                signature=a.get("signature"), candidates=cands)

@app.get("/securities")
def list_securities(cur=Depends(db)):
    at = datetime.now(timezone.utc)
    g = graph(cur)
    return [dict(**g.ref(i), **{k: n["attrs"].get(k) for k in ("ticker", "asset_class", "country", "sector", "index")},
                 n_exposures=sum(1 for e in g.inc[i] if e["type"] == "exposure"))
            for i, n in g.nodes.items() if n["type"] == "security"]

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
    by 24h trading volume), its links, and securities exposed to it."""
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
                exposures=[dict(security=g.ref(e["dst"]), weight=e["weight"], rationale=e["note"])
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
def list_factors(family: str | None = Query(None, description="country_equity | fx | commodity | rates | industry"),
                 cur=Depends(db)):
    """The factor catalog (seeds/factors.csv) with how many questions are relevant to each."""
    at = datetime.now(timezone.utc)
    g = graph(cur)
    live, hits = live_questions(cur, g, at), factor_links(g)
    return [dict(f, n_questions=len(hits[f["id"]]), n_live_questions=sum(live[q] for q in hits[f["id"]]))
            for f in factors.CATALOG.values() if not family or f["family"] == family]

@app.get("/factors/{factor_id}")
def get_factor(factor_id: str, cur=Depends(db)):
    """Factor with every question relevant to it (live first, then by 24h trading volume) and the
    paths that link each one (question -> entity [-> its country] -> factor)."""
    if factor_id not in factors.CATALOG: raise HTTPException(404, f"factor {factor_id} not found")
    at = datetime.now(timezone.utc)
    g = graph(cur)
    links = factor_links(g, factor_id)[factor_id]
    qp = question_probs(cur, g, list(links), at)
    return dict(as_of=at, **factors.CATALOG[factor_id],
                questions=sorted((dict(question_summary(g, x, qp), paths=links[x]) for x in links),
                                 key=lambda r: (r["status"] != "live", -(r["volume_24h"] or 0), r["name"])))

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
                       cur=Depends(db)):
    """Questions this security is exposed to, summed over every path question -> entity
    [-> entity] -> security: `score` (relevance 0-1), `impact` (signed: > 0 = expected to rise if
    the event happens), `direction`, `risk` (|impact| x sqrt(p(1-p))) and the strongest `paths`."""
    at = datetime.now(timezone.utc)
    g = graph(cur)
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
    return dict(as_of=at, security=g.ref(sid), count=len(rows), exposures=rows[:limit])

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
