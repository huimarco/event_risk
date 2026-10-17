#!/usr/bin/env python3
"""
Knowledge-graph API for geopolitical event risk.

  uvicorn api:app            (docs at /docs)

Auth: set API_KEYS="key1,key2" to require an `X-API-Key` header; unset = open (local dev).

Time travel: every read endpoint takes
  as_of        valid time: the state of the world at this moment (default now)
  known_as_of  transaction time: only what the graph had recorded by then (default now).
               Use both = T for a backtest-safe, point-in-time view.

Proposition probability = blend of its markets' latest cleaned prices at or before as_of (within
7 days), weighted by open interest when every market reports it, else equally. Markets join a
proposition only when their contract signatures match (matching.py); propositions with materially
different terms are linked as `near_equivalent` and never blended.

Entities are discovered from market titles via Wikipedia; entity links come from Wikidata statements
and Wikipedia co-occurrence. Exposure scores are path products: involves x [entity link weight] x
exposure weight. Exposure weights are analyst priors (seeds/exposures.csv) until empirical event
betas exist; scores rank relevance, they are not betas.
"""
import json, os, threading, time
from contextlib import asynccontextmanager
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone

from pathlib import Path

import duckdb
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from collector import DB

LOOKBACK = timedelta(days=7)
MUTEX_SHOWN = 3  # per node in /graph neighbourhoods
GRAPH_TTL = 300  # seconds the current graph is reused; it only changes when graph_build runs
BLENDED, LOGICAL = {"same_as"}, {"mutex", "implies", "near_equivalent"}
API_KEYS = {k for k in os.getenv("API_KEYS", "").split(",") if k}
ISO = "strftime({} AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ')"
INTERVALS = {"1h": "1 hour", "1d": "1 day"}

def require_key(request: Request, x_api_key: str | None = Header(None)):
    if request.url.path == "/": return  # the web page itself is public; its data calls are not
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
IncludeClosed = Query(False, description="Include propositions with no price in the 7 days before as_of (resolved / delisted). Default: live only.")

# ── graph + prices ────────────────────────────────────────────────────────────
class Graph:
    """The graph at (as_of, known_as_of), loaded once per request."""
    def __init__(self, cur, as_of, known):
        self.nodes = {r[0]: dict(id=r[0], type=r[1], name=r[2], attrs=json.loads(r[3] or "{}"), valid_from=r[4])
                      for r in cur.execute(f"""
            SELECT node_id, node_type, name, attrs::VARCHAR, {ISO.format('valid_from')}
            FROM nodes_asof(?, ?)""", [as_of, known]).fetchall()}
        self.edges = [dict(src=r[0], dst=r[1], type=r[2], weight=r[3], confidence=r[4],
                           provenance=r[5], note=r[6], attrs=json.loads(r[7] or "{}"))
                      for r in cur.execute("""
            SELECT src_id, dst_id, edge_type, weight, confidence, provenance, note, attrs::VARCHAR
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

    def exposures_of(self, sec):
        """{proposition_id: (score, path)} for one security, walking back from it:
        security <- exposure - entity [<- link -> entity] <- involves - proposition."""
        best = {}
        for x in self.inc[sec]:
            if x["type"] != "exposure": continue
            held = x["src"]
            hops = [(held, x["weight"], [held])]
            hops += [(other, x["weight"] * (l["weight"] or 0), [other, held]) for l, other in self.linked(held)]
            for ent, w, via in hops:
                for inv in self.inc[ent]:
                    if inv["type"] != "involves": continue
                    score = round(inv["weight"] * w, 4)
                    if score > best.get(inv["src"], (0,))[0]:
                        best[inv["src"]] = (score, [inv["src"], *via, sec])
        return best

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
    return {m: dict(prob=p, open_interest=oi, observed_at=ts) for m, p, oi, ts in cur.execute(f"""
        SELECT 'mkt:' || venue || ':' || market_id, arg_max(prob, ts), arg_max(open_interest, ts),
               {ISO.format('max(ts)')}
        FROM v_market_prob WHERE ts <= ? AND ts > ? AND prob IS NOT NULL GROUP BY 1""",
        [at, at - LOOKBACK]).fetchall()}

def market_probs(cur, market_ids, at, g=None):
    if g is not None and g.latest:  # shared "now" graph: answer from memory
        snap = g.latest["now"] if at > g.latest["at"] - timedelta(hours=12) else g.latest["ago"]
        return {m: snap[m] for m in market_ids if m in snap}
    if not market_ids: return {}
    rows = cur.execute(f"""
        SELECT 'mkt:' || venue || ':' || market_id, arg_max(prob, ts), arg_max(open_interest, ts),
               {ISO.format('max(ts)')}
        FROM v_market_prob
        WHERE ts <= ? AND ts > ? AND prob IS NOT NULL
          AND 'mkt:' || venue || ':' || market_id IN (SELECT unnest(?::VARCHAR[]))
        GROUP BY 1""", [at, at - LOOKBACK, list(market_ids)]).fetchall()
    return {m: dict(prob=p, open_interest=oi, observed_at=ts) for m, p, oi, ts in rows}

def blend(vals):
    vals = [v for v in vals if v and v["prob"] is not None]
    if not vals: return None
    w = [v["open_interest"] for v in vals]
    if not all(w): w = [1.0] * len(vals)
    return round(sum(v["prob"] * x for v, x in zip(vals, w)) / sum(w), 4)

def prop_probs(cur, g, pids, at):
    """{pid: {prob, prob_24h_ago, change_24h}} from blended same_as markets."""
    members = {p: [m for m, rel, _ in g.markets(p) if rel in BLENDED] for p in pids}
    ids = {m for ms in members.values() for m in ms}
    now_, ago = market_probs(cur, ids, at, g), market_probs(cur, ids, at - timedelta(hours=24), g)
    out = {}
    for p, ms in members.items():
        a, b = blend([now_.get(m) for m in ms]), blend([ago.get(m) for m in ms])
        out[p] = dict(prob=a, prob_24h_ago=b,
                      change_24h=round(a - b, 4) if a is not None and b is not None else None)
    return out

def proposition_summary(g, pid, probs):
    n = g.nodes[pid]
    return dict(id=pid, name=n["name"], close_time=n["attrs"].get("close_time"),
                venues=n["attrs"].get("venues"), **probs.get(pid, {}))

# ── endpoints ─────────────────────────────────────────────────────────────────
@app.get("/health")
def health(cur=Depends(db)):
    last = cur.execute(f"SELECT {ISO.format('max(finished_at)')} FROM collection_runs WHERE status != 'failed'").fetchone()[0]
    return dict(status="ok", last_successful_run=last)

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
            and (not entity or any(e["dst"] == entity for e in g.out[i] if e["type"] == "involves"))]
    probs = prop_probs(cur, g, pids, at)
    rows = [proposition_summary(g, p, probs) for p in pids if include_closed or probs[p]["prob"] is not None]
    key = {"move": lambda r: -abs(r["change_24h"] or 0), "prob": lambda r: -(r["prob"] or 0),
           "name": lambda r: r["name"]}[sort]
    return dict(as_of=at, known_as_of=known, count=len(rows), propositions=sorted(rows, key=key)[:limit])

@app.get("/propositions/{proposition_id}")
def get_proposition(proposition_id: str, as_of: datetime | None = AsOf,
                    known_as_of: datetime | None = KnownAsOf, cur=Depends(db)):
    """Proposition with its markets, entities and logical relations."""
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
        markets=[dict(id=m, relation=rel, note=note, venue=g.nodes[m]["attrs"]["venue"],
                      name=g.nodes[m]["name"], rules=g.nodes[m]["attrs"].get("rules"), **(mprobs.get(m) or {}))
                 for m, rel, note in mkts],
        entities=[dict(**g.ref(e["dst"]), matched=(e["attrs"] or {}).get("matched"), confidence=e["confidence"])
                  for e in g.out[proposition_id] if e["type"] == "involves"],
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

def neighbourhood(g, root, depth, fanout):
    """Nodes within `depth` hops of `root` (edges in both directions, markets left out). Each node
    keeps its `fanout` strongest links *per kind of neighbour* (propositions, entities,
    securities; at most MUTEX_SHOWN mutex), so a hub's 60 propositions don't crowd out its
    links to other entities."""
    seen, frontier, edges = {root: 0}, deque([root]), {}
    while frontier:
        n = frontier.popleft()
        if seen[n] == depth: continue
        by_kind = defaultdict(list)
        for e in sorted(g.out[n] + g.inc[n], key=lambda e: -(e["weight"] or 0)):
            other = e["dst"] if e["src"] == n else e["src"]
            if g.nodes[other]["type"] != "market":
                by_kind[g.nodes[other]["type"]].append((e, other))
        for links in by_kind.values():
            mutex = [e for e, _ in links if e["type"] == "mutex"][:MUTEX_SHOWN]  # mutex sets are cliques
            for e, other in [(e, o) for e, o in links if e["type"] != "mutex" or e in mutex][:fanout]:
                edges[(e["src"], e["type"], e["dst"])] = e
                if other not in seen:
                    seen[other] = seen[n] + 1; frontier.append(other)
    return seen, [e for e in edges.values() if e["src"] in seen and e["dst"] in seen]

@app.get("/graph/{node_id}")
def node_graph(node_id: str, depth: int = Query(1, ge=1, le=3), fanout: int = Query(30, ge=1, le=200),
               as_of: datetime | None = AsOf, known_as_of: datetime | None = KnownAsOf, cur=Depends(db)):
    """Neighbourhood of any proposition, entity or security, for visualisation."""
    at, known = times(as_of, known_as_of)
    g = graph(cur, as_of, known_as_of)
    if node_id not in g.nodes: raise HTTPException(404, f"{node_id} not found at this as_of / known_as_of")
    seen, edges = neighbourhood(g, node_id, depth, fanout)
    probs = prop_probs(cur, g, [i for i in seen if g.nodes[i]["type"] == "proposition"], at)
    return dict(as_of=at, known_as_of=known, root=node_id,
                nodes=[g.ref(i) | dict(depth=d, entity_type=g.nodes[i]["attrs"].get("entity_type"),
                                       **{k: probs[i][k] for k in ("prob", "change_24h")} if i in probs else {})
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
    return [dict(**g.ref(i), entity_type=n["attrs"].get("entity_type"), description=n["attrs"].get("description"),
                 n_propositions=sum(1 for e in g.inc[i] if e["type"] == "involves"))
            for i, n in g.nodes.items() if n["type"] == "entity" and (not type or n["attrs"].get("entity_type") == type)]

@app.get("/entities/{entity_id}")
def get_entity(entity_id: str, as_of: datetime | None = AsOf, known_as_of: datetime | None = KnownAsOf,
               cur=Depends(db)):
    """Entity with the propositions that involve it, its links, and securities exposed to it."""
    at, known = times(as_of, known_as_of)
    g = graph(cur, as_of, known_as_of)
    n = g.get(entity_id, "entity")
    pids = [e["src"] for e in g.inc[entity_id] if e["type"] == "involves"]
    probs = prop_probs(cur, g, pids, at)
    return dict(as_of=at, known_as_of=known, **g.ref(entity_id), **n["attrs"],
                propositions=sorted((proposition_summary(g, p, probs) for p in pids),
                                    key=lambda r: -abs(r["change_24h"] or 0)),
                links=sorted((dict(relation=e["type"], entity=g.ref(other), weight=e["weight"],
                                   provenance=e["provenance"]) for e, other in g.linked(entity_id)),
                             key=lambda l: -(l["weight"] or 0)),
                exposures=[dict(security=g.ref(e["dst"]), weight=e["weight"], rationale=e["note"])
                           for e in g.out[entity_id] if e["type"] == "exposure"])

@app.get("/securities/{ticker}/exposures")
def security_exposures(ticker: str, min_score: float = Query(0.1, ge=0, le=1),
                       limit: int = Query(50, le=500), include_closed: bool = IncludeClosed,
                       as_of: datetime | None = AsOf, known_as_of: datetime | None = KnownAsOf,
                       cur=Depends(db)):
    """Propositions this security is exposed to, via proposition -> entity [-> entity] -> security."""
    at, known = times(as_of, known_as_of)
    g = graph(cur, as_of, known_as_of)
    sid = f"sec:{ticker}"
    g.get(sid, "security")
    hits = {p: v for p, v in g.exposures_of(sid).items() if v[0] >= min_score}
    probs = prop_probs(cur, g, list(hits), at)
    rows = [dict(proposition_summary(g, p, probs), score=s, path=[g.ref(x) for x in path])
            for p, (s, path) in hits.items() if include_closed or probs[p]["prob"] is not None]
    rows.sort(key=lambda r: (-r["score"], -abs(r["change_24h"] or 0)))
    return dict(as_of=at, known_as_of=known, security=g.ref(sid), count=len(rows), exposures=rows[:limit])

class Holding(BaseModel):
    ticker: str
    weight: float = Field(description="Portfolio weight, e.g. 0.05 for 5%")

class ScanRequest(BaseModel):
    holdings: list[Holding]
    as_of: datetime | None = None
    known_as_of: datetime | None = None
    min_score: float = 0.1
    move_threshold: float = Field(0.05, description="Flag propositions whose |24h change| >= this")
    limit: int = 25
    include_closed: bool = False

@app.post("/portfolio/scan")
def portfolio_scan(req: ScanRequest, cur=Depends(db)):
    """Rank propositions by portfolio exposure = sum over holdings of weight x exposure score.
    `moved` flags propositions whose blended probability moved >= move_threshold in 24h."""
    at, known = times(req.as_of, req.known_as_of)
    g = graph(cur, req.as_of, req.known_as_of)
    agg, contrib, unknown = defaultdict(float), defaultdict(list), []
    for h in req.holdings:
        sid = f"sec:{h.ticker}"
        if sid not in g.nodes:
            unknown.append(h.ticker); continue
        for p, (s, path) in g.exposures_of(sid).items():
            if s >= req.min_score:
                agg[p] += h.weight * s
                contrib[p].append(dict(ticker=h.ticker, weight=h.weight, score=s,
                                       via=[g.nodes[x]["name"] for x in path[1:-1]]))
    probs = prop_probs(cur, g, list(agg), at)
    live = [p for p in agg if req.include_closed or probs[p]["prob"] is not None]
    top = sorted(live, key=lambda p: -agg[p])[:req.limit]
    rows = []
    for p in top:
        r = proposition_summary(g, p, probs)
        rows.append(dict(r, exposure=round(agg[p], 4),
                         moved=r["change_24h"] is not None and abs(r["change_24h"]) >= req.move_threshold,
                         holdings=sorted(contrib[p], key=lambda c: -c["weight"] * c["score"])[:5]))
    return dict(as_of=at, known_as_of=known, unknown_tickers=unknown, count=len(live), propositions=rows)

@app.get("/", include_in_schema=False)
def web():
    """Interactive graph explorer (web/index.html)."""
    return FileResponse(Path(__file__).parent / "web" / "index.html")
