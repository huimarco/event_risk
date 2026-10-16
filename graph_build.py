#!/usr/bin/env python3
"""
Build the knowledge graph from collected contract terms, Wikipedia / Wikidata, and one curated
input (entity -> security exposures) into graph_nodes / graph_edges. Bitemporal and append-only: each run diffs the desired graph against
the current one and records only changes (new or changed -> 'assert', gone -> 'retract').

Nodes: market, proposition, entity, security
Edges:
  market -> proposition       same_as           markets with matching contract signatures
                                                (matching.py) form one proposition
  proposition -> proposition  near_equivalent   same event, materially different terms
                              implies           signature windows/thresholds nest, or
                                                date/strike ladders within an event
                              mutex             mutually exclusive events (venue flags)
  proposition -> entity       involves          phrases in market titles linked to Wikipedia
                                                articles (entities.py); entities are
                                                regenerated from the markets on every run
  entity -> entity            citizen_of, ...   Wikidata statements between entities
                              related           Wikipedia co-occurrence (entities.py)
  entity -> security          exposure          seeds/exposures.csv — the only hand-written
                                                input (analyst priors until event betas)
Human overrides of market matches (seeds/match_overrides.csv) always win over the algorithm.

Scope: markets matching THEME_REGEX, minus EXCLUDE_REGEX noise.

  python graph_build.py           build / update the graph
  python graph_build.py --review  CSV of matches, rejected candidates (with reasons) and
                                  unparsed markets, for writing overrides
"""
import csv, hashlib, itertools, json, re, sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import entities, matching
from collector import THEME_REGEX, connect, insert, log, run_job

ROOT = Path(__file__).parent
SEEDS = ROOT / "seeds"
REFERENCE_VALID_FROM = datetime(2020, 1, 1, tzinfo=timezone.utc)  # reference facts: valid for all our history
# Theme-matching markets that carry no risk signal (speech mentions, trivia).
EXCLUDE_REGEX = re.compile(r"what will .* say|shake hands|nobel|pope|covid|births", re.I)
# Subtitles that make an event a date ladder: "Before 2027", "Before Jan 1, 2028", "June 30".
DATE_SUBTITLE = re.compile(r"^(before|by)\b|^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.? \d", re.I)
GREATER, LESS = {"greater", "greater_or_equal"}, {"less", "less_or_equal"}

def seed(name):
    with open(SEEDS / name) as f:
        return [r for r in csv.DictReader(f) if not next(iter(r.values()), "").startswith("#")]

def utc(x):
    return x.replace(tzinfo=timezone.utc) if x else None

def node(node_id, node_type, name, valid_from, **attrs):
    return dict(node_id=node_id, node_type=node_type, name=name, attrs=attrs,
                valid_from=valid_from, valid_to=None)

def edge(src, dst, edge_type, valid_from, weight=1.0, confidence=1.0, provenance="rule",
         note=None, sign=None, **attrs):
    return dict(edge_id=f"{src}|{edge_type}|{dst}", src_id=src, dst_id=dst, edge_type=edge_type,
                sign=sign, weight=weight, confidence=confidence, provenance=provenance, note=note,
                attrs=attrs, valid_from=valid_from, valid_to=None)

def market_id(m):
    return f"mkt:{m['venue']}:{m['market_id']}"

def load_markets(con):
    """Latest terms of in-scope markets. Timestamps come back naive UTC."""
    cur = con.execute("""
        SELECT venue, market_id, event_id, series_id, event_title, title, subtitle, rules_primary,
               neg_risk, strike_type, floor_strike, cap_strike, tags::VARCHAR AS tags,
               open_time AT TIME ZONE 'UTC' AS open_time, close_time AT TIME ZONE 'UTC' AS close_time,
               first_seen_at AT TIME ZONE 'UTC' AS first_seen_at
        FROM v_market_latest""")
    cols = [d[0] for d in cur.description]
    out = []
    for row in cur.fetchall():
        m = dict(zip(cols, row))
        for k in ("open_time", "close_time", "first_seen_at"):
            m[k] = utc(m[k])
        m["text"] = " ".join(str(m[k]) for k in ("series_id", "event_title", "title", "subtitle", "tags") if m[k])
        if THEME_REGEX.search(m["text"]) and not EXCLUDE_REGEX.search(m["text"]):
            out.append(m)
    return out

def market_name(m):
    title, sub = m["title"] or m["event_title"] or m["market_id"], m["subtitle"]
    return f"{title} — {sub}" if sub and sub.lower() not in title.lower() else title

# ── market matching ───────────────────────────────────────────────────────────
def latest_prices(con):
    """{(venue, market_id): latest cleaned probability in the last 7 days}, for matching's price check."""
    return {(v, m): p for v, m, p in con.execute("""
        SELECT venue, market_id, arg_max(prob, ts) FROM v_market_prob
        WHERE ts > now() - INTERVAL 7 DAY AND prob IS NOT NULL GROUP BY ALL""").fetchall()}

def discover(con, markets):
    """Entities for these markets (plus those carrying exposures), from Wikipedia / Wikidata."""
    questions = {(m["venue"], m["market_id"]): matching.question_text(m) for m in markets}
    return entities.build(con, questions, extra_qids={r["qid"] for r in seed("exposures.csv")})

def match(markets, prices, ents):
    """Signatures, matches and rejections for in-scope markets (see matching.py)."""
    return matching.match_markets(markets, ents.mentions, ents.parents, prices)

def resolve(matches, keys):
    """Pairwise relations: algorithm first, then seeds/match_overrides.csv on top.
    Returns {frozenset(pair): (relation, src, dst, confidence, provenance, note)}."""
    rel = {frozenset((m.src, m.dst)): (m.relation, m.src, m.dst, m.confidence, "signature",
                                       "; ".join(m.diffs) or None) for m in matches}
    for o in seed("match_overrides.csv"):
        a, b = (o["venue_a"], o["market_a"]), (o["venue_b"], o["market_b"])
        if a not in keys or b not in keys:
            log.warning("override for out-of-scope market ignored: %s %s", a, b); continue
        if o["relation"] == "different": rel.pop(frozenset((a, b)), None)
        else: rel[frozenset((a, b))] = (o["relation"], a, b, 1.0, "curated", o["note"] or None)
    return rel

def group(markets, relations):
    """{market key: proposition id}. same_as pairs are unioned; a proposition is named after its
    earliest-listed market, so its id stays stable as later markets join."""
    parent = {k: k for k in (( m["venue"], m["market_id"]) for m in markets)}
    find = lambda k: k if parent[k] == k else find(parent[k])
    for r, a, b, *_ in relations.values():
        if r == "same_as": parent[find(a)] = find(b)
    first = {(m["venue"], m["market_id"]): (m["open_time"] or m["first_seen_at"] or REFERENCE_VALID_FROM,
                                             m["venue"], m["market_id"]) for m in markets}
    groups = defaultdict(list)
    for k in parent: groups[find(k)].append(k)
    prop_of = {}
    for ks in groups.values():
        anchor = min(ks, key=first.get)
        for k in ks: prop_of[k] = f"prop:{anchor[0]}:{anchor[1]}"
    return prop_of

# ── desired graph ─────────────────────────────────────────────────────────────
def build(markets, prices, ents):
    nodes, edges = {}, {}
    add_n = lambda n: nodes.__setitem__(n["node_id"], n)
    add_e = lambda e: edges.__setitem__(e["edge_id"], e)

    # markets -> propositions, by contract signature (+ overrides)
    key = lambda m: (m["venue"], m["market_id"])
    by_key = {key(m): m for m in markets}
    _, matches, _ = match(markets, prices, ents)
    relations = resolve(matches, by_key)
    keyed = group(markets, relations)
    prop_of = {market_id(m): keyed[key(m)] for m in markets}
    joined = defaultdict(list)  # notes of the same_as matches that joined each market
    for r, a, b, _, prov, note in relations.values():
        if r == "same_as":
            for k in (a, b): joined[k].append((prov, note))
    members = defaultdict(list)
    for m in markets:
        pid, start = prop_of[market_id(m)], m["open_time"] or m["first_seen_at"]
        members[pid].append(m)
        add_n(node(market_id(m), "market", market_name(m), start, venue=m["venue"],
                   market_id=m["market_id"], event_id=m["event_id"], series_id=m["series_id"],
                   close_time=m["close_time"], rules=m["rules_primary"]))
        how = joined[key(m)]
        add_e(edge(market_id(m), pid, "same_as", start,
                   provenance="curated" if any(p == "curated" for p, _ in how) else "signature" if how else "rule",
                   note="; ".join(sorted({n for _, n in how if n})) or None))

    prop_start = {}
    for pid, ms in members.items():
        anchor = next(m for m in ms if pid == f"prop:{m['venue']}:{m['market_id']}")
        prop_start[pid] = min((m["open_time"] or m["first_seen_at"] for m in ms if m["open_time"] or m["first_seen_at"]), default=None)
        closes = [m["close_time"] for m in ms if m["close_time"]]
        add_n(node(pid, "proposition", market_name(anchor), prop_start[pid],
                   close_time=min(closes) if closes else None,
                   venues=sorted({m["venue"] for m in ms}), n_markets=len(ms)))

    # proposition <-> proposition from signature comparison
    for r, a, b, conf, prov, note in relations.values():
        pa, pb = keyed[a], keyed[b]
        if r == "same_as" or pa == pb: continue
        if r == "near_equivalent": pa, pb = sorted((pa, pb))
        add_e(edge(pa, pb, r, max(filter(None, (prop_start[pa], prop_start[pb])), default=None),
                   confidence=conf, provenance=prov, note=note))

    # logical edges within each venue event
    by_event = defaultdict(list)
    for m in markets:
        by_event[(m["venue"], m["event_id"])].append(m)
    for (venue, event_id), ms in by_event.items():
        pids = {prop_of[market_id(m)]: m for m in ms}  # one market per proposition
        if len(pids) < 2: continue
        start = lambda a, b: max(filter(None, (prop_start[a], prop_start[b])), default=None)
        if any(m["neg_risk"] for m in ms):
            for a, b in itertools.combinations(sorted(pids), 2):
                add_e(edge(a, b, "mutex", start(a, b), event_id=event_id))
            continue
        # strike ladders: higher floor implies lower floor; lower cap implies higher cap
        for kinds, key, desc in ((GREATER, "floor_strike", True), (LESS, "cap_strike", False)):
            lad = sorted(((m[key], p) for p, m in pids.items()
                          if m["strike_type"] in kinds and m[key] is not None), reverse=desc)
            for (_, a), (_, b) in zip(lad, lad[1:]):
                add_e(edge(a, b, "implies", start(a, b), ladder="strike", event_id=event_id))
        # date ladders: an earlier deadline implies the later one
        if all(m["subtitle"] and DATE_SUBTITLE.search(m["subtitle"]) for m in pids.values()):
            lad = sorted((m["close_time"], p) for p, m in pids.items() if m["close_time"])
            for (ta, a), (tb, b) in zip(lad, lad[1:]):
                if ta < tb:
                    add_e(edge(a, b, "implies", start(a, b), ladder="date", event_id=event_id))

    # entities, securities, curated links and exposures
    # entities (discovered from the markets), securities, and the links between them
    for eid, e in ents.entities.items():
        add_n(node(eid, "entity", e["name"], REFERENCE_VALID_FROM, entity_type=e["type"], qid=e["qid"],
                   description=e["description"], wikipedia=e["title"]))
    with open(ROOT / "securities.csv") as f:
        for s in csv.DictReader(f):
            add_n(node(f"sec:{s['ticker']}", "security", s["name"], REFERENCE_VALID_FROM, ticker=s["ticker"],
                       asset_class=s["asset_class"], country=s["country"], role=s["role"]))
    for a, b, kind, w in ents.links:
        add_e(edge(a, b, kind, REFERENCE_VALID_FROM, weight=w or 0.0, confidence=0.9 if kind != "related" else 0.6,
                   provenance="wikidata" if kind != "related" else "wikipedia_cooccurrence"))
    for r in seed("exposures.csv"):
        add_e(edge(f"wd:{r['qid']}", f"sec:{r['ticker']}", "exposure", REFERENCE_VALID_FROM,
                   weight=float(r["weight"]), confidence=0.3, provenance="analyst_prior",
                   note=r["rationale"] or None))

    # proposition -> entity: the Wikipedia-linked mentions in its markets' questions
    for pid, ms in members.items():
        seen = {}
        for m in ms:
            for mt in ents.mentions.get((m["venue"], m["market_id"]), []):
                seen.setdefault(mt["id"], mt)
        for eid, mt in seen.items():
            add_e(edge(pid, eid, "involves", prop_start[pid], confidence=0.8 if mt["named"] else 0.6,
                       provenance="wikipedia_link", matched=mt["surface"],
                       role="actor_or_place" if mt["named"] else "concept"))

    dangling = [e for e in edges.values() if e["src_id"] not in nodes or e["dst_id"] not in nodes]
    for e in dangling:
        log.warning("dropping edge with unknown endpoint: %s", e["edge_id"])
        del edges[e["edge_id"]]
    return nodes, edges

# ── diff + write ──────────────────────────────────────────────────────────────
def digest(row):
    keep = {k: v for k, v in row.items() if k not in ("recorded_at", "run_id")}
    return hashlib.sha256(json.dumps(keep, sort_keys=True, default=str).encode()).hexdigest()[:16]

def sync(con, table, key, desired, run_id, t):
    """Rows asserting new/changed items and retracting vanished ones."""
    cols = [c for c, *_ in con.execute(f"DESCRIBE {table}").fetchall()]
    current = {k: (op, h) for k, op, h in con.execute(f"""
        SELECT {key}, arg_max(op, recorded_at), arg_max(attrs_hash, recorded_at)
        FROM {table} GROUP BY 1""").fetchall()}
    rows = []
    for k, d in desired.items():
        h = digest(d)
        if current.get(k) != ("assert", h):
            rows.append(d | dict(attrs=json.dumps(d["attrs"], default=str), attrs_hash=h, op="assert",
                                 recorded_at=t, run_id=run_id))
    for k, (op, _) in current.items():
        if op == "assert" and k not in desired:
            rows.append(dict.fromkeys(cols) | {key: k, "op": "retract", "recorded_at": t, "run_id": run_id})
    return [dict.fromkeys(cols) | r for r in rows]

def graph(con, run_id, errors, stats):
    markets = load_markets(con)
    nodes, edges = build(markets, latest_prices(con), discover(con, markets))
    t = datetime.now(timezone.utc)
    node_rows = sync(con, "graph_nodes", "node_id", nodes, run_id, t)
    edge_rows = sync(con, "graph_edges", "edge_id", edges, run_id, t)
    con.begin()
    insert(con, "graph_nodes", node_rows)
    insert(con, "graph_edges", edge_rows)
    con.commit()
    stats |= {"nodes": Counter(n["node_type"] for n in nodes.values()),
              "edges": Counter(e["edge_type"] for e in edges.values()),
              "node_changes": len(node_rows), "edge_changes": len(edge_rows)}
    return True

# ── review ────────────────────────────────────────────────────────────────────
def review():
    """CSV for human review: algorithmic matches, rejected candidates with the reason, and
    markets the parser couldn't read. Copy a row into seeds/match_overrides.csv to override."""
    con = connect()
    markets, prices = load_markets(con), latest_prices(con)
    sigs, matches, rejected = match(markets, prices, discover(con, markets))
    con.close()
    name = {(m["venue"], m["market_id"]): market_name(m) for m in markets}
    w = csv.writer(sys.stdout)
    w.writerow(["kind", "relation_or_reason", "venue_a", "market_a", "name_a", "venue_b", "market_b", "name_b", "notes"])
    for m in sorted(matches, key=lambda m: (m.relation, name[m.src])):
        w.writerow(["match", m.relation, *m.src, name[m.src], *m.dst, name[m.dst], "; ".join(m.diffs)])
    for a, b, why in rejected:
        w.writerow(["rejected", why, *a, name[a], *b, name[b], ""])
    for s in sigs.values():
        if not s.parsed:
            w.writerow(["unparsed", "; ".join(s.issues), *s.key, name[s.key], "", "", "", ""])

if __name__ == "__main__":
    if "--review" in sys.argv:
        review()
    else:
        sys.exit(run_job("graph", graph))
