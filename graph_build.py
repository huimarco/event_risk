#!/usr/bin/env python3
"""
Build the knowledge graph from collected contract terms, Wikipedia / Wikidata, and the curated
inputs (entity -> security exposures and their channel signs) into graph_nodes / graph_edges. Bitemporal and append-only: each run diffs the desired graph against
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
                                                regenerated from the markets on every run.
                                                Signed: role (actor / target / party / concept),
                                                channel, weight = severity x role, sign = what
                                                YES means for the entity (matching.EFFECTS)
  entity -> entity            citizen_of, ...   Wikidata statements between entities
                              related           Wikipedia co-occurrence (entities.py)
  entity -> security          exposure          seeds/exposures.csv — analyst priors until event
                                                betas; per-channel signs (which securities gain
                                                from escalation) from seeds/channel_signs.csv
Human overrides of market matches (seeds/match_overrides.csv) always win over the algorithm.
Market nodes carry their match outcome (merged / linked / unmatched / unparsed, with the parsed
signature, parser issues and the nearest rejected candidates) for the review tab in the web app.

Scope: geopolitical markets (minus EXCLUDE_REGEX noise). Polymarket's geopolitics tag is the
venue's own classification; a Kalshi World/Politics market is geopolitical when it involves a
foreign state actor — a country other than the US, or a person/organisation belonging to one
(Wikidata parents) — so "Trump visits China" is in and "Senate confirms X" is out.

Runs after every collect, but only does work when its inputs changed: a new market, new contract
terms, an edited seed file, or new graph code. Otherwise it exits without touching the graph.
When it does run, decisions are point-in-time (matching.py) and Wikipedia lookups are cached
(entities.py), so existing connections stay as they were; only the new market's are added.

  python graph_build.py           build / update the graph
  python graph_build.py --review  CSV of matches, rejected candidates (with reasons) and
                                  unparsed markets, for writing overrides
  python graph_build.py --refresh wikidata      re-fetch entity facts (type, parent, statements)
  python graph_build.py --refresh relatedness   re-fetch Wikipedia co-occurrence counts
"""
import csv, hashlib, itertools, json, re, sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa

import entities, matching
from collector import connect, insert, log, run_job

ROOT = Path(__file__).parent
SEEDS = ROOT / "seeds"
REFERENCE_VALID_FROM = datetime(2020, 1, 1, tzinfo=timezone.utc)  # reference facts: valid for all our history
# Theme-matching markets that carry no risk signal (speech mentions, trivia).
EXCLUDE_REGEX = re.compile(r"what will .* say|shake hands|nobel|pope|covid|births", re.I)
# Subtitles that make an event a date ladder: "Before 2027", "Before Jan 1, 2028", "June 30".
DATE_SUBTITLE = re.compile(r"^(before|by)\b|^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.? \d", re.I)
GREATER, LESS = {"greater", "greater_or_equal"}, {"less", "less_or_equal"}
HOME = "Q30"  # United States: involving only the US is domestic politics
PRICE_LOOKUP_BATCH = 5000  # (market, time) requests per query; one huge ASOF join times out on MotherDuck
REFRESH_KINDS = {"wikidata": {"entity"}, "relatedness": {"mentions", "comentions", "df"}}  # wiki_cache kinds

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
    """Latest terms of candidate markets (geopolitical scope is decided after entity discovery),
    when each was first listed, and when it first had a price. Timestamps naive UTC."""
    cur = con.execute("""
        SELECT venue, market_id, event_id, series_id, event_title, title, subtitle, rules_primary,
               neg_risk, strike_type, floor_strike, cap_strike, tags::VARCHAR AS tags,
               open_time AT TIME ZONE 'UTC' AS open_time, close_time AT TIME ZONE 'UTC' AS close_time,
               first_seen_at AT TIME ZONE 'UTC' AS first_seen_at, listed_at AT TIME ZONE 'UTC' AS listed_at,
               priced_at AT TIME ZONE 'UTC' AS priced_at
        FROM v_market_latest
        JOIN (SELECT venue, market_id, min(first_seen_at) AS listed_at FROM market_versions GROUP BY ALL)
          USING (venue, market_id)
        LEFT JOIN (SELECT venue, market_id, min(ts) AS priced_at FROM v_market_prob WHERE prob IS NOT NULL GROUP BY ALL)
          USING (venue, market_id)
        ORDER BY venue, market_id""")  # fixed order, so every derived note and choice is reproducible
    cols = [d[0] for d in cur.description]
    out = []
    for row in cur.fetchall():
        m = dict(zip(cols, row))
        for k in ("open_time", "close_time", "first_seen_at", "listed_at", "priced_at"):
            m[k] = utc(m[k])
        m["text"] = " ".join(str(m[k]) for k in ("series_id", "event_title", "title", "subtitle", "tags") if m[k])
        if not EXCLUDE_REGEX.search(m["text"]):
            out.append(m)
    return out

def market_name(m):
    title, sub = m["title"] or m["event_title"] or m["market_id"], m["subtitle"]
    return f"{title} — {sub}" if sub and sub.lower() not in title.lower() else title

# ── market matching ───────────────────────────────────────────────────────────
def prices_at(con):
    """For matching's price check: a function {(market key, t)} -> {(market key, t): probability},
    the latest cleaned price at or before t (within 7 days)."""
    def lookup(requests):
        out = {}
        reqs = sorted(requests)
        for part in (reqs[i:i + PRICE_LOOKUP_BATCH] for i in range(0, len(reqs), PRICE_LOOKUP_BATCH)):
            con.register("req", pa.table({"i": list(range(len(part))), "venue": [k[0] for k, _ in part],
                                           "market_id": [k[1] for k, _ in part],
                                           "t": pa.array([t for _, t in part], pa.timestamp("ms", tz="UTC"))}))
            rows = con.execute("""
                SELECT r.i, p.prob FROM req r
                ASOF JOIN (SELECT venue, market_id, ts, prob FROM v_market_prob WHERE prob IS NOT NULL
                           AND (venue, market_id) IN (SELECT (venue, market_id) FROM req)) p
                  ON r.venue = p.venue AND r.market_id = p.market_id AND r.t >= p.ts
                WHERE p.ts > r.t - INTERVAL 7 DAY""").fetchall()
            con.unregister("req")
            out |= {part[i]: prob for i, prob in rows}
        return out
    return lookup

def fingerprint(con):
    """Everything the graph is built from: market terms, seed files and the graph code."""
    n, last = con.execute("SELECT count(*), max(first_seen_at)::VARCHAR FROM market_versions").fetchone()
    h = hashlib.sha256()
    for f in sorted([*SEEDS.glob("*.csv"), ROOT / "securities.csv",
                     *(ROOT / m for m in ("graph_build.py", "matching.py", "entities.py"))]):
        h.update(f.name.encode() + f.read_bytes())
    return f"{n}|{last}|{h.hexdigest()[:16]}"

def geopolitical(key, actors, ents):
    """Polymarket markets come from its geopolitics tag; others need a foreign state actor: one
    with a country, none of which is the US (so dual-citizen Musk is domestic)."""
    return key[0] == "polymarket" or any(
        (cs := entities.countries_of(a[3:], ents)) and HOME not in cs for a in actors)

def discover(con, markets):
    """Entities for the geopolitical markets among these (plus entities carrying exposures),
    from Wikipedia / Wikidata. ents.mentions has exactly the in-scope markets."""
    questions = {(m["venue"], m["market_id"]): matching.question_text(m) for m in markets}
    return entities.build(con, questions, extra_qids={r["qid"] for r in seed("exposures.csv")},
                          in_scope=geopolitical)

def in_scope(markets, ents):
    return [m for m in markets if (m["venue"], m["market_id"]) in ents.mentions]

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

def channel_signs(secs):
    """{ticker: {channel: sign}} for the exceptions in seeds/channel_signs.csv (default +1: an
    adverse event for an exposed entity hurts the security). A ticker row beats a sub-industry row."""
    rows = seed("channel_signs.csv")
    by_match = defaultdict(dict)
    for r in rows:
        by_match[r["match"]][r["channel"]] = float(r["sign"])
    return {s["ticker"]: by_match.get(s.get("sub_industry") or "", {}) | by_match.get(s["ticker"], {})
            for s in secs if s["ticker"] in by_match or (s.get("sub_industry") or "") in by_match}

def readable(text, ents):
    """Matcher notes name entities by Wikidata id ("entities: Q22686 vs Q58132"); show names."""
    return text and re.sub(r"\bQ\d+\b", lambda q: ents.entities.get(f"wd:{q[0]}", {}).get("name", q[0]), text)

def match_status(markets, sigs, relations, rejected, members, prop_of, ents):
    """{market key: attrs} saying how each market fared in matching, for the review tab:
    merged (shares a proposition) / linked (near_equivalent or implies another market only) /
    unmatched (parsed, but no partner) / unparsed (the parser couldn't read it), with its
    signature, parser issues and the nearest rejected candidates (other venue first)."""
    linked = {k for r, a, b, *_ in relations.values() if r != "same_as" for k in (a, b)}
    near = defaultdict(list)
    for a, b, why in rejected:
        near[a].append((b, why)); near[b].append((a, why))
    label = lambda i: ents.entities.get(i, {}).get("name", i)
    out = {}
    for m in markets:
        k, sig = (m["venue"], m["market_id"]), sigs.get((m["venue"], m["market_id"]))
        status = ("merged" if len(members[prop_of[market_id(m)]]) > 1 else "linked" if k in linked
                  else "unparsed" if not (sig and sig.parsed) else "unmatched")
        cands = sorted(near[k], key=lambda c: (c[0][0] == k[0], c[0]))[:3]
        out[k] = dict(match=status, match_issues=list(sig.issues) if sig else [],
                      signature=sig and dict(
                          action=sig.action, actors=sorted(map(label, sig.actors)),
                          targets=sorted(map(label, sig.targets)), concepts=sorted(s for _, s in sig.concepts),
                          window=sig.window and [str(d) if d else None for d in sig.window],
                          threshold=sig.threshold and list(sig.threshold), conditions=sorted(sig.conditions),
                          negated=sig.negated),
                      candidates=[dict(market=market_id(dict(venue=v, market_id=i)), reason=readable(why, ents))
                                  for (v, i), why in cands])
    return out

# ── desired graph ─────────────────────────────────────────────────────────────
def build(markets, prices, ents):
    nodes, edges = {}, {}
    add_n = lambda n: nodes.__setitem__(n["node_id"], n)
    add_e = lambda e: edges.__setitem__(e["edge_id"], e)

    # markets -> propositions, by contract signature (+ overrides)
    key = lambda m: (m["venue"], m["market_id"])
    by_key = {key(m): m for m in markets}
    sigs, matches, rejected = match(markets, prices, ents)
    relations = resolve(matches, by_key)
    keyed = group(markets, relations)
    prop_of = {market_id(m): keyed[key(m)] for m in markets}
    joined = defaultdict(list)  # notes of the same_as matches that joined each market
    for r, a, b, _, prov, note in relations.values():
        if r == "same_as":
            for k in (a, b): joined[k].append((prov, note))
    members = defaultdict(list)
    for m in markets:
        members[prop_of[market_id(m)]].append(m)
    status = match_status(markets, sigs, relations, rejected, members, prop_of, ents)
    for m in markets:
        pid, start = prop_of[market_id(m)], m["open_time"] or m["first_seen_at"]
        add_n(node(market_id(m), "market", market_name(m), start, venue=m["venue"],
                   market_id=m["market_id"], event_id=m["event_id"], series_id=m["series_id"],
                   close_time=m["close_time"], rules=m["rules_primary"], **status[key(m)]))
        how = joined[key(m)]
        add_e(edge(market_id(m), pid, "same_as", start,
                   provenance="curated" if any(p == "curated" for p, _ in how) else "signature" if how else "rule",
                   note=readable("; ".join(sorted({n for _, n in how if n})), ents) or None))

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
                   confidence=conf, provenance=prov, note=readable(note, ents)))

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
        # Ladders link every pair of rungs, not just neighbours, so a new rung only adds links
        # (a chain would retract "by 2027 => by 2028" when "by mid-2027" is listed).
        # strike: a higher floor implies a lower floor; a lower cap implies a higher cap
        for kinds, key, desc in ((GREATER, "floor_strike", True), (LESS, "cap_strike", False)):
            lad = sorted(((m[key], p) for p, m in pids.items()
                          if m["strike_type"] in kinds and m[key] is not None), reverse=desc)
            for (va, a), (vb, b) in itertools.combinations(lad, 2):
                if va != vb:
                    add_e(edge(a, b, "implies", start(a, b), ladder="strike", event_id=event_id))
        # date: an earlier deadline implies a later one
        if all(m["subtitle"] and DATE_SUBTITLE.search(m["subtitle"]) for m in pids.values()):
            lad = sorted((m["close_time"], p) for p, m in pids.items() if m["close_time"])
            for (ta, a), (tb, b) in itertools.combinations(lad, 2):
                if ta < tb:
                    add_e(edge(a, b, "implies", start(a, b), ladder="date", event_id=event_id))

    # entities, securities, curated links and exposures
    # entities (discovered from the markets), securities, and the links between them
    for eid, e in ents.entities.items():
        add_n(node(eid, "entity", e["name"], REFERENCE_VALID_FROM, entity_type=e["type"], qid=e["qid"],
                   description=e["description"], wikipedia=e["title"]))
    with open(ROOT / "securities.csv") as f:
        secs = list(csv.DictReader(f))
    for s in secs:
        add_n(node(f"sec:{s['ticker']}", "security", s["name"], REFERENCE_VALID_FROM, ticker=s["ticker"],
                   **{k: s.get(k) or None for k in ("asset_class", "country", "role", "sector",
                                                   "sub_industry", "issuer_qid", "index")}))
    signs = channel_signs(secs)
    for a, b, kind, w in ents.links:
        add_e(edge(a, b, kind, REFERENCE_VALID_FROM, weight=w or 0.0, confidence=0.9 if kind != "related" else 0.6,
                   provenance="wikidata" if kind != "related" else "wikipedia_cooccurrence"))
    for r in seed("exposures.csv"):
        add_e(edge(f"wd:{r['qid']}", f"sec:{r['ticker']}", "exposure", REFERENCE_VALID_FROM,
                   weight=float(r["weight"]), confidence=0.3, provenance="analyst_prior",
                   note=r["rationale"] or None, sign=1.0, channel_signs=signs.get(r["ticker"], {})))

    # proposition -> entity: the Wikipedia-linked mentions in its markets' questions, signed by
    # what YES means for each (role in the contract signature x action effect). The first parsed
    # market (earliest listed) speaks for the proposition, so the edge never changes as others join.
    for pid, ms in members.items():
        seen = {}
        for m in ms:
            for mt in ents.mentions.get((m["venue"], m["market_id"]), []):
                seen.setdefault(mt["id"], mt)
        ordered = sorted(ms, key=lambda m: (m["open_time"] or m["first_seen_at"] or REFERENCE_VALID_FROM,
                                            m["venue"], m["market_id"]))
        msigs = [sigs[(m["venue"], m["market_id"])] for m in ordered]
        sig = next((s for s in msigs if s.action), msigs[0])
        for eid, mt in seen.items():
            role, channel, weight, sign = matching.effect(sig, eid)
            add_e(edge(pid, eid, "involves", prop_start[pid], weight=weight, sign=sign,
                       confidence=0.8 if mt["named"] else 0.6, provenance="wikipedia_link",
                       matched=mt["surface"], role=role, action=sig.action, channel=channel))

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
    inputs = fingerprint(con)
    if entities.REFRESH:  # a refresh always rebuilds, so changed facts reach the graph
        stats["refreshed"] = sorted(entities.REFRESH)
        last = None
    else:
        last = con.execute("""SELECT stats->>'inputs' FROM collection_runs WHERE job = 'graph' AND status = 'ok'
                              ORDER BY started_at DESC LIMIT 1""").fetchone()
    if last and last[0] == inputs:
        stats |= {"inputs": inputs, "skipped": "no new markets, terms, seeds or code since last build"}
        return True
    markets = load_markets(con)
    ents = discover(con, markets)
    nodes, edges = build(in_scope(markets, ents), prices_at(con), ents)
    t = datetime.now(timezone.utc)
    node_rows = sync(con, "graph_nodes", "node_id", nodes, run_id, t)
    edge_rows = sync(con, "graph_edges", "edge_id", edges, run_id, t)
    con.begin()
    insert(con, "graph_nodes", node_rows)
    insert(con, "graph_edges", edge_rows)
    con.commit()
    stats |= {"inputs": inputs, "nodes": Counter(n["node_type"] for n in nodes.values()),
              "edges": Counter(e["edge_type"] for e in edges.values()),
              "node_changes": len(node_rows), "edge_changes": len(edge_rows)}
    return True

# ── review ────────────────────────────────────────────────────────────────────
def review():
    """CSV for human review: algorithmic matches, rejected candidates with the reason, and
    markets the parser couldn't read. Copy a row into seeds/match_overrides.csv to override."""
    con = connect()
    markets, prices = load_markets(con), prices_at(con)
    ents = discover(con, markets)
    markets = in_scope(markets, ents)
    sigs, matches, rejected = match(markets, prices, ents)
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
    elif "--refresh" in sys.argv:
        what = sys.argv[sys.argv.index("--refresh") + 1:][:1]
        if not what or what[0] not in REFRESH_KINDS:
            sys.exit(f"usage: graph_build.py --refresh {{{'|'.join(REFRESH_KINDS)}}}")
        entities.REFRESH |= REFRESH_KINDS[what[0]]
        sys.exit(run_job("graph", graph))
    else:
        sys.exit(run_job("graph", graph))
