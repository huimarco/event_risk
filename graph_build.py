#!/usr/bin/env python3
"""
Build the knowledge graph from collected contract terms, Wikipedia / Wikidata, and the curated
inputs (entity -> issuer exposures and their channel signs) into graph_nodes / graph_edges. Bitemporal and append-only: each run diffs the desired graph against
the current one and records only changes (new or changed -> 'assert', gone -> 'retract').

Nodes: market, proposition, question, entity, issuer, security, factor
Edges:
  market -> proposition       same_as           markets with matching contract signatures
                                                (matching.py) form one proposition
  proposition -> proposition  near_equivalent   same event, materially different terms
                              implies           signature windows/thresholds nest, or
                                                date/strike ladders within an event
                              mutex             mutually exclusive events (venue flags)
  proposition -> question     rung_of           propositions whose signatures differ only in
                                                deadline (or, per measurement period, in
                                                threshold) are rungs of one question
                                                (matching.ladder_key, questions())
  question -> entity          involves          phrases in market titles linked to Wikipedia
                                                articles (entities.py); entities are
                                                regenerated from the markets on every run.
                                                Signed: role (actor / target / party / concept),
                                                channel, weight = severity x role, sign = what
                                                YES means for the entity (matching.EFFECTS)
  question -> factor          relevant_to       market factors (country equities, FX, oil, rates, ...) the
                                                question matters to; qualitative, no weight or sign
                                                (factors.py, seeds/entity_factors.csv)
  entity -> entity            citizen_of, ...   Wikidata statements between entities
                              related           Wikipedia co-occurrence (entities.py)
  entity -> issuer            exposure          seeds/exposures.csv — analyst priors until event
                                                betas; per-channel signs (which issuers gain
                                                from escalation) from seeds/channel_signs.csv;
                                                plus entity -> the same company as issuer
                                                (weight 1) when the issuer is itself an entity
  issuer -> security          issued            securities.csv: which issuer issued each ticker
                                                (reference data). How much a security moves with
                                                a question or factor is measured, not stored here:
                                                security_betas.py -> security_loadings /
                                                security_betas, attached by the API
Human overrides of market matches (seeds/match_overrides.csv) always win over the algorithm.
Market nodes carry their match outcome (merged / linked / unmatched / unparsed, with the parsed
signature, parser issues and the nearest rejected candidates) for the review tab in the web app.

Candidates (GRAPH_SOURCES): the collector stores far more markets than the graph uses. The graph
reads its core sources (Kalshi World / Politics, Polymarket geopolitics) and, for events that
have traded at least SCOPED_MIN_VOLUME, its scoped sources (Kalshi Elections / Economics;
Polymarket world, world-elections, tariffs, fed-rates, economic-policy). Widening the graph means
adding a source here (and a theme to scope()), not collecting more.

Scope (scope()): geopolitical and credit-relevant policy markets (minus EXCLUDE_REGEX noise).
Polymarket's geopolitics tag is the venue's own classification; any other market is in scope when
it involves a foreign state actor — a country other than the US, or a person/organisation
belonging to one (Wikidata parents) — so "Trump visits China" is in and "Senate confirms X" is out,
or when its question is about a policy topic that moves credit and rates (POLICY_REGEX: Fed rates,
defaults and restructurings, IMF programmes, government shutdowns, debt ceilings, tariffs,
sanctions), US-only or not. The collector applies the same test to its extra categories and tags.

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
# Which collected markets the graph considers: Kalshi by category (stored as the market's tag;
# markets collected before 2026-10-27 have none and came from the core categories), Polymarket by
# event tag. Scoped sources also need SCOPED_MIN_VOLUME traded across the event (drops US House,
# provincial and mayoral races) — and every candidate must still pass scope().
GRAPH_SOURCES = dict(kalshi_core={"World", "Politics"}, kalshi_scoped={"Elections", "Economics", "Science and Technology"},
                     poly_core={"geopolitics"},
                     poly_scoped={"world", "world-elections", "tariffs", "fed-rates", "economic-policy", "ai", "compute", "ai-regulation"})
SCOPED_MIN_VOLUME = 25000  # $ lifetime volume across the event
# Policy topics that move credit and rates: in scope even when only the US is involved.
POLICY_REGEX = re.compile(r"fed(?:eral)? funds|\bFOMC\b|\bFed\b.*\brates?\b|\brate (?:cut|hike)s?\b|\bFed decision"
                          r"|\bdefaults?\b|debt restructuring|\bIMF\b|\bbailout\b|government shutdown"
                          r"|debt (?:ceiling|limit)|\btariff|\bsanction", re.I)
# Data-centre / AI-infrastructure topics (decision 2026-10-28, option A): in scope US-only or not,
# like POLICY_REGEX. Kalshi Science & Technology and Polymarket ai / compute / ai-regulation are
# scoped sources for them.
THEME_REGEX = re.compile(r"data ?cent(?:er|re)s?|datacenter|hyperscal|\bAI[- ]chips?\b|chip exports?|export controls?"
                         r"|semiconductor|\bGPUs?\b|\bHBM\b|stargate|\bAI (?:infrastructure|compute|capex|moratorium|preemption)", re.I)
PRICE_LOOKUP_BATCH = 5000  # (market, time) requests per query; one huge ASOF join times out on MotherDuck
REFRESH_KINDS = {"wikidata": {"entity"}, "relatedness": {"mentions", "comentions", "df"}}  # wiki_cache kinds

def seed(name):
    with open(SEEDS / name) as f:
        return [r for r in csv.DictReader(f) if not next(iter(r.values()), "").startswith("#")]

# Hand-made themes (decision 2026-10-28, by request): entities the Wikipedia linker can't produce,
# attached to markets by one reasoning pass (seeds/theme_links.csv) and to issuers by analyst priors
# (seeds/theme_exposures.csv). Every linked market is a graph candidate, in scope whatever its
# category, volume or EXCLUDE_REGEX.
THEMES = {r["theme_id"]: r for r in seed("themes.csv")}
THEME_LINKS = defaultdict(dict)   # (venue, market_id) -> {theme id: link row}
for r in seed("theme_links.csv"):
    assert r["theme_id"] in THEMES, f"theme_links.csv: unknown theme {r['theme_id']}"
    THEME_LINKS[tuple(r["market"].split(":", 1))][r["theme_id"]] = r
THEME_MARKETS = set(THEME_LINKS)
THEME_WEIGHT = {3: 1.0, 2: 0.6, 1: 0.3}   # involves weight by link strength

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

CANDIDATE_SQL = """(venue = 'kalshi' AND (tags IS NULL OR list_has_any(from_json(tags, '["VARCHAR"]'), $kc)))
                  OR (venue = 'polymarket' AND list_has_any(from_json(tags, '["VARCHAR"]'), $pc))"""

def source_params():
    g = GRAPH_SOURCES
    return dict(kc=sorted(g["kalshi_core"] | g["kalshi_scoped"]), pc=sorted(g["poly_core"] | g["poly_scoped"]),
                tm=sorted(f"{v}:{m}" for v, m in THEME_MARKETS))
THEMED_SQL = "venue || ':' || market_id IN (SELECT unnest($tm::VARCHAR[]))"

def source_class(m):
    """core | scoped | None for a market, from its venue tags (GRAPH_SOURCES)."""
    tags = set(json.loads(m["tags"])) if m["tags"] else None
    g = GRAPH_SOURCES
    if m["venue"] == "kalshi":
        return "core" if tags is None or tags & g["kalshi_core"] else "scoped" if tags & g["kalshi_scoped"] else None
    return "core" if tags and tags & g["poly_core"] else "scoped" if tags and tags & g["poly_scoped"] else None

def load_markets(con):
    """Latest terms of candidate markets (GRAPH_SOURCES; geopolitical scope is decided after entity
    discovery), when each was first listed, and when it first had a price. Scoped sources keep only
    events that have traded SCOPED_MIN_VOLUME (latest lifetime volume per market, summed). Timestamps
    naive UTC."""
    event_volume = defaultdict(float)
    for venue, event_id, v in con.execute(f"""
        SELECT l.venue, l.event_id, s.volume FROM v_market_latest l
        JOIN (SELECT venue, market_id, arg_max(volume, ts) AS volume FROM market_snapshots
              WHERE volume IS NOT NULL GROUP BY ALL) s USING (venue, market_id)
        WHERE ({CANDIDATE_SQL}) OR {THEMED_SQL}""", source_params()).fetchall():
        event_volume[(venue, event_id)] += v or 0
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
        WHERE (""" + CANDIDATE_SQL + ") OR " + THEMED_SQL + """
        ORDER BY venue, market_id""", source_params())  # fixed order, so every derived note and choice is reproducible
    cols = [d[0] for d in cur.description]
    out = []
    for row in cur.fetchall():
        m = dict(zip(cols, row))
        cls, themed = source_class(m), (m["venue"], m["market_id"]) in THEME_MARKETS
        if not themed and (not cls or (cls == "scoped" and event_volume[(m["venue"], m["event_id"])] < SCOPED_MIN_VOLUME)): continue
        for k in ("open_time", "close_time", "first_seen_at", "listed_at", "priced_at"):
            m[k] = utc(m[k])
        m["text"] = " ".join(str(m[k]) for k in ("series_id", "event_title", "title", "subtitle", "tags") if m[k])
        if themed or not EXCLUDE_REGEX.search(m["text"]):
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
    """Everything the graph is built from: candidate markets' terms (GRAPH_SOURCES; markets the
    graph never reads don't trigger a rebuild), seed files and the graph code."""
    n, last = con.execute(f"SELECT count(*), max(first_seen_at)::VARCHAR FROM market_versions WHERE ({CANDIDATE_SQL}) OR {THEMED_SQL}",
                          source_params()).fetchone()
    h = hashlib.sha256()
    for f in sorted([*SEEDS.glob("*.csv"), ROOT / "issuers.csv", ROOT / "securities.csv",
                     *(ROOT / m for m in ("graph_build.py", "matching.py", "entities.py", "factors.py"))]):
        h.update(f.name.encode() + f.read_bytes())
    # turning Wikipedia fetching on rebuilds, so lookups skipped while it was off get made
    return f"{n}|{last}|{h.hexdigest()[:16]}|wiki={'on' if entities.WIKI_FETCH else 'off'}"

def geopolitical(key, actors, ents):
    """A foreign state actor: one with a country, none of which is the US (so dual-citizen Musk is
    domestic)."""
    return any((cs := entities.countries_of(a[3:], ents)) and HOME not in cs for a in actors)

def scope(texts, tags=None):
    """in_scope(key, actor ids, ents) for entities.discover over {key: text}: Polymarket's
    geopolitics tag, a policy topic (POLICY_REGEX) in the text, or a foreign state actor.
    tags = {key: [tag slugs]}; without it every Polymarket market counts as tagged (callers that
    only sweep the geopolitics tag)."""
    def in_scope(key, actors, ents):
        if key[0] == "polymarket" and tags is None: return True
        if key[0] == "polymarket" and "geopolitics" in tags.get(key, ()): return True
        if key in THEME_MARKETS: return True   # curated theme links pull their markets in (seeds/theme_links.csv)
        text = texts.get(key) or ""
        return bool(POLICY_REGEX.search(text) or THEME_REGEX.search(text)) or geopolitical(key, actors, ents)
    return in_scope

def discover(con, markets):
    """Entities for the geopolitical markets among these (plus entities carrying exposures),
    from Wikipedia / Wikidata. ents.mentions has exactly the in-scope markets."""
    questions = {(m["venue"], m["market_id"]): matching.question_text(m) for m in markets}
    tags = {(m["venue"], m["market_id"]): json.loads(m["tags"]) if m["tags"] and m["venue"] == "polymarket" else []
            for m in markets}
    return entities.build(con, questions, extra_qids={r["qid"] for r in seed("exposures.csv")},
                          in_scope=scope(questions, tags))

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

def group(markets, relations, sigs):
    """{market key: proposition id}. same_as pairs are unioned; a proposition is named after its
    earliest-listed market, so its id stays stable as later markets join.

    Matching is pairwise and allows a day of slack across venues, so unions can chain: Kalshi
    "before Jul 21" = Polymarket "by Jul 20", and = Polymarket "by Jul 19", would put two
    different Polymarket deadlines in one proposition. A union is therefore refused if it would
    join two markets of the same venue with different deadlines (window ends); the pair becomes
    near_equivalent (rewritten in `relations`). Same-venue markets whose windows differ only in
    start (relisted later; the matcher verified the elapsed time by price) still merge. Curated overrides always merge. Pairs are unioned in key order,
    curated first, so the outcome is reproducible."""
    parent = {k: k for k in (( m["venue"], m["market_id"]) for m in markets)}
    members = {k: [k] for k in parent}
    find = lambda k: k if parent[k] == k else find(parent[k])
    deadline = lambda k: sigs[k].window[1] if k in sigs and sigs[k].window else None
    def clash(ra, rb):
        return next(((x, y) for x in members[ra] for y in members[rb]
                     if x[0] == y[0] and deadline(x) != deadline(y)), None)
    for pair, (r, a, b, conf, prov, note) in sorted(relations.items(),
                                                    key=lambda kv: (kv[1][4] != "curated", sorted(kv[0]))):
        if r != "same_as": continue
        ra, rb = find(a), find(b)
        if ra == rb: continue
        if prov != "curated" and (c := clash(ra, rb)):
            why = (f"not merged: would put two {c[0][0]} markets with different deadlines "
                   f"({c[0][1]}, {c[1][1]}) in one proposition")
            relations[pair] = ("near_equivalent", a, b, conf, prov, "; ".join(filter(None, (why, note))))
            continue
        parent[ra] = rb
        members[rb] += members.pop(ra)
    first = {(m["venue"], m["market_id"]): (m["open_time"] or m["first_seen_at"] or REFERENCE_VALID_FROM,
                                             m["venue"], m["market_id"]) for m in markets}
    groups = defaultdict(list)
    for k in parent: groups[find(k)].append(k)
    prop_of = {}
    for ks in groups.values():
        anchor = min(ks, key=first.get)
        for k in ks: prop_of[k] = f"prop:{anchor[0]}:{anchor[1]}"
    return prop_of

def channel_signs(issuers):
    """{issuer_id: {channel: sign}} for the exceptions in seeds/channel_signs.csv (default +1: an
    adverse event for an exposed entity hurts the issuer). An issuer row beats a sub-industry row."""
    rows = seed("channel_signs.csv")
    by_match = defaultdict(dict)
    for r in rows:
        by_match[r["match"]][r["channel"]] = float(r["sign"])
    return {i["issuer_id"]: by_match.get(i.get("sub_industry") or "", {}) | by_match.get(i["issuer_id"], {})
            for i in issuers if i["issuer_id"] in by_match or (i.get("sub_industry") or "") in by_match}

def issuer_node_id(issuer_id):
    return f"iss:{issuer_id}"

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

def questions(members, psig, prop_start):
    """{question id: [proposition ids, first-listed first]}. Propositions sharing a ladder key
    (matching.ladder_key) are rungs of one question. Within one venue, two propositions with the same
    window and threshold are different outcomes ("Person O" / "Person P" as Iran's head of state)
    or different rules, not rungs: each becomes a question of its own. A question is named and
    identified after its first-listed rung, so its id stays stable as rungs are added."""
    first = lambda p: (prop_start[p] or REFERENCE_VALID_FROM, p)
    groups = defaultdict(list)
    for pid in sorted(members, key=first):
        k = matching.ladder_key(psig[pid])
        groups[k if k else ("single", pid)].append(pid)
    out = {}
    for k, pids in groups.items():
        venue = lambda p: members[p][0]["venue"]
        slot = Counter((venue(p), psig[p].window, psig[p].threshold) for p in pids)
        clash = [p for p in pids if len(pids) > 1 and slot[(venue(p), psig[p].window, psig[p].threshold)] > 1]
        for p in clash:
            out[f"q:{p[5:]}"] = [p]
        rest = [p for p in pids if p not in clash]
        if rest:
            out[f"q:{rest[0][5:]}"] = rest
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
    keyed = group(markets, relations, sigs)
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

    # entities (discovered from the markets), hand-made themes, issuers, and the links between them
    for tid, t in THEMES.items():
        add_n(node(f"theme:{tid}", "entity", t["name"], REFERENCE_VALID_FROM, entity_type="theme", qid=None,
                   description=t["definition"], wikipedia=None, region=t["region"]))
    for eid, e in ents.entities.items():
        add_n(node(eid, "entity", e["name"], REFERENCE_VALID_FROM, entity_type=e["type"], qid=e["qid"],
                   description=e["description"], wikipedia=e["title"]))
    issuers, i_nodes, i_edges = issuer_layer(set(ents.entities))
    for n in i_nodes: add_n(n)
    for e in i_edges: add_e(e)
    for a, b, kind, w in ents.links:
        add_e(edge(a, b, kind, REFERENCE_VALID_FROM, weight=w or 0.0, confidence=0.9 if kind != "related" else 0.6,
                   provenance="wikidata" if kind != "related" else "wikipedia_cooccurrence"))

    # propositions -> questions (ladders), then question -> entity
    listed = lambda m: (m["open_time"] or m["first_seen_at"] or REFERENCE_VALID_FROM, m["venue"], m["market_id"])
    psig = {}  # the first parsed market (earliest listed) speaks for its proposition
    for pid, ms in members.items():
        msigs = [sigs[(m["venue"], m["market_id"])] for m in sorted(ms, key=listed)]
        psig[pid] = next((s for s in msigs if s.action), msigs[0])
    for qid, rungs in questions(members, psig, prop_start).items():
        sig = psig[rungs[0]]
        q_start = min(filter(None, (prop_start[p] for p in rungs)), default=None)
        closes = [nodes[p]["attrs"]["close_time"] for p in rungs if nodes[p]["attrs"]["close_time"]]
        add_n(node(qid, "question", matching.question_name(nodes[rungs[0]]["name"], sig), q_start,
                   kind="single" if len(rungs) == 1 else "thresholds" if sig.threshold else "deadlines",
                   n_rungs=len(rungs), close_time=max(closes) if closes else None,
                   venues=sorted({v for p in rungs for v in nodes[p]["attrs"]["venues"]})))
        for p in rungs:
            w = psig[p].window
            add_e(edge(p, qid, "rung_of", prop_start[p], window=w and [str(d) if d else None for d in w],
                       threshold=psig[p].threshold and list(psig[p].threshold)))
        # question -> entity: the Wikipedia-linked mentions in its markets' questions, signed by
        # what YES means for each (role in the contract signature x action effect), from the
        # first rung's signature (all rungs share it but for deadline / threshold)
        seen = {}
        for p in rungs:
            for m in sorted(members[p], key=listed):
                for mt in ents.mentions.get((m["venue"], m["market_id"]), []):
                    seen.setdefault(mt["id"], mt)
        for eid, mt in seen.items():
            role, channel, weight, sign = matching.effect(sig, eid)
            add_e(edge(qid, eid, "involves", q_start, weight=weight, sign=sign,
                       confidence=0.8 if mt["named"] else 0.6, provenance="wikipedia_link",
                       matched=mt["surface"], role=role, action=sig.action, channel=channel))
        # question -> theme: curated reasoning links on any of its markets; the strongest wins
        themed = {}
        for p in rungs:
            for m in members[p]:
                for tid, r in THEME_LINKS.get((m["venue"], m["market_id"]), {}).items():
                    if tid not in themed or int(r["strength"]) > int(themed[tid]["strength"]): themed[tid] = r
        for tid, r in sorted(themed.items()):
            add_e(edge(qid, f"theme:{tid}", "involves", q_start, weight=THEME_WEIGHT[int(r["strength"])],
                       sign=float(r["sign"]) if int(r["sign"]) else None, confidence=0.5, provenance="curated",
                       note=r["rationale"], role="concept", action=sig.action, channel="theme",
                       strength=int(r["strength"]), basis=r["source"]))

    # factors, GICS industries and their links (factor_layer)
    citizen_of = defaultdict(list)
    for a, b, kind, _ in ents.links:
        if kind == "citizen_of": citizen_of[a].append(b)
    involved = defaultdict(list)
    for e in edges.values():
        if e["edge_type"] == "involves":
            a = e["attrs"]
            involved[e["src_id"]].append((e["dst_id"], a.get("role"), a.get("action"), a.get("channel")))
    f_nodes, f_edges = factor_layer(issuers, involved, citizen_of, lambda x: nodes[x]["name"],
                                    lambda q: nodes[q]["valid_from"] if q in nodes else False)
    for n in f_nodes: add_n(n)
    for e in f_edges: add_e(e)

    dangling = [e for e in edges.values() if e["src_id"] not in nodes or e["dst_id"] not in nodes]
    for e in dangling:
        log.warning("dropping edge with unknown endpoint: %s", e["edge_id"])
        del edges[e["edge_id"]]
    return nodes, edges

def issuer_layer(entity_ids):
    """Issuers (issuers.csv), securities (securities.csv) and their edges: entity -> issuer
    `exposure` (seeds/exposures.csv, plus weight 1 from an entity that is the issuer itself) and
    issuer -> security `issued`. entity_ids = the graph's entity node ids. Returns (issuers rows,
    nodes, edges)."""
    nodes, edges = [], {}
    with open(ROOT / "issuers.csv") as f:
        issuers = list(csv.DictReader(f))
    for i in issuers:  # reference_ticker stays out of the graph: it is only for backtests
        nodes.append(node(issuer_node_id(i["issuer_id"]), "issuer", i["name"], REFERENCE_VALID_FROM,
                          qid=None if i["issuer_id"].startswith("local:") else i["issuer_id"],
                          **{k: i.get(k) or None for k in ("country", "sector", "sub_industry", "headquarters", "lei", "index")}))
    signs = channel_signs(issuers)
    add = lambda e: edges.setdefault(e["edge_id"], e)
    for r in seed("exposures.csv"):
        add(edge(f"wd:{r['qid']}", issuer_node_id(r["issuer_id"]), "exposure", REFERENCE_VALID_FROM,
                 weight=float(r["weight"]), confidence=0.3, provenance="analyst_prior",
                 note=r["rationale"] or None, sign=1.0, channel_signs=signs.get(r["issuer_id"], {})))
    issuer_ids = {i["issuer_id"] for i in issuers}
    for r in seed("theme_exposures.csv"):   # hand-made theme -> issuer exposures (seeds/themes.csv)
        assert r["theme_id"] in THEMES and r["issuer_id"] in issuer_ids, f"theme_exposures.csv: bad row {r}"
        add(edge(f"theme:{r['theme_id']}", issuer_node_id(r["issuer_id"]), "exposure", REFERENCE_VALID_FROM,
                 weight=float(r["weight"]), confidence=0.3, provenance="analyst_prior",
                 note=r["rationale"] or None, sign=1.0, channel_signs=signs.get(r["issuer_id"], {})))
    with open(ROOT / "securities.csv") as f:
        for x in csv.DictReader(f):
            nodes.append(node(f"sec:{x['ticker']}", "security", x["name"], REFERENCE_VALID_FROM, ticker=x["ticker"],
                              **{k: x.get(k) or None for k in ("asset_class", "country", "index")}, primary=bool(x["primary"])))
            if x["issuer_id"]:
                add(edge(issuer_node_id(x["issuer_id"]), f"sec:{x['ticker']}", "issued", REFERENCE_VALID_FROM,
                         provenance="reference"))
    for i in issuers:  # an entity that is itself the issuer (TSMC mentioned in a question -> TSMC the issuer)
        src, dst = f"wd:{i['issuer_id']}", issuer_node_id(i["issuer_id"])
        if src in entity_ids:
            add(edge(src, dst, "exposure", REFERENCE_VALID_FROM, weight=1.0, confidence=1.0, provenance="rule",
                     note="the issuer itself", sign=1.0, channel_signs=signs.get(i["issuer_id"], {})))
    return issuers, nodes, list(edges.values())

def factor_layer(issuers, involved, citizen_of, name_of, start_of):
    """Factor nodes (macro and niche), GICS industry nodes and their edges, from the seeds:
    factor -> factor `drives`, niche factor -> industry `driver_of`, issuer -> industry
    `classified_as`, question -> factor `relevant_to` (factors.derive over each question's involves
    edges, plus curated rows of seeds/question_links.csv) and question -> industry / issuer `affects`
    (curated only). Curated factor links join a rule link as an extra path (channel "curated", sign =
    what YES means for the factor) or stand alone with provenance "curated". involved = {question id: [(entity id, role, action, channel)]}, citizen_of = {entity id:
    [country ids]}, name_of(node id) / start_of(question id) from the rest of the graph (start_of
    returns False for a question not in it). Returns (nodes, edges)."""
    import factors  # here, not at the top: factors reads seeds through this module
    nodes, edges = [], []
    for fid, f in factors.CATALOG.items():
        nodes.append(node(f"factor:{fid}", "factor", f["name"], REFERENCE_VALID_FROM, layer=f["layer"], family=f["family"],
                          definition=f["definition"], **({"side": f["side"], "earnings_sign": f["earnings_sign"]} if f["layer"] == "niche" else {})))
    for src, dst, sign, why in factors.LINKS:
        edges.append(edge(f"factor:{src}", f"factor:{dst}", "drives", REFERENCE_VALID_FROM, weight=None, confidence=0.3,
                          provenance="analyst_prior", note=why, sign=float(sign)))
    for code, ind in factors.INDUSTRIES.items():
        nodes.append(node(f"gics:{code}", "industry", ind["name"], REFERENCE_VALID_FROM, code=code, industry_group=ind["industry_group"],
                          sector=ind["sector"], sub_industries=ind["sub_industries"]))
    for fid, f in factors.CATALOG.items():
        for code, subs in factors.industries_of(fid).items():
            edges.append(edge(f"factor:{fid}", f"gics:{code}", "driver_of", REFERENCE_VALID_FROM, weight=None, confidence=0.3,
                              provenance="analyst_prior", sign=float(f["earnings_sign"]), side=f["side"], sub_industries=subs))
    for i in issuers:
        sub = factors.SUB_BY_NAME.get(i.get("sub_industry") or "")
        if sub:
            edges.append(edge(issuer_node_id(i["issuer_id"]), f"gics:{sub['industry_code']}", "classified_as", REFERENCE_VALID_FROM,
                              provenance="reference", sub_industry=sub["sub_industry"], sub_industry_code=sub["sub_industry_code"]))
    curated = defaultdict(list)  # (question, factor) -> curated paths
    issuer_ids = {issuer_node_id(i["issuer_id"]) for i in issuers}
    for r in factors.QUESTION_LINKS:
        q, kind, target, sign = r["question_id"], r["target_type"], r["target_id"], int(r["sign"] or 0) or None
        if start_of(q) is False: continue   # question not in this graph
        if kind == "factor":
            curated[(q, target)].append(dict(nodes=[], role=None, channel="curated", sign=sign, rationale=r["rationale"]))
        else:
            dst = f"gics:{target}" if kind == "industry" else issuer_node_id(target)
            if kind == "issuer" and dst not in issuer_ids: continue
            edges.append(edge(q, dst, "affects", start_of(q), weight=None, confidence=0.5, provenance="curated",
                              note=r["rationale"], sign=float(sign) if sign else None))
    for qid, inv in sorted(involved.items()):
        for fid, paths in sorted(factors.derive(inv, citizen_of).items()):
            paths += curated.pop((qid, fid), [])
            edges.append(edge(qid, f"factor:{fid}", "relevant_to", start_of(qid), weight=None, confidence=0.3,
                              provenance="analyst_prior", paths=paths,
                              note="; ".join(dict.fromkeys((" -> ".join(name_of(x) for x in p["nodes"]) or p["rationale"]) + f" ({p['channel']})"
                                                           for p in paths))))
    for (qid, fid), paths in sorted(curated.items()):
        edges.append(edge(qid, f"factor:{fid}", "relevant_to", start_of(qid), weight=None, confidence=0.5,
                          provenance="curated", paths=paths, note="; ".join(p["rationale"] for p in paths)))
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
    stats |= {"inputs": inputs, "wiki_not_looked_up": dict(entities.SKIPPED), "nodes": Counter(n["node_type"] for n in nodes.values()),
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
        if not entities.WIKI_FETCH:
            sys.exit("--refresh re-fetches from Wikipedia / Wikidata: set WIKI_FETCH=1 to allow it")
        entities.REFRESH |= REFRESH_KINDS[what[0]]
        sys.exit(run_job("graph", graph))
    else:
        sys.exit(run_job("graph", graph))
