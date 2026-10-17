"""
Entities discovered from market text and connected through Wikipedia / Wikidata — no hand-made
entity lists or links.

  link        phrases in each market question -> Wikipedia articles -> Wikidata items.
              Longest match wins; disambiguation pages are skipped. A lone word with no article of
              its own is a surname of an already-linked person ("Xi") or a country's demonym in
              Wikidata ("Chinese" -> China). Wikidata then decides what
              each one is: people, countries and organisations are actors; everything else is a
              concept, kept only if specific — in < CONCEPT_MAX_DF of Wikipedia articles
              ("tariff", "hukou", "ballistic missile" yes; "local government", "company" no).
  describe    Wikidata label, description, type, parent (citizenship for people, country for
              everything else — so Trump -> United States) and statements between entities.
  relatedness Normalized co-occurrence (Cilibrasi & Vitanyi 2007; the same formula as Milne &
              Witten 2008 on links): two entities are related when many articles mention both,
                1 - (log max(|A|,|B|) - log |A∩B|) / (log W - log min(|A|,|B|))
              with A, B the sets of articles mentioning each and W the number of articles.
              Mentions in prose, not links: navigation templates put unrelated companies on
              hundreds of shared pages (WuXi Biologics & TSMC: 332 shared links, 1 shared
              mention). Fewer than MIN_COOCCURRENCE shared articles is too little evidence.

Every lookup is cached in MotherDuck (wiki_cache), so each phrase, entity and pair is fetched once.
Refreshing a kind (REFRESH, set by `graph_build.py --refresh`) re-fetches it and appends the new
values; the latest row wins and old values stay as history.
"""
import json, math, re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from collector import chunks, get, insert, log, now

WIKI = "https://en.wikipedia.org/w/api.php"
WIKIDATA = "https://www.wikidata.org/w/api.php"
SPARQL = "https://query.wikidata.org/sparql"
UA = {"User-Agent": "event-risk/0.1 (https://github.com/huimarco/event_risk)"}
CONCEPT_MAX_DF = 0.01      # single lowercase words: concept if in < 1% of articles
MIN_RELATEDNESS = 0.25     # keep entity <-> entity links at or above this
MIN_COOCCURRENCE = 10      # ... and only with at least this many articles mentioning both
REFRESH = set()            # cache kinds to re-fetch this run, e.g. {"entity"}
_refreshed = {}            # kind -> keys already re-fetched this run
# Wikidata statements kept as typed links between entities.
PROPERTIES = {"P27": "citizen_of", "P17": "country", "P749": "parent_org", "P127": "owned_by",
              "P361": "part_of", "P131": "located_in", "P35": "head_of_state",
              "P6": "head_of_government", "P452": "industry", "P1001": "jurisdiction"}
HUMAN = "Q5"
MAX_NAME_WORDS, MAX_CONCEPT_WORDS = 6, 3   # "United States Department of the Treasury" vs "ballistic missile"
# Words that can't start or end a name ("Will Xi…", "…in Taiwan"); "New" is allowed (New Hampshire).
NAME_STOP = set("will would does do is are was who what when which how the a an in on by of to for and or".split())
CONNECTORS = {"of", "the", "and", "for", "on"}
STOP = set("""will would does what when which who how the a an in on by of to for and or before
    after between during until with from into this that these those any all new next more least
    there their them they its is be are was were has have had been yes no none january february
    march april may june july august september october november december jan feb mar apr jun jul
    aug sep sept oct nov dec monday tuesday wednesday thursday friday saturday sunday""".split())

# ── cache ─────────────────────────────────────────────────────────────────────
def cached(con, kind, keys, fetch, batch=50, workers=1):
    """{key: value} for every key; fetch(list of missing keys) -> {key: value} fills the gaps."""
    keys = sorted(set(keys))
    if not keys: return {}
    have = {k: json.loads(v) for k, v in con.execute("""
        SELECT key, arg_max(value, fetched_at)::VARCHAR FROM wiki_cache
        WHERE kind = ? AND key IN (SELECT unnest(?::VARCHAR[])) GROUP BY key""", [kind, keys]).fetchall()}
    if kind in REFRESH:  # re-fetch everything not already refreshed in this run
        done = _refreshed.setdefault(kind, set())
        have = {k: v for k, v in have.items() if k in done}
        done.update(keys)
    missing = [k for k in keys if k not in have]
    if missing: log.info("wiki %-10s %5d cached, fetching %d", kind, len(have), len(missing))
    new = {}
    with ThreadPoolExecutor(workers) as ex:  # saved batch by batch, so an interrupted run keeps its progress
        for part, got in zip(chunks(missing, batch), ex.map(fetch, chunks(missing, batch))):
            insert(con, "wiki_cache", [dict(kind=kind, key=k, value=json.dumps(got.get(k)), fetched_at=now())
                                       for k in part])
            new |= got
    return have | {k: new.get(k) for k in missing}

def wiki(params, url=WIKI):
    return get(url, dict(params, format="json", formatversion=2), headers=UA)

def total_articles(con):
    return cached(con, "meta", ["articles"], lambda _: {"articles": wiki(dict(
        action="query", meta="siteinfo", siprop="statistics"))["query"]["statistics"]["articles"]})["articles"]

# ── phrase -> article ─────────────────────────────────────────────────────────
def fetch_titles(phrases):
    """Resolve phrases as Wikipedia titles (following redirects); None if missing/ambiguous."""
    q = wiki(dict(action="query", titles="|".join(phrases), redirects=1, prop="pageprops",
                  ppprop="wikibase_item|disambiguation"))["query"]
    hop = {x["from"]: x["to"] for x in q.get("normalized", []) + q.get("redirects", [])}
    pages = {p["title"]: p for p in q.get("pages", [])}
    out = {}
    for ph in phrases:
        t = ph
        for _ in range(3): t = hop.get(t, t)
        p = pages.get(t, {})
        props = p.get("pageprops", {})
        ok = not p.get("missing") and "disambiguation" not in props and props.get("wikibase_item")
        out[ph] = dict(title=p["title"], qid=props["wikibase_item"]) if ok else None
    return out

def fetch_df(words):
    """Share of articles containing each word (document frequency)."""
    return {w: wiki(dict(action="query", list="search", srsearch=f'"{w}"', srlimit=1, srinfo="totalhits",
                         srprop=""))["query"]["searchinfo"]["totalhits"] for w in words}

TOKEN = re.compile(r"(?:[A-Z]\.){2,}|[A-Za-z][\w’'&-]*")  # "U.S." or a word

@dataclass(frozen=True)
class Candidate:
    phrase: str
    start: int
    end: int
    named: bool     # capitalised -> named entity; lowercase -> concept
    words: int

def candidates(question):
    """Every n-gram worth looking up — names up to MAX_NAME_WORDS, concepts up to MAX_CONCEPT_WORDS —
    longest first."""
    toks = [(m.group(), m.start(), m.end()) for m in TOKEN.finditer(question)]
    out = []
    for i in range(len(toks)):
        for j in range(i + 1, min(i + MAX_NAME_WORDS, len(toks)) + 1):
            words = [w for w, _, _ in toks[i:j]]
            if len(question[toks[i][1]:toks[j - 1][2]]) < 2: continue
            named = (words[0][0].isupper() and words[-1][0].isupper()
                     and all(w[0].isupper() or w.lower() in CONNECTORS for w in words)
                     and words[0].lower() not in NAME_STOP and words[-1].lower() not in NAME_STOP)
            concept = (all(w.islower() for w in words) and j - i <= MAX_CONCEPT_WORDS
                       and words[0] not in STOP and words[-1] not in STOP and (j - i > 1 or len(words[0]) >= 4))
            if named or concept:
                out.append(Candidate(question[toks[i][1]:toks[j - 1][2]], toks[i][1], toks[j - 1][2],
                                     named, j - i))
    return sorted(out, key=lambda c: (-c.words, c.start))

def link(con, questions):
    """{key: [mention]} with mention = dict(id, title, start, end, named, surface)."""
    cands = {k: candidates(q) for k, q in questions.items()}
    singles = {c.phrase.lower() for cs in cands.values() for c in cs if not c.named and c.words == 1}
    df, W = cached(con, "df", singles, fetch_df, batch=10, workers=4), total_articles(con)
    keep = lambda c: c.named or c.words > 1 or (df.get(c.phrase.lower()) or W) / W < CONCEPT_MAX_DF
    forms = lambda c: (c.phrase,) if c.named else (c.phrase, c.phrase.rstrip("s"))  # try singular
    titles = cached(con, "title", {f for cs in cands.values() for c in cs if keep(c) for f in forms(c)},
                    fetch_titles, batch=50, workers=4)
    out = {}
    for k, cs in cands.items():
        taken, ms = [], []
        for c in cs:
            if not keep(c) or any(c.start < b and a < c.end for a, b in taken): continue
            if hit := next((titles[f] for f in forms(c) if titles.get(f)), None):
                taken.append((c.start, c.end))
                ms.append(dict(id=f"wd:{hit['qid']}", title=hit["title"], start=c.start, end=c.end,
                               named=c.named, surface=c.phrase))
        out[k] = ms
    # A lone capitalised word with no article of its own ("Xi") is the one already-linked
    # named entity whose title contains it ("Xi Jinping") — only if exactly one does.
    by_word = {}
    for ms in out.values():
        for m in ms:
            if m["named"]:
                for w in re.findall(r"[A-Za-z][\w-]+", m["title"]): by_word.setdefault(w, set()).add((m["id"], m["title"]))
    lone = {k: [c for c in cs if c.named and c.words == 1] for k, cs in cands.items()}
    demonyms = cached(con, "demonym", {c.phrase for cs in lone.values() for c in cs
                                       if len(by_word.get(c.phrase, ())) != 1}, fetch_demonyms, batch=50)
    for k, cs in lone.items():
        taken = [(m["start"], m["end"]) for m in out[k]]
        for c in cs:
            if any(c.start < b and a < c.end for a, b in taken): continue
            if len(by_word.get(c.phrase, ())) == 1:
                (eid, title), = by_word[c.phrase]
            elif hit := demonyms.get(c.phrase):
                eid, title = f"wd:{hit['qid']}", hit["title"]
            else: continue
            taken.append((c.start, c.end))
            out[k].append(dict(id=eid, title=title, start=c.start, end=c.end, named=True, surface=c.phrase))
        out[k].sort(key=lambda m: m["start"])
    return out

def fetch_demonyms(words):
    """Countries whose Wikidata demonym (P1549) is the word: "Chinese" -> China. None if not exactly one."""
    values = " ".join(f'"{w}"@en' for w in words)
    rows = get(SPARQL, dict(format="json", query=f"""
        SELECT ?d ?c ?title WHERE {{ VALUES ?d {{ {values} }} ?c wdt:P1549 ?d ; wdt:P297 [] .
          ?a schema:about ?c ; schema:isPartOf <https://en.wikipedia.org/> ; schema:name ?title }}"""),
        headers=UA)["results"]["bindings"]
    hits = {}
    for r in rows:
        hits.setdefault(r["d"]["value"], set()).add((r["c"]["value"].rsplit("/", 1)[-1], r["title"]["value"]))
    return {w: dict(qid=next(iter(hits[w]))[0], title=next(iter(hits[w]))[1]) if len(hits.get(w, ())) == 1 else None
            for w in words}

# ── Wikidata ──────────────────────────────────────────────────────────────────
def fetch_entities(qids):
    ents = wiki(dict(action="wbgetentities", ids="|".join(qids), props="labels|descriptions|sitelinks",
                     languages="en", sitefilter="enwiki"), url=WIKIDATA)["entities"]
    values = " ".join(f"wd:{q}" for q in qids)
    props = " ".join(f"wdt:{p}" for p in ["P31", "P297", *PROPERTIES])
    rows = get(SPARQL, dict(format="json", query=f"""
        SELECT ?item ?p ?v WHERE {{ VALUES ?item {{ {values} }} VALUES ?p {{ {props} }} ?item ?p ?v }}"""),
        headers=UA)["results"]["bindings"]
    claims = {q: {} for q in qids}
    for r in rows:
        q, p, v = (r[x]["value"].rsplit("/", 1)[-1] for x in ("item", "p", "v"))
        claims[q].setdefault(p, []).append(v)
    return {q: dict(label=(e.get("labels", {}).get("en") or {}).get("value", q),
                    description=(e.get("descriptions", {}).get("en") or {}).get("value"),
                    title=(e.get("sitelinks", {}).get("enwiki") or {}).get("title"),
                    claims=claims.get(q, {}))
            for q, e in ents.items()}

def entity_type(e):
    c = e["claims"]
    return ("person" if HUMAN in c.get("P31", []) else "country" if c.get("P297") else
            "organisation_or_place" if c.get("P17") else "concept")

def parent(qid, e):
    """Citizenship for people, country for everything else; countries are roots."""
    c = e["claims"]
    if c.get("P297"): return None
    ps = [p for p in (c.get("P27") if HUMAN in c.get("P31", []) else c.get("P17")) or [] if p != qid]
    return ps[0] if ps else None

def describe(con, qids):
    """{qid: entity}, including every entity's parent chain."""
    ents, todo = {}, set(qids)
    while todo:
        got = cached(con, "entity", todo, fetch_entities, batch=50)
        ents |= {q: e for q, e in got.items() if e}
        todo = {p for q, e in got.items() if e and (p := parent(q, e)) and p not in ents}
    return ents

# ── relatedness ───────────────────────────────────────────────────────────────
def _mentioning(titles):
    """Number of articles whose text mentions every title (as a phrase)."""
    q = " ".join(f'"{re.sub(r" [(].*[)]$", "", t).replace(chr(34), "")}"' for t in titles)
    return wiki(dict(action="query", list="search", srsearch=q, srlimit=1, srinfo="totalhits",
                     srprop=""))["query"]["searchinfo"]["totalhits"]

def relatedness(con, pairs):
    """{(title_a, title_b): normalized co-occurrence in [0, 1]}, keys sorted."""
    pairs = {tuple(sorted(p)) for p in pairs if p[0] != p[1]}
    one = cached(con, "mentions", {t for p in pairs for t in p},
                 lambda ts: {t: _mentioning([t]) for t in ts}, batch=5, workers=8)
    both = cached(con, "comentions", {"|".join(p) for p in pairs},
                  lambda ks: {k: _mentioning(k.split("|")) for k in ks}, batch=5, workers=8)
    W, out = total_articles(con), {}
    for a, b in pairs:
        A, B, AB = one.get(a) or 0, one.get(b) or 0, both.get(f"{a}|{b}") or 0
        if AB < MIN_COOCCURRENCE or min(A, B) == 0: out[(a, b)] = 0.0; continue
        sr = 1 - (math.log(max(A, B)) - math.log(AB)) / (math.log(W) - math.log(min(A, B)))
        out[(a, b)] = round(max(0.0, min(1.0, sr)), 3)
    return out

# ── everything graph_build needs ──────────────────────────────────────────────
@dataclass
class EntityGraph:
    mentions: dict      # market key -> [mention]
    entities: dict      # node id ("wd:Q865") -> dict(name, type, description, title, qid)
    parents: dict       # node id -> [parent node id]   (for matching actors)
    links: list         # (src id, dst id, edge type, weight)

def countries_of(qid, ents):
    """Every country an entity belongs to: itself for a country, all citizenships for a person
    (Musk: South Africa, Canada, US), every listed country for anything else."""
    e = ents.get(qid)
    if not e: return set()
    c = e["claims"]
    if c.get("P297"): return {qid}
    return set((c.get("P27") if HUMAN in c.get("P31", []) else c.get("P17")) or []) - {qid}

def discover(con, questions, in_scope=None):
    """Cheap first pass: linked mentions per question, with `named` = actor (person, country,
    organisation) per Wikidata, and the Wikidata items behind them (parent chains included).
    `in_scope(key, actor_ids, ents)` drops questions before any expensive lookups."""
    mentions = link(con, questions)
    found = describe(con, {m["id"][3:] for ms in mentions.values() for m in ms})
    is_actor = lambda m: m["id"][3:] in found and entity_type(found[m["id"][3:]]) != "concept"
    mentions = {k: [dict(m, named=is_actor(m)) for m in ms] for k, ms in mentions.items()}
    if in_scope:
        mentions = {k: ms for k, ms in mentions.items()
                    if in_scope(k, {m["id"] for m in ms if m["named"]}, found)}
    return mentions, found

def build(con, questions, extra_qids=(), in_scope=None):
    """Entities for `questions` ({market key: question text}) that `in_scope` keeps, plus
    `extra_qids` (e.g. entities that carry security exposures), their parents, and the links
    between all of them. `mentions` holds only in-scope questions."""
    node = lambda q: f"wd:{q}"
    mentions, found = discover(con, questions, in_scope)
    title = lambda i: (found.get(i[3:]) or {}).get("title")
    is_actor = lambda m: m["named"]
    pair = lambda a, b: tuple(sorted((title(a), title(b))))

    # Everything that isn't an actor is a concept. Keep a concept only if it is specific (rare on
    # Wikipedia) and related to an actor in the same question: "tariff" with Trump, yes;
    # "correspondent" with the US Treasury, no.
    concept_ms = [m for ms in mentions.values() for m in ms if not is_actor(m)]
    df, W = cached(con, "df", {m["surface"].lower() for m in concept_ms}, fetch_df, batch=10, workers=8), total_articles(con)
    specific = lambda m: (df.get(m["surface"].lower()) or W) / W < CONCEPT_MAX_DF
    asked = {pair(a["id"], c["id"]) for ms in mentions.values() for c in ms if not is_actor(c) and specific(c)
             for a in ms if is_actor(a) and title(a["id"]) and title(c["id"])}
    sr = relatedness(con, asked)
    relevant = lambda c, ms: any(sr.get(pair(a["id"], c["id"]), 0) >= MIN_RELATEDNESS for a in ms if is_actor(a))
    mentions = {k: [dict(m, named=is_actor(m)) for m in ms
                    if is_actor(m) or (specific(m) and relevant(m, ms))] for k, ms in mentions.items()}

    ents = describe(con, {m["id"][3:] for ms in mentions.values() for m in ms} | set(extra_qids))
    entities = {node(q): dict(name=e["label"], type=entity_type(e), description=e["description"],
                              title=e["title"], qid=q) for q, e in ents.items()}
    parents = {node(q): [node(p)] for q, e in ents.items() if (p := parent(q, e)) and p in ents}

    # links: Wikidata statements between entities, scored and supplemented by co-occurrence
    typed = {(node(q), node(v), PROPERTIES[p]) for q, e in ents.items()
             for p, vs in e["claims"].items() if p in PROPERTIES for v in vs if v in ents and v != q}
    pairs = {(a, b) for a, b, _ in typed}
    for ms in mentions.values():  # co-mentioned in a question
        pairs |= {(x["id"], y["id"]) for x in ms for y in ms}
    pairs |= {(m, node(x)) for m in {m["id"] for ms in mentions.values() for m in ms}
              for x in extra_qids}  # every entity vs every exposure entity
    title = lambda i: entities[i]["title"]
    pairs = {tuple(sorted(p)) for p in pairs if p[0] != p[1] and p[0] in entities and p[1] in entities
             and title(p[0]) and title(p[1])}  # undirected
    sr = relatedness(con, {(title(a), title(b)) for a, b in pairs})
    score = lambda a, b: sr.get(tuple(sorted((title(a), title(b)))), 0.0)

    links = [(a, b, t, score(a, b) or None) for a, b, t in typed]
    typed_pairs = {frozenset((a, b)) for a, b, _ in typed}
    links += [(a, b, "related", s) for a, b in sorted(pairs)
              if frozenset((a, b)) not in typed_pairs and (s := score(a, b)) >= MIN_RELATEDNESS]
    return EntityGraph(mentions, entities, parents, links)
