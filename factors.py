"""
Question -> factor links: the market factors (country equities, currencies, oil, freight, policy
rates, credit spreads, ...) a question is relevant to. The link itself is qualitative (no weight
or sign); factor_betas.py measures how much each factor actually moved when the question's
probability jumped, in the factor's units (seeds/factors.csv), and the API reports that next to it.

A question links to a factor when its contract signature involves an entity listed in
seeds/entity_factors.csv — directly, or through a person's citizenship (Putin -> Russia) — and
  - the action's channel (matching.EFFECTS: conflict / trade / sanctions / instability /
    diplomacy) is one the row lists,
  - the entity's role (actor / target / party / concept) is one the row lists (an oil exporter
    matters when it is sanctioned or struck, not when it captures a village), and
  - the action is severe enough (MIN_SEVERITY: appointments, votes and public appearances
    don't move factors).
Unparsed questions have no channel and link to nothing. The rows are analyst priors keyed by
Wikidata id, like seeds/exposures.csv; the catalog is seeds/factors.csv.

graph_build.py writes them as factor nodes and question -> factor `relevant_to` edges (decision
2026-10-20, replacing read-time computation); the API reads those edges through links().

Two layers of factors (2026-10-27):
  macro  seeds/factors.csv: market factors with a price series (equities, FX, commodities, rates,
         credit) and economic indicators (CPI, GDP, payrolls, PMIs; monthly / quarterly, so not
         measured by factor_betas.py — `series` blank).
  niche  seeds/niche_factors.csv: an industry's revenue / cost / investment drivers (load factor,
         jet fuel cost, NIM, wafer demand, ...), each tied to GICS industries (seeds/gics.csv) with
         the sign of its effect on earnings. Qualitative, never measured.
Factors drive each other through seeds/factor_links.csv (`drives` edges: macro -> macro, macro ->
niche, niche -> niche, signed), and niche factors drive GICS industries (`driver_of`), which
issuers are classified into (`classified_as`) — a second path from a question to an issuer:
question -> macro factor -> niche factor -> GICS industry <- issuer. All hand-written analyst
priors (decision 2026-10-27: hardcoded for now, no algorithm).
"""
from collections import defaultdict

from graph_build import SEEDS, seed
from matching import EFFECTS

MIN_SEVERITY = 0.3  # matching.EFFECTS severity of the action

# proxy (macro indicators without a daily series): the daily market factor that prices the indicator,
# "-" = the indicator moves against it (unemployment vs the 2-year yield). Analyst choice
# (2026-10-28), used by the Macro dashboard to measure an indicator through its proxy.
CATALOG = {r["factor_id"]: dict(id=r["factor_id"], name=r["name"], layer="macro", family=r["family"],
                                definition=r["definition"], series=r["series"] or None, units=r["units"] or None,
                                proxy=(r.get("proxy") or "").lstrip("-") or None, proxy_sign=-1 if (r.get("proxy") or "").startswith("-") else 1)
           for r in seed("factors.csv")}
for f, c in CATALOG.items():
    assert not c["proxy"] or (CATALOG.get(c["proxy"]) or {}).get("series"), f"factors.csv: {f}: proxy {c['proxy']} has no series"
    assert not (c["proxy"] and c["series"]), f"factors.csv: {f} has a series and a proxy"
for r in seed("niche_factors.csv"):
    assert r["factor_id"] not in CATALOG, f"niche_factors.csv: duplicate factor {r['factor_id']}"
    CATALOG[r["factor_id"]] = dict(id=r["factor_id"], name=r["name"], layer="niche", family=r["side"],
                                   definition=r["definition"], series=None, units=None, side=r["side"],
                                   earnings_sign=int(r["earnings_sign"]), gics=r["gics"].split(";"))

# GICS: sub-industry code -> its row; industries (6-digit) -> name, group, sector, sub-industries
GICS = {r["sub_industry_code"]: r for r in seed("gics.csv")}
SUB_BY_NAME = {r["sub_industry"]: r for r in GICS.values()}
INDUSTRIES = {}
for r in GICS.values():
    i = INDUSTRIES.setdefault(r["industry_code"], dict(code=r["industry_code"], name=r["industry"],
                                                       industry_group=r["industry_group"], sector=r["sector"],
                                                       sector_code=r["sector_code"], sub_industries=[]))
    i["sub_industries"].append(dict(code=r["sub_industry_code"], name=r["sub_industry"]))

# factor_prices keeps every series a factor was ever measured by (append-only); readers use only the
# factor's current series (seeds/factors.csv), so switching a factor to a better proxy never mixes them.
CURRENT_SERIES = [f"{f}|{c['series']}" for f, c in CATALOG.items() if c["series"]]
SERIES_FILTER = "factor_id || '|' || series IN (SELECT unnest(?::VARCHAR[]))"

CLEANED_FAMILIES = {"country_equity", "industry"}  # equity indices / ETFs: a one-day move of 60% is a data error
MAX_DAILY_RATIO, REVERT_DAYS = 1.6, 5

def clean_levels(fid, points):
    """[(date, level)] sorted, for estimation: for equity index / ETF factors, a level more than
    MAX_DAILY_RATIO away from the previous one is a bad print if the series comes back within
    REVERT_DAYS observations (dropped: 1306.T printed 37.6 for 382.7 on 2026-03-30/31), else a share
    split (earlier levels rescaled onto today's basis). Other factors are returned as they are."""
    if CATALOG[fid]["family"] not in CLEANED_FAMILIES: return points
    pts, out, i = [(d, v) for d, v in points if v is not None and v > 0], [], 0
    far = lambda a, b: max(a / b, b / a) > MAX_DAILY_RATIO
    while i < len(pts):
        if out and far(pts[i][1], out[-1][1]):
            back = next((j for j in range(i + 1, min(i + 1 + REVERT_DAYS, len(pts))) if not far(pts[j][1], out[-1][1])), None)
            if back is not None:
                i = back; continue
            r = pts[i][1] / out[-1][1]
            out = [(d, v * r) for d, v in out]
        out.append(pts[i]); i += 1
    return out

def industries_of(fid):
    """{industry code: sub-industry codes it applies to, or None for the whole industry} for a
    niche factor (its gics codes may name a sector, group, industry or sub-industry)."""
    out = {}
    for c in CATALOG[fid].get("gics") or ():
        assert any(k.startswith(c) for k in GICS), f"niche_factors.csv: {fid}: unknown GICS code {c}"
        if len(c) == 8:
            ind = GICS[c]["industry_code"]
            if out.get(ind, ()) is not None: out.setdefault(ind, []).append(c)
        else:
            for ind in INDUSTRIES:
                if ind.startswith(c): out[ind] = None
    return out

# Curated question links (seeds/question_links.csv, hand-assessed 2026-10-27): question -> factor /
# GICS industry / issuer, sign = what YES means for the target. Optional file.
QUESTION_LINKS = seed("question_links.csv") if (SEEDS / "question_links.csv").exists() else []
for r in QUESTION_LINKS:
    assert r["target_type"] in ("factor", "industry", "issuer"), f"question_links.csv: bad target_type {r['target_type']}"
    assert r["target_type"] != "factor" or r["target_id"] in CATALOG, f"question_links.csv: unknown factor {r['target_id']}"
    assert r["target_type"] != "industry" or r["target_id"] in INDUSTRIES, f"question_links.csv: unknown industry {r['target_id']}"

# The 11 GICS sectors, each priced by its SPDR sector ETF (seeds/gics_sectors.csv), and the
# question -> sector links (seeds/gics_sector_links.csv: one LLM judgment pass over the live
# questions, 2026-10-28, to be replaced by a methodical version). sign = what YES means for the
# sector's stocks, strength 3 first-order / 2 material / 1 marginal. Not graph edges:
# security_betas.py measures each link against the sector ETF and the /gics dashboard reads both.
SECTORS = {r["sector_code"]: dict(code=r["sector_code"], name=r["sector"], etf=r["etf"], etf_name=r["etf_name"])
           for r in seed("gics_sectors.csv")}
assert {i["sector_code"] for i in INDUSTRIES.values()} == set(SECTORS), "gics_sectors.csv must list every gics.csv sector"
SECTOR_LINKS = defaultdict(dict)  # question id -> {sector code: dict(sign, strength, rationale)}
for r in seed("gics_sector_links.csv") if (SEEDS / "gics_sector_links.csv").exists() else []:
    assert r["sector_code"] in SECTORS, f"gics_sector_links.csv: unknown sector {r['sector_code']}"
    SECTOR_LINKS[r["question_id"]][r["sector_code"]] = dict(sign=int(r["sign"]), strength=int(r["strength"]), rationale=r["rationale"])

LINKS = []  # factor -> factor transmission: (src, dst, sign, rationale)
for r in seed("factor_links.csv"):
    assert r["src"] in CATALOG and r["dst"] in CATALOG, f"factor_links.csv: unknown factor in {r['src']} -> {r['dst']}"
    LINKS.append((r["src"], r["dst"], int(r["sign"]), r["rationale"]))
RULES = defaultdict(list)  # entity id -> [(factor id, channels, roles, rationale)]
for r in seed("themes.csv"):   # hand-made themes (graph_build.THEMES): a themed question is relevant to its factors
    for fid in filter(None, r["factors"].split(";")):
        assert fid in CATALOG, f"themes.csv: {r['theme_id']}: unknown factor {fid}"
        RULES[f"theme:{r['theme_id']}"].append((fid, {"theme"}, {"concept"}, f"{r['name']} theme"))
for r in seed("entity_factors.csv"):
    assert r["factor_id"] in CATALOG, f"entity_factors.csv: unknown factor {r['factor_id']}"
    RULES[f"wd:{r['qid']}"].append((r["factor_id"], set(r["channels"].split(";")), set(r["roles"].split(";")),
                                    r["rationale"]))

def derive(involves, citizen_of):
    """{factor id: paths} for one question. involves = [(entity id, role, action, channel)] from its
    `involves` edges, citizen_of = {entity id: [country ids]}. A path = dict(nodes=[entity ids:
    the mentioned entity, then the one the rule matched], role, channel, rationale)."""
    paths = defaultdict(list)
    for ent, role, action, channel in involves:
        if channel != "theme" and (not channel or EFFECTS[action][1] < MIN_SEVERITY): continue  # curated themes: no severity gate
        for src in [ent, *citizen_of.get(ent, ())]:
            for fid, channels, roles, why in RULES.get(src, ()):
                if channels & {channel, "any"} and roles & {role, "any"}:
                    paths[fid].append(dict(nodes=list(dict.fromkeys([ent, src])), role=role, channel=channel,
                                           rationale=why))
    return paths

def links(g, qid):
    """[factor + paths] for one question, read from its `relevant_to` edges in the graph."""
    out = []
    for e in g.out[qid]:
        if e["type"] != "relevant_to" or e["dst"] not in g.nodes: continue
        paths = [dict(p, nodes=[g.ref(n) for n in p["nodes"] if n in g.nodes]) for p in e["attrs"]["paths"]]
        out.append(dict(CATALOG[e["dst"][7:]], paths=paths))
    return sorted(out, key=lambda f: f["id"])
