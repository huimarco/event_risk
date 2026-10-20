"""
Question -> factor links: the market factors (country equities, currencies, oil, freight, policy
rates, ...) a question is relevant to. Qualitative: a link has no weight, loading or sign.

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
"""
from collections import defaultdict

from graph_build import seed
from matching import EFFECTS

MIN_SEVERITY = 0.3  # matching.EFFECTS severity of the action

CATALOG = {r["factor_id"]: dict(id=r["factor_id"], name=r["name"], family=r["family"], definition=r["definition"])
           for r in seed("factors.csv")}
RULES = defaultdict(list)  # entity id -> [(factor id, channels, roles, rationale)]
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
        if not channel or EFFECTS[action][1] < MIN_SEVERITY: continue
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
