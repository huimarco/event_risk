"""Dashboard rankings (api.rank_rows) and the ranked endpoints — /dashboard/movers, /gics,
/gics/{sector}, /dashboard/issuers/{id}, /dashboard/factors/{id} — against a throwaway DuckDB
built from schema.sql with synthetic markets, prices, graph and beta rows (no MotherDuck needed).

  pip install pytest httpx && python -m pytest tests
"""
import json, os, sys, tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb, pytest

ROOT = Path(__file__).resolve().parent.parent
NOW = datetime.now(timezone.utc)
# question -> (rungs: [(prob now, 24h ago, 7d ago, 30d ago, 24h volume)])
QUESTIONS = {
    "q:t:1": [(0.45, 0.20, 0.40, 0.10, 100)],     # +25 pts 24h
    "q:t:2": [(0.68, 0.80, 0.30, 0.60, 5000)],    # -12 pts 24h, +38 1w
    "q:t:3": [(0.50, 0.47, 0.52, 0.05, 50)],      # +3, -2, +45
    "q:t:4": [(0.90, 0.90, None, None, 20)],      # 0 pts 24h, no 1w / 1m history
    "q:t:5": [(0.30, None, None, None, 10)],      # no history at all
    "q:t:L": [(0.60, 0.50, 0.55, 0.50, 1000), (0.20, 0.20, 0.20, 0.20, 10)],  # two rungs: headline = the first
}
CLOSED = "q:t:C"  # last priced 10 days ago

def build(path):
    con = duckdb.connect(path)
    con.execute("SET TimeZone = 'UTC'")
    con.execute((ROOT / "schema.sql").read_text())
    rec = NOW - timedelta(days=60)
    node = lambda i, t, name, attrs=None: con.execute(
        "INSERT INTO graph_nodes VALUES (?, ?, ?, ?, '', 'assert', NULL, NULL, ?, 'test')", [i, t, name, json.dumps(attrs or {}), rec])
    edge = lambda s, d, t, attrs=None: con.execute(
        "INSERT INTO graph_edges VALUES (?, ?, ?, ?, NULL, 1, 1, 'test', NULL, ?, '', 'assert', NULL, NULL, ?, 'test')",
        [f"{s}>{d}", s, d, t, json.dumps(attrs or {}), rec])
    snap = lambda mid, ts, p, vol: con.execute(
        "INSERT INTO market_snapshots (source, run_id, ts, venue, market_id, last, volume_24h, open_interest) VALUES ('realtime', 'test', ?, 'kalshi', ?, ?, ?, 100)",
        [ts, mid, p, vol])
    for q, rungs in {**QUESTIONS, CLOSED: [(None, None, None, None, 0)]}.items():
        node(q, "question", f"Question {q[-1]}", dict(kind="deadlines" if len(rungs) > 1 else None, n_rungs=len(rungs), venues=["kalshi"]))
        for k, (now, d1, d7, d30, vol) in enumerate(rungs):
            p, m = f"prop:{q[-1]}{k}", f"M{q[-1]}{k}"
            node(p, "proposition", f"Rung {q[-1]}{k}", dict(venues=["kalshi"]))
            node(f"mkt:kalshi:{m}", "market", f"Market {m}", dict(venue="kalshi"))
            edge(f"mkt:kalshi:{m}", p, "same_as")
            edge(p, q, "rung_of", dict(window=[None, f"2027-0{k + 1}-01"]))
            if q == CLOSED:
                snap(m, NOW - timedelta(days=10), 0.5, 0); continue
            for ago, v in ((timedelta(hours=1), now), (timedelta(hours=25), d1), (timedelta(days=7, hours=1), d7), (timedelta(days=30, hours=1), d30)):
                if v is not None: snap(m, NOW - ago, v, vol)
    node("iss:Q1", "issuer", "Acme Corp", dict(sector="Information Technology"))
    node("sec:AAA", "security", "Acme Corp", dict(ticker="AAA", primary=True))
    edge("iss:Q1", "sec:AAA", "issued")
    run = lambda table, cols, rows: [con.execute(f"INSERT INTO {table} (run_id, computed_at, {cols}) VALUES ('r1', ?, {', '.join('?' * len(r))})",
                                                 [NOW, *r]) for r in rows]
    # sector 45 (XLK): measured |beta| q2 5 > q3 1 > q5 0.5 > q1 0.1; q4 linked but unmeasured
    run("security_betas", "question_id, ticker, source, n_events, beta, se, shrunk, shrunk_se, units",
        [(q, "XLK", "sector", 3, b, 1.0, b, 0.5, "%") for q, b in (("q:t:1", 0.1), ("q:t:2", -5.0), ("q:t:3", 1.0), ("q:t:5", 0.5))])
    # issuer AAA: channel rows stored by |shrunk| (q3, q2, q1); statistical-only rows q4, q5
    run("issuer_event_betas", "ticker, issuer_id, question_id, rank, n_evaluated, n_live, channels, n_events, beta, se, q, shrunk, shrunk_se, units",
        [("AAA", "iss:Q1", q, k, 900, 3, ch, 4, b, 0.5, 0.1, b, 0.3, "%") for q, k, ch, b in
         (("q:t:3", 1, "sector", 0.9), ("q:t:2", 2, "factors;entity", -0.5), ("q:t:1", 3, "entity", 0.2), ("q:t:4", 1, "", 0.9), ("q:t:5", 2, "", -0.4))])
    # factor brent (%): detectable = q2 (unlinked, P(effect) 0.8), q1 (linked); weak = q3
    run("factor_event_betas", "factor_id, question_id, rank, n_evaluated, n_live, linked, channel, n_events, beta, se, q, shrunk, shrunk_se, units, p_effect",
        [("brent", q, k, 800, 2, lk, ch, 3, b, 1.0, qv, b, 0.5, "%", pe) for q, k, lk, ch, b, qv, pe in
         (("q:t:2", 1, False, None, -2.0, 0.2, 0.8), ("q:t:1", 2, True, "conflict", 1.0, 0.01, None), ("q:t:3", 1, False, None, 0.3, 0.6, 0.2))])
    con.close()

@pytest.fixture(scope="module")
def client():
    path = os.path.join(tempfile.mkdtemp(), "test.duckdb")
    build(path)
    os.environ["DB"], os.environ["API_KEYS"] = path, ""
    sys.path.insert(0, str(ROOT))
    for m in ("api", "collector"): sys.modules.pop(m, None)
    import api, factors
    from fastapi.testclient import TestClient
    link = lambda s: dict(sign=s, strength=2, rationale="test")
    factors.SECTOR_LINKS.clear()
    factors.SECTOR_LINKS.update({"q:t:1": {"45": link(1)}, "q:t:2": {"45": link(-1)}, "q:t:3": {"45": link(1), "10": link(1)},
                                 "q:t:4": {"45": link(0)}, "q:t:5": {"45": link(1)}})
    return TestClient(api.app)

ids = lambda rows: [r["id"] for r in rows]

# ── the ranking utility ─────────────────────────────────────────────────────
def test_rank_rows_unit():
    import api
    rows = [dict(id="a", change_24h=-0.30), dict(id="b", change_24h=None), dict(id="c", change_24h=0.10),
            dict(id="d", change_24h=0.30), dict(id="e", change_24h=0.0)]
    out = api.rank_rows(rows, "move_24h")
    assert ids(out) == ["a", "d", "c", "e", "b"]          # |move|, ties keep input order, None last (0 is a value)
    assert ids(rows) == ["a", "b", "c", "d", "e"]          # input untouched
    m = {"a": dict(q=0.5), "b": dict(q=None), "c": dict(q=0.01), "d": {}, "e": dict(q=0.2)}
    assert ids(api.rank_rows(rows, "q", lambda r: m[r["id"]])) == ["c", "e", "a", "b", "d"]  # lowest q first
    beta = {"a": 1.0, "b": 9.0, "c": -4.0, "d": None, "e": 2.0}
    meas = lambda r: dict(beta=beta[r["id"]])
    assert ids(api.rank_rows(rows, "sensitivity", meas)) == ["b", "c", "e", "a", "d"]
    assert ids(api.rank_rows(rows, "repricing", meas)) == ["c", "a", "e", "b", "d"]  # |beta x change|: .4, .3, 0, then None
    info = api.ranking_info("repricing", rows, meas)
    assert (info["n_ranked"], info["n_available"], info["evidence"]) == (5, 3, "estimated")

# ── Biggest Movers ──────────────────────────────────────────────────────────
def test_movers_default_24h(client):
    d = client.get("/dashboard/movers").json()
    assert d["ranked_by"] == "move_24h"
    assert ids(d["events"]) == ["q:t:1", "q:t:2", "q:t:L", "q:t:3", "q:t:4", "q:t:5"]  # -12 beats +10; no history last
    assert [e["position"] for e in d["events"]] == [1, 2, 3, 4, 5, 6]
    assert CLOSED not in ids(d["events"])
    assert d["ranking"]["n_ranked"] == 6 and d["ranking"]["n_available"] == 5
    e = {x["id"]: x for x in d["events"]}
    assert e["q:t:2"]["change_24h"] == pytest.approx(-0.12) and e["q:t:L"]["change_24h"] == pytest.approx(0.10)
    assert e["q:t:5"]["change_24h"] is None and e["q:t:4"]["change_1w"] is None

def test_movers_one_row_per_question(client):
    ev = client.get("/dashboard/movers", params=dict(limit=500)).json()["events"]
    assert len(ids(ev)) == len(set(ids(ev)))
    L = next(x for x in ev if x["id"] == "q:t:L")
    assert L["prob"] == pytest.approx(0.60) and L["headline"]["id"] == "prop:L0" and L["volume_24h"] == pytest.approx(1010)

@pytest.mark.parametrize("rank, expected", [
    ("move_1w", ["q:t:2", "q:t:L", "q:t:1", "q:t:3", "q:t:4", "q:t:5"]),   # q1 / qL tie at 5 pts: higher volume first
    ("move_1m", ["q:t:3", "q:t:1", "q:t:L", "q:t:2", "q:t:4", "q:t:5"]),
    ("volume", ["q:t:2", "q:t:L", "q:t:1", "q:t:3", "q:t:4", "q:t:5"]),
    ("prob", ["q:t:4", "q:t:2", "q:t:L", "q:t:3", "q:t:1", "q:t:5"])])
def test_movers_rankings(client, rank, expected):
    assert ids(client.get("/dashboard/movers", params=dict(rank=rank)).json()["events"]) == expected

def test_movers_limit_and_validation(client):
    assert ids(client.get("/dashboard/movers", params=dict(limit=2)).json()["events"]) == ["q:t:1", "q:t:2"]
    assert client.get("/dashboard/movers", params=dict(rank="sensitivity")).status_code == 422  # no beta on this board

# ── GICS ────────────────────────────────────────────────────────────────────
def sector(client, code, **p):
    return client.get(f"/gics/{code}", params=p).json()

def test_gics_default_unchanged(client):
    d = client.get("/gics").json()
    s = next(x for x in d["sectors"] if x["code"] == "45")
    assert d["ranked_by"] == "sensitivity" and ids(s["events"]) == ["q:t:2", "q:t:3", "q:t:5", "q:t:1", "q:t:4"]
    assert s["events"][-1]["sector_move"] is None   # unmeasured stays unmeasured, ranked last
    same = client.get("/gics", params=dict(rank="sensitivity")).json()
    assert [ids(x["events"]) for x in same["sectors"]] == [ids(x["events"]) for x in d["sectors"]]

def test_gics_ranks_full_linked_set_before_limit(client):
    # q1 has the smallest measured beta, so it is outside the default top 2 but tops the 24h ranking
    assert ids(sector(client, "45", limit=2)["events"]) == ["q:t:2", "q:t:3"]
    s = sector(client, "45", limit=2, rank="move_24h")
    assert ids(s["events"]) == ["q:t:1", "q:t:2"] and s["ranking"]["n_ranked"] == 5 and s["n_linked"] == 5

@pytest.mark.parametrize("rank, expected", [
    ("move_1w", ["q:t:2", "q:t:1", "q:t:3", "q:t:5", "q:t:4"]),   # no history: default order
    ("move_1m", ["q:t:3", "q:t:1", "q:t:2", "q:t:5", "q:t:4"]),
    ("repricing", ["q:t:2", "q:t:3", "q:t:1", "q:t:5", "q:t:4"]),   # |beta x 24h change|: .6, .03, .025; q5 no change, q4 no beta
    ("risk", ["q:t:2", "q:t:3", "q:t:5", "q:t:1", "q:t:4"])])
def test_gics_rankings(client, rank, expected):
    assert ids(sector(client, "45", rank=rank)["events"]) == expected

def test_gics_sectors_independent(client):
    assert ids(sector(client, "10", rank="move_24h")["events"]) == ["q:t:3"]
    assert ids(sector(client, "45")["events"])[0] == "q:t:2"
    e = sector(client, "45", rank="repricing")["events"][0]
    assert e["repricing_24h"] == pytest.approx(-5.0 * -0.12) and e["sector_move"]["beta"] == pytest.approx(-5.0)

# ── Issuer ──────────────────────────────────────────────────────────────────
def issuer(client, **p):
    return client.get("/dashboard/issuers/iss:Q1", params=p).json()

def test_issuer_default_and_channel_split(client):
    d = issuer(client)
    assert d["ranked_by"] == "sensitivity"
    assert ids(d["events"]) == ["q:t:3", "q:t:2", "q:t:1"] and ids(d["statistical"]) == ["q:t:4", "q:t:5"]
    assert all(e["move"]["channels"] for e in d["events"]) and not any(e["move"]["channels"] for e in d["statistical"])
    assert d["ranking"]["complete"] and d["ranking"]["n_live"] == 3

@pytest.mark.parametrize("rank, chan, stat", [
    ("move_24h", ["q:t:1", "q:t:2", "q:t:3"], ["q:t:4", "q:t:5"]),
    ("move_1w", ["q:t:2", "q:t:1", "q:t:3"], ["q:t:4", "q:t:5"]),
    ("move_1m", ["q:t:3", "q:t:1", "q:t:2"], ["q:t:4", "q:t:5"]),
    ("repricing", ["q:t:2", "q:t:1", "q:t:3"], ["q:t:4", "q:t:5"]),  # .06, .05, .027; q4 0 x .9 = 0, q5 None
    ("risk", ["q:t:3", "q:t:2", "q:t:1"], ["q:t:4", "q:t:5"])])
def test_issuer_rankings_stay_within_lists(client, rank, chan, stat):
    d = issuer(client, rank=rank)
    assert ids(d["events"]) == chan and ids(d["statistical"]) == stat
    assert d["events"][0]["repricing_24h"] is not None or rank != "repricing"

# ── Macro ───────────────────────────────────────────────────────────────────
def macro(client, **p):
    return client.get("/dashboard/factors/brent", params=p).json()

@pytest.mark.parametrize("rank, events", [
    ("sensitivity", ["q:t:2", "q:t:1"]), ("move_24h", ["q:t:1", "q:t:2"]), ("repricing", ["q:t:1", "q:t:2"]),  # |1 x .25| > |-2 x -.12|
    ("q", ["q:t:1", "q:t:2"]), ("p_effect", ["q:t:2", "q:t:1"])])   # linked q1 has no P(effect): last
def test_macro_rankings(client, rank, events):
    d = macro(client, rank=rank)
    assert ids(d["events"]) == events and ids(d["weak"]) == ["q:t:3"]
    assert all(e["sensitivity"]["units"] == "%" for e in d["events"] + d["weak"])

def test_macro_p_effect_unavailable_for_linked(client):
    r = macro(client, rank="p_effect")["ranking"]
    assert r["n_ranked"] == 2 and r["n_available"] == 1

# ── existing endpoints keep working ─────────────────────────────────────────
def test_existing_endpoints(client):
    assert client.get("/questions", params=dict(sort="move")).json()["questions"][0]["id"] == "q:t:1"
    assert client.get("/questions/q:t:L/contracts").json()["contracts"]
    assert client.get("/dashboard/issuers").status_code == 200 and client.get("/dashboard/factors").status_code == 200
    assert client.get("/").status_code == 200
    assert client.get("/gics", params=dict(rank="nope")).status_code == 422
