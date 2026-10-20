"""
Market matching by contract signature.

Each market is parsed into a signature — what happens (action), who and where (entities), when it
counts (window), how much (threshold), and qualifying conditions — from venue fields and the
venues' templated rules text. Two markets are then compared field by field:

  same_as          every field equal, prices agree        -> one proposition, prices blended
  near_equivalent  same event, but entities only equal    -> linked, never blended
                   via a parent (White House ~ US), a
                   sibling action (meet ~ seen together),
                   different conditions (in person vs incl.
                   phone calls), or prices that disagree
  implies          equal except one window (or             -> logical edge A => B
                   threshold) strictly inside the other
  (no match)       anything else, or anything unparsed; a rejected candidate carries the reason

Candidates are only compared when they share an action family and the same top-level entities,
and every distinctive word of one question must appear in the other market's text.

Windows that differ only in time already elapsed ("Dec 2 2025 - Dec 31 2026" vs "Jan 1 - Dec 31
2026", seen in Oct 2026) are the same bet only if the event didn't happen in the gap. The text can't
tell; prices can. So the final check is the market's own: a same_as pair whose latest prices differ
by more than PRICE_TOLERANCE is downgraded to near_equivalent, and an elapsed-time-only pair is
same_as only when prices confirm it.

Each pair is judged once, at a fixed moment: when both markets are listed and both have a price
(that date, and both prices then). A decision therefore never changes later: the graph only moves
when markets are added or their terms change.

Opposite bets never match: a negated question ("agrees not to join NATO", "military action ends",
"blockade lifted") is only compared with other negated ones. Within one venue the operative rules
are compared too — rules that name something neither question mentions (a different intersection
in "Will Russia capture Novopavlivka?") make the pair near_equivalent, not same_as.

EFFECTS says what a YES means for each entity (channel, severity, direction by role), for the
signed proposition -> entity edges.

Unparsed markets never match; they are listed for review. Human overrides win (graph_build.py).
"""
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from itertools import combinations

# ── vocabulary ────────────────────────────────────────────────────────────────
# (action, kind, symmetric, pattern). First match wins, so specific before general.
# kind: occurrence = "does X happen in the window"; measure = a value observed at / over the
# window (can't drop elapsed time); race = "who is first/next" (depends on the field of rivals).
# Actions only match within a family; siblings in a family differ => near_equivalent.
FAMILY = {"appear_together": "meet", "enter": "capture"}
ACTIONS = [(a, FAMILY.get(a, a), k, s, re.compile(p, re.I)) for a, k, s, p in [
    ("appear_together", "occurrence", True, r"seen together|photographed together|same frame"),
    ("trade_agreement", "occurrence", True, r"trade (?:deal|agreement)|tariff agreement"),
    ("tariff_rate", "measure", False, r"tariff rate"),
    ("tariff_change", "occurrence", False, r"tariff"),
    ("meet", "occurrence", True, r"\bmeets?\b|\bmeeting\b|\bmet\b|\bsummit\b"),
    ("talk", "occurrence", True, r"\btalks?\b|phone call|\bspeak"),
    ("visit", "occurrence", False, r"\bvisits?\b|\bvisited\b|\btravel(?:s|led)? to\b"),
    ("invade", "occurrence", False, r"\binvade|\binvasion|military offensive"),
    ("military_clash", "occurrence", True, r"military (?:clash|encounter)|armed clash"),
    ("blockade", "occurrence", False, r"blockade"),
    ("missile_launch", "occurrence", False, r"missile launch|launch(?:es)? a (?:ballistic )?missile"),
    ("ceasefire", "occurrence", True, r"cease-?fire|\btruce\b|peace (?:deal|agreement|treaty)|armistice"),
    ("capture", "occurrence", False, r"\bcaptures?\b|\bseiz(?:e|es|ure)\b|takes? control"),
    ("enter", "occurrence", False, r"\benters?\b"),
    ("criticize", "occurrence", False, r"critici[sz]e|disparage|\binsults?\b"),
    ("strike", "occurrence", False,
     r"\bstrikes?\b|airstrike|\battacks?\b|military (?:action|engagement|operations?)|\braids?\b|\btargets?\b"
     r"|incursion|\bbomb|\bdowns?\b|shoots? down|launch(?:es)? an? (?:military|ground|air|attack|offensive|operation)"),
    ("annex", "occurrence", False, r"\bannex"),
    ("close", "occurrence", False, r"\bclos(?:es|ed|ure|e)\b|\bshut(?:s|down)?\b"),
    ("reopen", "occurrence", False, r"\breopen|\brestarts?\b|\bresumes?\b"),
    ("deploy", "occurrence", False, r"\bsends? (?:warships|troops|forces|ships)|\bdeploy|\bforces in\b"),
    ("evacuate", "occurrence", False, r"\bevacuat"),
    ("withdraw", "occurrence", False, r"\bwithdraw"),
    ("transit", "measure", False, r"\btransits?\b|transit calls"),
    ("join", "occurrence", False, r"\bjoins?\b|\baccession\b"),
    ("coup", "occurrence", False, r"\bcoup\b"),
    ("regime_fall", "occurrence", False, r"(?:regime|government) (?:falls?|collapses?)|\bcollapse of\b|overthrown|toppled"),
    ("outage", "occurrence", False, r"blackout|outage|\bhack|cyber|\bincident\b"),
    ("purge", "occurrence", False, r"\bpurge"),
    ("leave_office", "occurrence", False,
     r"\bout (?:as|by|before)\b|out of power|ceases? to (?:be|occupy)|leave office|\bresign|departure as"
     r"|steps? down|\bleaves? (?:as |office|the (?:presidency|government|post)|(?:prime minister|president|chancellor)\b)"
     r"|\bleaves? [A-Z]?\w* ?(?:house|office) "),
    ("sanction_relief", "occurrence", False,
     r"sanctions?[- ]relief|lift\w* (?:the |all |any )?sanctions|sanctions? (?:are |be |is )?(?:lifted|eased|removed)"
     r"|eas\w+ (?:the )?sanctions|remov\w+ (?:the |all )?sanctions"),
    ("sanction", "occurrence", False, r"\bsanction"),
    ("list_add", "occurrence", False, r"\badded to\b|inclusion of"),
    ("list_remove", "occurrence", False, r"removed from"),
    ("travel_advisory", "occurrence", False, r"level 4|travel advisory|do not travel"),
    ("recognize", "occurrence", False, r"recogni[sz]e"),
    ("abolish", "occurrence", False, r"\babolish|abolition"),
    ("obtain", "occurrence", False, r"\bobtains?\b"),
    ("unban", "occurrence", False, r"\bunban|lift(?:s)? (?:its|the) ban"),
    ("ban", "occurrence", False, r"\bbans?\b|\bbar\b|prohibit"),
    ("impeach", "occurrence", False, r"\bimpeach"),
    ("sentence", "occurrence", False, r"\bsentenced?\b|convicted"),
    ("permit", "occurrence", False, r"\bpermit(?:s|ting)?\b|\ballow(?:s|ing)?\b"),
    ("legislation", "occurrence", False, r"legislation|\bbill\b|becomes? law"),
    ("fine", "occurrence", False, r"\bfined?\b|\bpenalty"),
    ("election_win", "measure", False, r"\bwins?\b.*election|election.*\bwins?\b"),
    ("vote", "measure", False, r"turnout|referendum|\bvotes? (?:for|on|to)\b|qualif\w* for the (?:second|next) round"),
    ("agreement", "occurrence", True, r"\bagrees?\b|\bagreement\b|\bdeal\b|\baccords?\b|\bsigns?\b|\btreaty\b"),
    ("appear", "occurrence", False, r"public appearance|seen in public|\bseen\b|\bappears?\b"),
    ("appoint", "occurrence", False,
     r"\bappointed\b|\bnominee\b|confirmed as|\bbe (?:the )?(?:next |first )?(?:minister|prime minister|president|"
     r"chancellor|head of|leader|commander|chair|vice|secretary|speaker|governor|supreme leader|ceo)"),
]]
RACE = re.compile(r"\bnext\b|first listed|first to\b", re.I)
PRICE_TOLERANCE = 0.10

# What a YES outcome means for the entities involved — the structural prior for direction and size
# of impact (event betas will calibrate it). action: (channel, severity 0-1, polarity for the
# target, polarity for the actor). Polarity -1 = adverse / escalation for that entity (or more of
# a concept: more tariffs, more trade war), +1 = favourable / de-escalation, 0 = direction unknown.
# Symmetric actions treat every party as a target. Concepts take the target's polarity.
EFFECTS = {
    "invade":          ("conflict",    1.0, -1, -0.6),
    "blockade":        ("conflict",    0.8, -1, -0.5),
    "military_clash":  ("conflict",    0.6, -1, -1),
    "missile_launch":  ("conflict",    0.4, -1, -0.5),
    "strike":          ("conflict",    0.5, -1, -0.5),
    "capture":         ("conflict",    0.6, -1, -0.3),
    "enter":           ("conflict",    0.5, -1, -0.3),
    "ceasefire":       ("conflict",    0.6, +1, +1),     # de-escalation
    "transit":         ("conflict",    0.4, +1, 0),      # YES = traffic above the threshold: flowing
    "join":            ("diplomacy",   0.3, +1, +1),
    "annex":           ("conflict",    0.7, -1, -0.5),
    "close":           ("conflict",    0.5, -1, -0.5),   # airspace, straits, borders
    "reopen":          ("conflict",    0.3, +1, +1),
    "deploy":          ("conflict",    0.3, -1, 0),
    "evacuate":        ("instability", 0.4, -1, 0),
    "withdraw":        ("diplomacy",   0.3, 0, 0),       # troops (de-escalation) or treaties (escalation)
    "criticize":       ("diplomacy",   0.1, -1, 0),
    "regime_fall":     ("instability", 0.8, -1, -1),
    "outage":          ("instability", 0.3, -1, -1),
    "agreement":       ("diplomacy",   0.4, +1, +1),
    "vote":            ("instability", 0.2, 0, 0),
    "appear":          ("instability", 0.1, 0, 0),
    "appoint":         ("instability", 0.15, 0, 0),
    "coup":            ("instability", 0.7, -1, -1),
    "purge":           ("instability", 0.3, -1, -1),
    "leave_office":    ("instability", 0.4,  0, 0),
    "impeach":         ("instability", 0.3, -1, 0),
    "travel_advisory": ("instability", 0.3, -1, 0),
    "sanction":        ("sanctions",   0.6, -1, -0.2),
    "sanction_relief": ("sanctions",   0.5, +1, 0),
    "list_add":        ("sanctions",   0.4, -1, 0),
    "list_remove":     ("sanctions",   0.4, +1, 0),
    "fine":            ("sanctions",   0.2, -1, 0),
    "tariff_change":   ("trade",       0.5, -1, -0.3),
    "tariff_rate":     ("trade",       0.5, -1, -0.3),   # YES = rate above the threshold
    "ban":             ("trade",       0.3, -1, 0),
    "unban":           ("trade",       0.3, +1, 0),
    "trade_agreement": ("trade",       0.5, +1, +1),
    "meet":            ("diplomacy",   0.2, +1, +1),
    "talk":            ("diplomacy",   0.15, +1, +1),
    "appear_together": ("diplomacy",   0.15, +1, +1),
    "visit":           ("diplomacy",   0.2, +1, +1),
    "recognize":       ("diplomacy",   0.3, +1, 0),
}
UNKNOWN_EFFECT = (None, 0.3, 0, 0)   # unparsed or unlisted actions: some relevance, no direction
FLIP_BELOW = {"tariff_rate", "transit"}
RELIEF = {"sanction_relief", "unban", "list_remove"}  # "lift" is already part of these actions  # measures whose YES flips meaning with a "below" threshold
ROLE_WEIGHT = {"target": 1.0, "actor": 0.6, "party": 1.0, "concept": 0.8}

def effect(sig, entity_id):
    """(role, channel, weight, sign) of a market's YES outcome for one of its entities.
    weight = severity x role weight; sign = polarity (None when unknown)."""
    channel, severity, on_target, on_actor = EFFECTS.get(sig.action, UNKNOWN_EFFECT)
    if sig.action in FLIP_BELOW and sig.threshold and sig.threshold[0] in ("<", "<="):
        on_target, on_actor = -on_target, -on_actor   # YES = tariffs stay low
    if sig.negated and sig.action not in RELIEF:
        on_target, on_actor = -on_target, -on_actor   # YES = it doesn't happen / it ends
    concepts = {c for c, _ in sig.concepts}
    role = ("concept" if entity_id in concepts else "target" if entity_id in sig.targets
            else "party" if sig.symmetric or not sig.targets else "actor")
    polarity = on_actor if role == "actor" else on_target
    return role, channel, round(severity * ROLE_WEIGHT[role], 3), (float(polarity) if polarity else None)

# Qualifying conditions read from the full rules text. Any difference => near_equivalent.
CONDITIONS = {c: re.compile(p, re.I) for c, p in {
    "includes_phone_calls": r"including phone calls?|phone calls? (?:count|qualif|are included)",
    "in_person": r"in[- ]person|physically",
    "photo_or_video": r"photograph|videotape|same frame",
    "trilateral": r"trilateral",
    "becomes_law": r"becomes? law|signed into law",
}.items()}
# Read from the question only (rules text is boilerplate-heavy): a difference => near_equivalent.
QUESTION_CONDITIONS = {"whole_area": re.compile(r"\ball of\b|\bentire(?:ty)?\b|full control", re.I)}
# A negated question is the opposite bet ("agrees not to join NATO" vs "joins NATO"): never matched.
# "Military action against Iran ends" is the opposite of a strike in the same way.
# Checked outside parentheses ("... (not merely in an annex)" qualifies, it doesn't negate).
NOT_A_DATE = r"(?!(?:the )?(?:year|month|week|day|quarter|q[1-4]|\d|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec))"
NEGATION = re.compile(r"\b(?:not|never|no longer|without|fails? to|won't|refuses? to|ends|stops|halts|lift(?:s|ed)?)\b"
                      rf"|\bend\b(?! of)|\bend of {NOT_A_DATE}", re.I)

MON = (r"(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|june?|july?|aug(?:ust)?"
       r"|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)")
DATE = rf"{MON}\.?\s+(\d{{1,2}})(?:st|nd|rd|th)?(?:,?\s+(\d{{4}}))?"
CLOCK = r"(?:,?\s+\d{1,2}(?::\d\d)?\s*[ap]\.?m\.?(?:\s+[a-z]{2,4})?)?,?"   # ", 12:00 PM ET,"
MONTHS = {m: i for i, m in enumerate("jan feb mar apr may jun jul aug sep oct nov dec".split(), 1)}
# Words that carry no identity of their own; every other word in a question must be accounted for
# in the other market's text (so "abolish hukou" never matches "abolish birth limits").
STOPWORDS = set("""will would does what when which who whom whose that this these those there their
    them they with from into onto over under about after before between during until within
    by on in at of to for and or but not any all each other than then also such have has had
    been being were was are is be the a an new next more most least make made makes agree agrees
    agreed announce announces announced confirm confirmed confirms officially formal formally
    become becomes became under year years month months day days end yes no none""".split())

# ── signature ─────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Signature:
    key: tuple                      # (venue, market_id)
    event: tuple                    # (venue, event_id)
    action: str | None
    family: str | None
    kind: str | None
    symmetric: bool
    actors: frozenset               # entity ids before the action verb (all, if symmetric)
    targets: frozenset              # entity ids after it
    concepts: frozenset             # (entity id, surface) of concepts: what is acted on (hukou)
    terms: frozenset                # distinctive words not covered by entities/action/dates
    window: tuple | None            # (start, end) dates, inclusive, ET
    window_from_rules: bool
    threshold: tuple | None         # (op, value) or ("between", (lo, hi))
    conditions: frozenset
    negated: bool
    rules_words: frozenset          # names in the operative rules, for same-venue checks
    qwords: frozenset               # words of the question
    text: str = field(repr=False)   # lowercased question + rules, for name cross-checks
    issues: tuple = ()

    @property
    def parsed(self):
        return self.action is not None and self.window is not None

def _date(mon, day, year, default_year):
    return date(int(year) if year else default_year, MONTHS[mon[:3]], int(day))

def _et_date(dt):
    return (dt - timedelta(hours=5)).date() if dt else None  # venue close times are UTC; ~ET

def parse_window(text, open_d, close_d):
    """(start, end) inclusive dates from templated rules/question text, or None."""
    t, y = text.lower(), (close_d or open_d or date(2000, 1, 1)).year
    d = lambda g, i: _date(g[i], g[i + 1], g[i + 2], y)
    # a measurement period: "from Oct 19, 2026 to Oct 25, 2026", "between March 17-23", "July 6-July 12"
    if m := re.search(rf"\bfrom {DATE}{CLOCK} (?:to|through|until|-) {DATE}", t):
        g = m.groups()
        end = d(g, 3)
        return _date(g[0], g[1], g[2], end.year), end
    if m := re.search(rf"\b(?:between |from )?{MON}\.?\s+(\d{{1,2}})\s?(?:-|–|to)\s?(?:{MON}\.?\s+)?(\d{{1,2}})\b(?:,?\s+(\d{{4}}))?", t):
        mon1, d1, mon2, d2, yr = m.groups()
        end = _date(mon2 or mon1, d2, yr, y)
        start = _date(mon1, d1, yr, y)
        if start <= end: return start, end
    if m := re.search(rf"between (?:(?:this |the )?market(?:'s)? creation|{DATE}){CLOCK} and {DATE}", t):
        g = m.groups()
        end = d(g, 3)
        return (_date(g[0], g[1], g[2], end.year) if g[0] else open_d), end
    if m := re.search(rf"after (?:issuance|{DATE}),? and before {DATE}", t):
        g = m.groups()
        return (d(g, 0) if g[0] else open_d), d(g, 3) - timedelta(days=1)
    if m := re.search(rf"\bon or before {DATE}", t):
        return open_d, d(m.groups(), 0)
    if m := re.search(rf"\bon {DATE}", t):
        day = d(m.groups(), 0)
        return day, day
    if m := re.search(rf"\bin {MON}\.?\s+(\d{{4}})\b", t):
        start = date(int(m[2]), MONTHS[m[1][:3]], 1)
        return start, (start.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
    if m := re.search(rf"\bbefore {DATE}", t):
        return open_d, d(m.groups(), 0) - timedelta(days=1)
    if m := re.search(r"\bbefore (\d{4})\b", t):
        return open_d, date(int(m[1]) - 1, 12, 31)
    if m := re.search(rf"\bbefore {MON}\b(?!\.?\s+\d)(?:\s+(\d{{4}}))?", t):   # "before July" = by June 30
        first = date(int(m[2]) if m[2] else y, MONTHS[m[1][:3]], 1)
        return open_d, first - timedelta(days=1)
    if m := re.search(rf"\bby (?:the )?(?:end of )?{DATE}", t):
        return open_d, d(m.groups(), 0)
    if m := re.search(r"\b(?:by (?:the )?end of|in) (\d{4})\b", t):
        return (open_d if "end of" in m[0] else date(int(m[1]), 1, 1)), date(int(m[1]), 12, 31)
    return None

def parse_threshold(m, question):
    lo, hi, st = m.get("floor_strike"), m.get("cap_strike"), m.get("strike_type")
    if st in ("greater", "greater_or_equal") and lo is not None: return (">=" if st.endswith("equal") else ">", lo)
    if st in ("less", "less_or_equal") and hi is not None: return ("<=" if st.endswith("equal") else "<", hi)
    if st == "between" and lo is not None and hi is not None: return ("between", (lo, hi))
    q = question.lower()
    # Percentages or counts ("more than 260 transit calls", "225 or more ships"), never years.
    N = r"\$?(\d[\d,]*(?:\.\d+)?)%?"
    num = lambda x: float(x.replace(",", ""))
    year = lambda *xs: all(re.fullmatch(r"(?:19|20)\d\d", x) for x in xs)
    if (r := re.search(rf"between {N} and {N}", q)) and not year(r[1], r[2]): return ("between", (num(r[1]), num(r[2])))
    if (r := re.search(rf"at least {N}|{N} or (?:above|more|higher)", q)) and not year(r[1] or r[2]):
        return (">=", num(r[1] or r[2]))
    if (r := re.search(rf"(?:above|more than|over|greater than) {N}", q)) and not year(r[1]): return (">", num(r[1]))
    if (r := re.search(rf"(?:below|under|less than|fewer than) {N}", q)) and not year(r[1]): return ("<", num(r[1]))
    if (r := re.search(r"\b(\d+)\s?-\s?(\d+)\b(?!\s*(?:am|pm))", q)) and not year(r[1], r[2]):  # "20-21 seats"
        return ("between", (num(r[1]), num(r[2])))
    if r := re.search(r"\b(\d+)\+", q): return (">=", num(r[1]))                             # "225+"
    if r := re.search(r"\b(\d+) times?\b", q): return ("==", num(r[1]))                      # "strike Yemen 2 times"
    return None

def find_action(question, head):
    """(action, family, kind, symmetric, verb match). The question wins over the rules; the verb's
    position (question only) splits entities into actors and targets."""
    for src in (question, head):
        for a, f, k, s, rx in ACTIONS:
            if verb := rx.search(src):
                return a, f, k, s, verb if src is question else None
    return None

OPERATIVE = re.compile(r"[^.]*\bwill resolve to\b", re.I)

def operative(rules):
    """The sentence(s) that decide the market: from the first "... will resolve to" sentence (skipping
    a news preamble such as "On December 10, 2025, Ukraine hit ...", whose date is not the window)
    up to "Otherwise"; the whole start of the rules when there is no such sentence (Kalshi)."""
    start = (OPERATIVE.search(rules) or re.match("", rules)).start()
    return rules[start:].split("Otherwise")[0][:600]

def names(text):
    """Capitalised words in rules text ("Yablunieva", "E50") — what a templated rule is about."""
    return frozenset(w.lower() for w in re.findall(r"\b[A-Z][A-Za-z0-9-]{2,}", text)
                     if w.lower() not in STOPWORDS and w.lower()[:3] not in MONTHS) - {"yes", "et", "pm", "am"}

def words(text):
    """Distinctive words (4+ letters, not stopwords or months)."""
    return frozenset(w for w in re.findall(r"[a-z][a-z-]{3,}", text.lower())
                     if w not in STOPWORDS and w[:3] not in MONTHS)

def question_text(m):
    """The text a market is identified by: its title plus its outcome label."""
    return " ".join(filter(None, (m["title"], m.get("subtitle"))))

def parse(m, mentions):
    """Signature of one market. `mentions` = entities linked in question_text(m) (entities.link):
    named ones are actors/targets, concepts are what is acted on."""
    question = question_text(m)
    rules = m.get("rules_primary") or ""
    head = operative(rules)
    issues = []

    action = find_action(question, head)
    if not action: issues.append("no recognised action")
    name, family, kind, symmetric, verb = action or (None, None, None, False, None)
    if kind and RACE.search(question + " " + head): kind = "race"

    spans, actors, targets, concepts = [], set(), set(), set()
    for mt in mentions:
        spans.append((mt["start"], mt["end"]))
        if not mt["named"]:
            concepts.add((mt["id"], mt["surface"].lower()))
        else:
            (targets if verb and not symmetric and mt["start"] > verb.start() else actors).add(mt["id"])
    targets -= actors  # an actor repeated later (e.g. in the subtitle) stays an actor
    named = [mt["id"] for mt in sorted(mentions, key=lambda mt: mt["start"]) if mt["named"]]
    if action and not verb and not symmetric and len(set(named)) > 1:
        # verb only in the rules: no position to split on, so the subject (first-named) is the actor
        actors, targets = {named[0]}, set(named) - {named[0]}
    elif symmetric or not verb:
        actors, targets = actors | targets, set()

    if verb: spans.append(verb.span())
    spans += [hit.span() for rx in CONDITIONS.values() for hit in rx.finditer(question)]
    terms = {w.group().lower() for w in re.finditer(r"[A-Za-z][A-Za-z-]{3,}", question)
             if not any(a <= w.start() < b for a, b in spans)
             and w.group().lower() not in STOPWORDS and w.group().lower()[:3] not in MONTHS}

    open_d, close_d = _et_date(m.get("open_time")), _et_date(m.get("close_time"))
    # Rules first (exact); but rules are often written for the whole event ("No strike by Sept 15")
    # while the question names this market's own date ("on September 12"): the narrower one wins.
    rw, qw = parse_window(head, open_d, close_d), parse_window(question, open_d, close_d)
    inside = lambda x, y: (x[0] or date.min) >= (y[0] or date.min) and x[1] <= y[1]
    window = qw if rw and qw and qw != rw and inside(qw, rw) else rw or qw
    from_rules = window is not None
    if not window and close_d:
        window, _ = (open_d, close_d), issues.append("window taken from venue close time")
    if not window: issues.append("no time window")

    return Signature(
        key=(m["venue"], m["market_id"]), event=(m["venue"], m["event_id"]), action=name,
        family=family, kind=kind, symmetric=symmetric, actors=frozenset(actors),
        targets=frozenset(targets), concepts=frozenset(concepts), terms=frozenset(terms), window=window,
        window_from_rules=from_rules, threshold=parse_threshold(m, question),
        conditions=frozenset([c for c, rx in CONDITIONS.items() if rx.search(rules)]
                             + [c for c, rx in QUESTION_CONDITIONS.items() if rx.search(question)]),
        negated=bool(NEGATION.search(re.sub(r"\([^)]*\)", " ", question))), rules_words=names(head), qwords=words(question), text=(question + " " + rules).lower(), issues=tuple(issues))

# ── questions (ladders) ───────────────────────────────────────────────────────
def ladder_key(sig):
    """Propositions with equal keys are rungs of one question. A question is everything in the
    signature but its deadline: "China invades Taiwan by Jun 2026 / Dec 2026 / Dec 2027" is one
    question with three rungs. Threshold questions are one per measurement period: "more than 15 /
    20 / 25 transit calls, week of Oct 19-25" is a question; the next week is another (consecutive
    periods are separate events, not a ladder). None = unparsed: a question of its own."""
    if not sig or not sig.action: return None
    base = (sig.action, sig.kind, sig.symmetric, sig.actors, sig.targets, frozenset(c for c, _ in sig.concepts),
            sig.terms - THRESHOLD_WORDS, sig.conditions, sig.negated)
    return base + (("period", sig.window) if sig.threshold else ("deadlines",))

# Words that phrase a threshold ("fewer than 20", "above 15"): not part of what the question asks.
THRESHOLD_WORDS = frozenset("fewer less more above below under over least most exactly times".split())

# A question's name is its first rung's, minus the deadline (or plus the period) and threshold.
_DATEPHRASE = re.compile(rf"\s*\b(?:by|before|in|on|between|after|through|during|from)\s+(?:the )?(?:end of\s+)?"
                         rf"(?:{MON}\b|\d)[^?—]*?(?=\?|—|$)|\s+(?:end of|this|next) (?:\d{{4}}|year|week|month)\b", re.I)
_THRESH = re.compile(r"\b(?:more|fewer|less) than \$?[\d,.]+%?|\$?[\d,.]+%?\s*(?:or more|or fewer|\+)|\b\d+\s?-\s?\d+\b"
                     r"|\b(?:at least|above|below|under|over) \$?[\d,.]+%?|\b\d+ times?\b", re.I)

def question_name(name, sig):
    n = name.split(" — ")[0]
    if sig and sig.threshold:
        n = _THRESH.sub("N", n)
    out = re.sub(r"\s{2,}", " ", _DATEPHRASE.sub("", n)).strip().replace(" ?", "?") or n
    if sig and sig.threshold and sig.window and sig.window[0]:
        a, b = sig.window
        period = f"{a:%b} {a.day} – {b:%b} {b.day}, {b.year}" if a != b else f"{b:%b} {b.day}, {b.year}"  # %-d is not portable to Windows
        out = f"{out.rstrip('?')} ({period})?"
    return out

# ── comparison ────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Match:
    src: tuple             # for implies: src => dst
    dst: tuple
    relation: str          # same_as | near_equivalent | implies
    confidence: float
    diffs: tuple           # human-readable differences / notes

def _roots(ids, parents):
    """Each entity replaced by its top-level parent (Trump -> US, White House -> US, Xi -> CN)."""
    def root(e, seen=()):
        ps = [p for p in parents.get(e, ()) if p not in seen]
        return root(ps[0], seen + (e,)) if ps else e
    return frozenset(root(e) for e in ids)

def _label(ids):
    return ", ".join(sorted(i.split(":", 1)[1] for i in ids)) or "-"

def compare(a, b, parents, today, prices=None):
    """Match between two signatures as of `today`, or a string saying why they don't match.
    `prices` = {market key: probability on that day}, for the price check."""
    if not (a.parsed and b.parsed): return "unparsed"
    if a.family != b.family: return f"action: {a.action} vs {b.action}"
    if a.negated != b.negated: return "negated on one side (opposite bets)"
    if "race" in (a.kind, b.kind) and a.event != b.event: return "race outcome depends on the field of rivals"
    near, notes, implies = [], [], None

    # who / where
    roles = lambda s: (s.actors | s.targets,) if s.symmetric else (s.actors, s.targets)
    if roles(a) != roles(b):
        if tuple(_roots(x, parents) for x in roles(a)) != tuple(_roots(x, parents) for x in roles(b)):
            return f"entities/roles: {_label(a.actors)} -> {_label(a.targets)} vs {_label(b.actors)} -> {_label(b.targets)}"
        near.append(f"entities: {_label(a.actors | a.targets)} vs {_label(b.actors | b.targets)}")
    for x, y in ((a, b), (b, a)):  # what is acted on: x's concepts and words must be in y too
        ids = {c for c, _ in y.concepts}
        missing = sorted({s for c, s in x.concepts if c not in ids and s not in y.text}
                         | {t for t in x.terms if not re.search(rf"\b{re.escape(t)}", y.text)})
        if missing: return f"terms only on one side: {', '.join(missing)}"

    # what
    if a.action != b.action:
        near.append(f"action: {a.action} vs {b.action}")

    # how much
    if a.threshold != b.threshold:
        if not (a.threshold and b.threshold and a.threshold[0] == b.threshold[0] in (">", ">=")):
            return f"threshold: {a.threshold} vs {b.threshold}"
        implies = (a, b) if a.threshold[1] > b.threshold[1] else (b, a)  # higher bar => lower bar

    # when
    # Venues draw day boundaries differently (ET vs UTC), so across venues a multi-day window may be
    # off by a day. Within a venue, or for a single-day window ("on May 14"), dates must be exact —
    # else a daily ladder (on Jan 31, on Feb 1, ...) chains into one proposition.
    slack = a.key[0] != b.key[0] and a.window[0] != a.window[1] and b.window[0] != b.window[1]
    close = lambda x, y: x == y or (slack and x and y and abs((x - y).days) <= 1)
    same = lambda x, y: close(x[0], y[0]) and close(x[1], y[1])
    within = lambda x, y: (y[0] is None or (x[0] and x[0] >= y[0])) and x[1] <= y[1]
    elapsed_only = False
    if not same(a.window, b.window):
        open_now = a.kind == b.kind == "occurrence" and min(a.window[1], b.window[1]) >= today
        trim = lambda w: (max(w[0] or today, today), w[1])
        if open_now and same(trim(a.window), trim(b.window)) and not implies:
            elapsed_only = True  # same remaining window; needs the price check below
            notes.append(f"windows differ only in elapsed time ({a.window[0]} vs {b.window[0]} start)")
        else:
            nested = (a, b) if within(a.window, b.window) else (b, a) if within(b.window, a.window) else None
            if implies or not nested or "measure" in (a.kind, b.kind):
                return f"window: {a.window[0]}..{a.window[1]} vs {b.window[0]}..{b.window[1]}"
            implies = nested  # narrower window => wider window

    # resolution rules: one venue writes rules from one template, so operative rules naming
    # something neither question has (a different intersection in "captures Novopavlivka") mean
    # different bets even under identical questions; "Benjamin Netanyahu" vs "Netanyahu" doesn't
    extra = (a.rules_words ^ b.rules_words) - a.qwords - b.qwords
    if a.key[0] == b.key[0] and extra:
        near.append(f"same venue, different resolution rules: {', '.join(sorted(extra)[:4])}")

    # qualifying conditions
    if a.conditions != b.conditions:
        near.append(f"conditions: {_label({'c:' + c for c in a.conditions})} vs "
                    f"{_label({'c:' + c for c in b.conditions})}")

    base = 0.9 if a.window_from_rules and b.window_from_rules else 0.7
    if implies:
        if near: return "implication only claimed when all other terms are identical: " + "; ".join(near)
        return Match(implies[0].key, implies[1].key, "implies", base, tuple(notes))
    pa, pb = (prices or {}).get(a.key), (prices or {}).get(b.key)
    if not near and pa is not None and pb is not None and abs(pa - pb) > PRICE_TOLERANCE:
        near.append(f"prices disagree ({pa:.3f} vs {pb:.3f}): terms differ in a way the text doesn't show")
    elif not near and elapsed_only and (pa is None or pb is None):
        near.append("elapsed-time difference not verified: no recent price for both markets")
    elif not near and elapsed_only:
        notes.append(f"verified by prices ({pa:.3f} vs {pb:.3f})")
    if near:
        return Match(a.key, b.key, "near_equivalent", base - 0.2, tuple(near + notes))
    return Match(a.key, b.key, "same_as", base, tuple(notes))

def match_markets(markets, mentions, parents, prices_at=None):
    """Signatures, matches and rejected candidates (with reasons) between markets in different
    events. Candidates share an action family and the same top-level entities; same-event
    structure is handled by the ladder/mutex rules in graph_build.

    Each pair is evaluated at the first moment both markets are listed and priced (`listed_at`,
    `priced_at`), with prices from `prices_at({(key, when)}) -> {(key, when): probability}` —
    deterministic on re-runs. A market never priced is judged at its listing (price check skipped)."""
    ready = {(m["venue"], m["market_id"]): max(filter(None, (m.get("listed_at"), m.get("priced_at"))),
                                                default=datetime.now(timezone.utc)) for m in markets}
    sigs = {s.key: s for s in (parse(m, mentions.get((m["venue"], m["market_id"]), [])) for m in markets)}
    blocks = {}
    for s in sigs.values():
        if s.parsed:
            blocks.setdefault((s.family, _roots(s.actors | s.targets, parents)), []).append(s)
    pairs = {(a, b): max(ready[a.key], ready[b.key]) for block in blocks.values()
             for a, b in (sorted(p, key=lambda x: x.key) for p in combinations(block, 2))  # fixed order: stable notes
             if a.event != b.event}
    px = prices_at({(x.key, t) for (a, b), t in pairs.items() for x in (a, b)}) if prices_at else {}
    matches, rejected = [], []
    for (a, b), t in pairs.items():
        r = compare(a, b, parents, t.date(), {x.key: px.get((x.key, t)) for x in (a, b)})
        (matches.append(r) if isinstance(r, Match) else rejected.append((a.key, b.key, r)))
    return sigs, matches, rejected
