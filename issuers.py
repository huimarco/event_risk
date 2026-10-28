#!/usr/bin/env python3
"""
Issuers (issuers.csv) and their listed securities (securities.csv).

issuers.csv: the companies (and, by hand, any other borrower) events reach through entities.
issuer_id = the issuer's Wikidata id (so an entity that is itself an issuer links to it), or
`local:<name>` for issuers with no Wikidata item (many private borrowers).
securities.csv: tickers (Yahoo symbols), each with its issuer_id (blank for funds / ETFs) and
`primary` = the listing used when one series must stand for the issuer (backtest.py). Which
securities move with which questions is measured, not written down (security_betas.py).

Refreshes S&P 500 members from Wikipedia's "List of S&P 500 companies". Run manually when the
index changes (quarterly rebalances), review the diff, and commit:

  python issuers.py

Adds new members, updates sector / sub-industry / headquarters for current ones, and clears the
index flag on companies that left (their rows stay). Share classes of one company (GOOG / GOOGL)
are one issuer with several securities. Hand-written rows are kept as they are. Tickers are
Yahoo symbols (BRK.B -> BRK-B).
"""
import csv, re
from html.parser import HTMLParser
from pathlib import Path

import entities
from collector import chunks

PATH = Path(__file__).parent / "issuers.csv"
COLUMNS = ["issuer_id", "name", "country", "sector", "sub_industry", "headquarters", "lei", "index"]
SEC_PATH = Path(__file__).parent / "securities.csv"
SEC_COLUMNS = ["ticker", "name", "issuer_id", "asset_class", "country", "primary", "index"]
INDEX = "sp500"
US_STATES = {s.strip() for s in """Alabama, Alaska, Arizona, Arkansas, California, Colorado,
    Connecticut, Delaware, Florida, Georgia, Hawaii, Idaho, Illinois, Indiana, Iowa, Kansas, Kentucky,
    Louisiana, Maine, Maryland, Massachusetts, Michigan, Minnesota, Mississippi, Missouri, Montana,
    Nebraska, Nevada, New Hampshire, New Jersey, New Mexico, New York, North Carolina, North Dakota,
    Ohio, Oklahoma, Oregon, Pennsylvania, Rhode Island, South Carolina, South Dakota, Tennessee, Texas,
    Utah, Vermont, Virginia, Washington, West Virginia, Wisconsin, Wyoming, D.C.""".split(",")}

class Table(HTMLParser):
    """Rows of the table with id="constituents": cell texts plus each cell's first link title."""
    def __init__(self):
        super().__init__()
        self.rows, self.on, self.row, self.cell, self.link = [], False, None, None, None
    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "table" and a.get("id") == "constituents": self.on = True
        elif self.on and tag == "tr": self.row = []
        elif self.on and tag == "td": self.cell, self.link = "", None
        elif self.on and tag == "a" and self.cell is not None and self.link is None: self.link = a.get("title")
    def handle_endtag(self, tag):
        if tag == "table" and self.on: self.on = False
        elif self.on and tag == "td" and self.row is not None:
            self.row.append((self.cell.strip(), self.link)); self.cell = None
        elif self.on and tag == "tr" and self.row:
            self.rows.append(self.row); self.row = None
    def handle_data(self, data):
        if self.cell is not None: self.cell += data

def constituents():
    html = entities.wiki(dict(action="parse", page="List of S&P 500 companies", prop="text", section=1))["parse"]["text"]
    t = Table(); t.feed(html)
    out = []
    for (sym, _), (name, article), (sector, _), (sub, _), (hq, _), *_ in t.rows:
        hq = re.sub(r"\[\w+\]", "", hq).strip()          # drop footnote markers: "Florida[2]"
        hq = "" if hq.lower() in ("none", "n/a") else hq  # remote-first companies list no HQ
        state = hq.rsplit(",", 1)[-1].strip()
        out.append(dict(ticker=sym.replace(".", "-"), name=name,
                        country="US" if state in US_STATES or hq in US_STATES else state if hq else "",
                        sector=sector, sub_industry=sub, headquarters=hq, article=article, index=INDEX))
    return out

def slug(name):
    return "local:" + re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")

def main():
    members = constituents()
    qids = {}
    for batch in chunks({m["article"] for m in members if m["article"]}, 50):
        qids |= {t: (hit or {}).get("qid", "") for t, hit in entities.fetch_titles(batch).items()}
    with open(PATH) as f:
        rows = {r["issuer_id"]: {c: r.get(c, "") or "" for c in COLUMNS} for r in csv.DictReader(f)}
    # Ids are stable: a listing whose ticker already belongs to an issuer keeps that issuer's id,
    # even if its Wikipedia article now resolves elsewhere (reported, not applied).
    with open(SEC_PATH) as f:
        secs = {r["ticker"]: {c: r.get(c, "") or "" for c in SEC_COLUMNS} for r in csv.DictReader(f)}
    known = {t: r["issuer_id"] for t, r in secs.items() if r["issuer_id"]}
    by_issuer = {}
    for m in members:  # share classes (GOOG / GOOGL) resolve to one article: one issuer
        found = qids.get(m["article"]) or slug(m["name"])
        iid = known.get(m["ticker"], found)
        if iid != found and not found.startswith("local:"):
            print(f"note: {m['ticker']} is {iid} in issuers.csv but its article now resolves to {found}; kept {iid}")
        by_issuer.setdefault(iid, []).append(m)
    added = [i for i in by_issuer if i not in rows]
    for iid, ms in by_issuer.items():
        m = min(ms, key=lambda m: (len(m["name"]), -len(m["ticker"])))  # "FedEx" over "FedEx Freight"; GOOGL over GOOG
        old = rows.get(iid)
        rows[iid] = (old | {c: m[c] for c in ("sector", "sub_industry", "headquarters", "index")} if old else
                     dict(issuer_id=iid, name=m["name"], country=m["country"], sector=m["sector"], sub_industry=m["sub_industry"],
                          headquarters=m["headquarters"], lei="", index=INDEX))
        has_primary = any(r["issuer_id"] == iid and r["primary"] for r in secs.values())
        for x in ms:
            secs[x["ticker"]] = (secs[x["ticker"]] | dict(index=INDEX) if x["ticker"] in secs else
                                 dict(ticker=x["ticker"], name=x["name"], issuer_id=iid, asset_class="equity", country=x["country"],
                                      primary="1" if x is m and not has_primary else "", index=INDEX))
    left = [i for i, r in rows.items() if r["index"] == INDEX and i not in by_issuer]
    for i in left:
        rows[i]["index"] = ""
    listed = {x["ticker"] for ms in by_issuer.values() for x in ms}
    for t, r in secs.items():
        if r["index"] == INDEX and t not in listed: r["index"] = ""
    with open(PATH, "w", newline="") as f:
        w = csv.DictWriter(f, COLUMNS, lineterminator="\n"); w.writeheader()
        w.writerows(sorted(rows.values(), key=lambda r: (r["index"] == INDEX, r["name"])))  # hand-written first
    with open(SEC_PATH, "w", newline="") as f:
        w = csv.DictWriter(f, SEC_COLUMNS, lineterminator="\n"); w.writeheader()
        w.writerows(sorted(secs.values(), key=lambda r: (r["asset_class"] != "etf", r["index"] == INDEX, r["ticker"])))
    print(f"{len(members)} {INDEX} listings = {len(by_issuer)} issuers ({len(added)} new, {len(left)} left); "
          f"{sum(1 for i in by_issuer if not i.startswith('local:'))} resolved to Wikidata; {len(rows)} issuers total")

if __name__ == "__main__":
    main()
