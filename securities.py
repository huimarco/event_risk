#!/usr/bin/env python3
"""
Refresh index constituents in securities.csv from Wikipedia's "List of S&P 500 companies".
Run manually when the index changes (quarterly rebalances), review the diff, and commit:

  python securities.py

Adds new members, updates sector / sub-industry / headquarters / issuer for current ones, and
clears the index flag on companies that left (their rows and price history stay). Hand-written
rows (ETFs, FX, non-US listings) are kept as they are. Tickers are Yahoo symbols (BRK.B -> BRK-B).
Each company's Wikipedia article is resolved to its Wikidata id (issuer_qid), so securities can
be tied to issuer entities in the graph.
"""
import csv, re
from html.parser import HTMLParser
from pathlib import Path

import entities
from collector import chunks

PATH = Path(__file__).parent / "securities.csv"
COLUMNS = ["ticker", "name", "asset_class", "country", "role", "sector", "sub_industry",
           "headquarters", "issuer_qid", "index"]
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
        out.append(dict(ticker=sym.replace(".", "-"), name=name, asset_class="equity",
                        country="US" if state in US_STATES or hq in US_STATES else state if hq else "",
                        role="index_member", sector=sector, sub_industry=sub, headquarters=hq,
                        article=article, index=INDEX))
    return out

def main():
    members = constituents()
    qids = {}
    for batch in chunks({m["article"] for m in members if m["article"]}, 50):
        qids |= {t: (hit or {}).get("qid", "") for t, hit in entities.fetch_titles(batch).items()}
    with open(PATH) as f:
        rows = {r["ticker"]: {c: r.get(c, "") or "" for c in COLUMNS} for r in csv.DictReader(f)}
    current = {m["ticker"] for m in members}
    added = [m["ticker"] for m in members if m["ticker"] not in rows]
    for m in members:
        new = {c: m.get(c, "") for c in COLUMNS} | {"issuer_qid": qids.get(m["article"], "")}
        old = rows.get(m["ticker"])
        # rows this script made are replaced; hand-written rows keep name/class/country/role
        rows[m["ticker"]] = new if not old or old["role"] == "index_member" else old | {
            c: new[c] for c in ("sector", "sub_industry", "headquarters", "issuer_qid", "index")}
    left = [t for t, r in rows.items() if r["index"] == INDEX and t not in current]
    for t in left:
        rows[t]["index"] = ""
    with open(PATH, "w", newline="") as f:
        w = csv.DictWriter(f, COLUMNS); w.writeheader()
        w.writerows(sorted(rows.values(), key=lambda r: (r["role"] == "index_member", r["ticker"])))  # hand-written first
    print(f"{len(members)} {INDEX} members ({len(added)} new, {len(left)} left); "
          f"{sum(1 for m in members if qids.get(m['article']))} resolved to Wikidata; {len(rows)} securities total")

if __name__ == "__main__":
    main()
