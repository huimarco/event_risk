// Excel custom functions (=EVENTRISK.*). Loaded by taskpane.html, which is the add-in's shared
// runtime, so these share localStorage (API key) and same-origin fetch with the pane and the API.

const CACHE_MS = 60 * 1000;  // identical requests within a minute share one response (recalcs, PROB+CHANGE+NAME)
const cache = new Map();     // url -> {t, promise}

function apiKey() { return localStorage.getItem("apiKey") || ""; }

function fail(message, code) {
  return new CustomFunctions.Error(code || CustomFunctions.ErrorCode.notAvailable, message);
}

// Excel passes dates as serial numbers (days since 1899-12-30); treat them as UTC.
function isoTime(v) {
  if (v === null || v === undefined || v === "") return undefined;
  if (typeof v === "number") return new Date(Math.round((v - 25569) * 86400000)).toISOString();
  const d = new Date(v);
  if (isNaN(d)) throw fail(`not a date: ${v}`, CustomFunctions.ErrorCode.invalidValue);
  return d.toISOString();
}

function excelDate(iso) { return Date.parse(iso) / 86400000 + 25569; }

function blank(v) { return v === null || v === undefined ? "" : v; }

// API timestamps ("2030-01-01 04:59:00+00:00") -> Excel date serial; blank if missing.
function dateCell(v) { return v ? excelDate(String(v).replace(" ", "T")) : ""; }

// Tickers as in securities.csv: upper case, Yahoo-style class shares (BRK.B -> BRK-B).
function normTicker(t) { return String(t).trim().toUpperCase().replace(".", "-"); }

// ["between", [lo, hi]] -> "20-21"; [">", 60] -> ">60"; null -> "".
function thresholdText(t) {
  if (!t) return "";
  return Array.isArray(t[1]) ? `${t[1][0]}-${t[1][1]}` : `${t[0]}${t[1]}`;
}

async function request(path, params) {
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(params || {})) if (v !== undefined && v !== null && v !== "") q.set(k, v);
  const url = `${path}${q.toString() ? "?" + q : ""}`;
  const hit = cache.get(url);
  if (hit && Date.now() - hit.t < CACHE_MS) return hit.promise;
  const promise = (async () => {
    const headers = {};
    if (apiKey()) headers["X-API-Key"] = apiKey();
    let r;
    try {
      r = await fetch(url, { headers });
    } catch (e) {
      throw fail("cannot reach the Event Risk API");
    }
    if (r.status === 401) throw fail("missing or invalid API key: set it in the Event Risk pane");
    if (!r.ok) {
      let detail = r.statusText;
      try { const d = (await r.json()).detail; detail = typeof d === "string" ? d : JSON.stringify(d); } catch (e) {}
      if (r.status === 404) throw fail(detail);  // e.g. not in the graph at this as_of / known_as_of
      throw fail(`API error ${r.status}: ${detail}`, CustomFunctions.ErrorCode.invalidValue);
    }
    return r.json();
  })();
  cache.set(url, { t: Date.now(), promise });
  promise.catch(() => cache.delete(url));  // don't cache failures
  return promise;
}

// Accepts a proposition id or a question id ("q:..."; what EXPOSURES returns). A question's
// prob / change_24h / name are its headline rung's, as in the API.
function proposition(id, asOf, knownAsOf) {
  if (!id) throw fail("id is required", CustomFunctions.ErrorCode.invalidValue);
  const kind = String(id).startsWith("q:") ? "questions" : "propositions";
  return request(`/${kind}/${encodeURIComponent(id)}`, { as_of: isoTime(asOf), known_as_of: isoTime(knownAsOf) });
}

async function prob(id, asOf, knownAsOf) {
  const p = await proposition(id, asOf, knownAsOf);
  if (p.prob === null || p.prob === undefined) throw fail("no price in the 7 days before as_of");
  return p.prob;
}

async function change(id, asOf, knownAsOf) {
  const p = await proposition(id, asOf, knownAsOf);
  if (p.change_24h === null || p.change_24h === undefined) throw fail("no price 24h earlier");
  return p.change_24h;
}

async function name(id) {
  return (await proposition(id)).name;
}

async function propositions(query, limit, sort, entity) {
  const r = await request("/propositions", { q: query, limit: limit || 20, sort: sort || "move", entity });
  return [["id", "name", "prob", "change_24h", "close_time"],
          ...r.propositions.map(p => [p.id, p.name, blank(p.prob), blank(p.change_24h), dateCell(p.close_time)])];
}

async function questions(query, limit, sort, entity) {
  const r = await request("/questions", { q: query, limit: limit || 20, sort: sort || "move", entity });
  return [["id", "name", "prob", "change_24h", "rungs", "headline"],
          ...r.questions.map(q => [q.id, q.name, blank(q.prob), blank(q.change_24h), q.n_rungs,
                                   q.headline ? q.headline.name : ""])];
}

// A question's ladder: one row per rung (proposition), latest deadline first, as in the API.
async function rungs(id, asOf, knownAsOf) {
  if (!String(id || "").startsWith("q:")) throw fail("question id (q:...) is required", CustomFunctions.ErrorCode.invalidValue);
  const q = await proposition(id, asOf, knownAsOf);
  const head = q.headline && q.headline.id;
  return [["id", "name", "deadline", "threshold", "prob", "change_24h", "status", "headline"],
          ...q.rungs.map(p => [p.id, p.name, p.window ? dateCell(p.window[1]) : "", thresholdText(p.threshold),
                               blank(p.prob), blank(p.change_24h), p.status, p.id === head])];
}

async function exposures(ticker, limit, minScore, asOf, sort) {
  if (!ticker) throw fail("ticker is required", CustomFunctions.ErrorCode.invalidValue);
  const r = await request(`/securities/${encodeURIComponent(normTicker(ticker))}/exposures`,
                          { limit: limit || 20, min_score: minScore ?? undefined, as_of: isoTime(asOf), sort });
  return [["id", "name", "prob", "change_24h", "score", "impact", "direction", "risk", "path"],
          ...r.exposures.map(e => [e.id, e.name, blank(e.prob), blank(e.change_24h), e.score, blank(e.impact),
                                   blank(e.direction), blank(e.risk), e.path.slice(1).map(n => n.name).join(" → ")])];
}

async function history(id, start, end, interval) {
  if (!id) throw fail("id is required", CustomFunctions.ErrorCode.invalidValue);
  if (String(id).startsWith("q:")) {  // a question's history = its current headline rung's
    const q = await proposition(id);
    if (!q.headline) throw fail("question has no live rung");
    id = q.headline.id;
  }
  const r = await request(`/propositions/${encodeURIComponent(id)}/history`,
                          { start: isoTime(start), end: isoTime(end), interval: interval || "1d" });
  return [["date", "prob", "n_markets"], ...r.points.map(p => [excelDate(p.t), blank(p.prob), p.n_markets])];
}

CustomFunctions.associate("PROB", prob);
CustomFunctions.associate("CHANGE", change);
CustomFunctions.associate("NAME", name);
CustomFunctions.associate("PROPOSITIONS", propositions);
CustomFunctions.associate("QUESTIONS", questions);
CustomFunctions.associate("RUNGS", rungs);
CustomFunctions.associate("EXPOSURES", exposures);
CustomFunctions.associate("HISTORY", history);

// Shared with the task pane.
window.EventRisk = { request, clearCache: () => cache.clear() };
