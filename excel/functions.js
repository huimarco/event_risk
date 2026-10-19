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

async function request(path, params, body) {
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(params || {})) if (v !== undefined && v !== null && v !== "") q.set(k, v);
  const url = `${path}${q.toString() ? "?" + q : ""}`;
  const key = body ? null : url;
  const hit = key && cache.get(key);
  if (hit && Date.now() - hit.t < CACHE_MS) return hit.promise;
  const promise = (async () => {
    const headers = { "Content-Type": "application/json" };
    if (apiKey()) headers["X-API-Key"] = apiKey();
    let r;
    try {
      r = await fetch(url, body ? { method: "POST", headers, body: JSON.stringify(body) } : { headers });
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
  if (key) {
    cache.set(key, { t: Date.now(), promise });
    promise.catch(() => cache.delete(key));  // don't cache failures
  }
  return promise;
}

function proposition(id, asOf, knownAsOf) {
  if (!id) throw fail("proposition_id is required", CustomFunctions.ErrorCode.invalidValue);
  return request(`/propositions/${encodeURIComponent(id)}`, { as_of: isoTime(asOf), known_as_of: isoTime(knownAsOf) });
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
          ...r.propositions.map(p => [p.id, p.name, blank(p.prob), blank(p.change_24h), blank(p.close_time)])];
}

async function exposures(ticker, limit, minScore, asOf) {
  if (!ticker) throw fail("ticker is required", CustomFunctions.ErrorCode.invalidValue);
  const r = await request(`/securities/${encodeURIComponent(String(ticker).trim().toUpperCase())}/exposures`,
                          { limit: limit || 20, min_score: minScore ?? undefined, as_of: isoTime(asOf) });
  return [["id", "name", "prob", "change_24h", "score", "path"],
          ...r.exposures.map(e => [e.id, e.name, blank(e.prob), blank(e.change_24h), e.score,
                                   e.path.slice(1).map(n => n.name).join(" → ")])];
}

async function history(id, start, end, interval) {
  if (!id) throw fail("proposition_id is required", CustomFunctions.ErrorCode.invalidValue);
  const r = await request(`/propositions/${encodeURIComponent(id)}/history`,
                          { start: isoTime(start), end: isoTime(end), interval: interval || "1d" });
  return [["date", "prob", "n_markets"], ...r.points.map(p => [excelDate(p.t), blank(p.prob), p.n_markets])];
}

// Rows of [ticker, weight]; rows whose weight isn't a number (headers, blanks) are skipped.
function parseHoldings(range) {
  return range.filter(row => row.length >= 2 && row[0] !== "" && typeof row[1] === "number")
              .map(row => ({ ticker: String(row[0]).trim().toUpperCase(), weight: row[1] }));
}

async function scan(holdings, limit, moveThreshold) {
  const h = parseHoldings(holdings);
  if (!h.length) throw fail("holdings must be two columns: ticker, numeric weight", CustomFunctions.ErrorCode.invalidValue);
  const r = await request("/portfolio/scan", {}, { holdings: h, limit: limit || 25,
                                                   move_threshold: moveThreshold ?? 0.05 });
  const rows = [["id", "name", "prob", "change_24h", "exposure", "moved", "top_holdings"],
                ...r.questions.map(p => [p.id, p.name, blank(p.prob), blank(p.change_24h), p.exposure, p.moved,
                                            p.holdings.map(c => c.ticker).join(", ")])];
  if (r.unknown_tickers.length)
    rows.push(["unknown tickers", r.unknown_tickers.join(", "), "", "", "", "", ""]);
  return rows;
}

CustomFunctions.associate("PROB", prob);
CustomFunctions.associate("CHANGE", change);
CustomFunctions.associate("NAME", name);
CustomFunctions.associate("PROPOSITIONS", propositions);
CustomFunctions.associate("EXPOSURES", exposures);
CustomFunctions.associate("HISTORY", history);
CustomFunctions.associate("SCAN", scan);

// Shared with the task pane.
window.EventRisk = { request, parseHoldings, clearCache: () => cache.clear() };
