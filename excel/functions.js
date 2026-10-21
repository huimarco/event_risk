// Excel custom functions (=ER.*). Loaded by taskpane.html, which is the add-in's shared
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
      if (r.status === 404) throw fail(detail);  // e.g. unknown id
      throw fail(`API error ${r.status}: ${detail}`, CustomFunctions.ErrorCode.invalidValue);
    }
    return r.json();
  })();
  cache.set(url, { t: Date.now(), promise });
  promise.catch(() => cache.delete(url));  // don't cache failures
  return promise;
}

// Ids: question "q:...", proposition (one rung, blended across venues) "prop:...", venue contract
// "mkt:...". Returns the API record for any of them.
function kindOf(id) {
  const s = String(id ?? "").trim();
  if (s.startsWith("q:")) return "question";
  if (s.startsWith("prop:")) return "proposition";
  if (s.startsWith("mkt:")) return "market";
  throw fail("use an id from ER.QUESTIONS, ER.RUNGS or ER.CONTRACTS (q:..., prop:... or mkt:...)",
             CustomFunctions.ErrorCode.invalidValue);
}
const PATHS = { question: "questions", proposition: "propositions", market: "markets" };
function record(id) {
  const s = String(id).trim();
  return request(`/${PATHS[kindOf(s)]}/${encodeURIComponent(s)}`, {});
}

// A question's prob / change_24h are its headline rung's (the most-traded live rung), as in the API.
async function prob(id) {
  const r = await record(id);
  if (r.prob === null || r.prob === undefined) throw fail("no price in the last 7 days");
  return r.prob;
}

async function change(id) {
  if (kindOf(id) === "market") throw fail("CHANGE takes a q: or prop: id", CustomFunctions.ErrorCode.invalidValue);
  const r = await record(id);
  if (r.change_24h === null || r.change_24h === undefined) throw fail("no price 24h earlier");
  return r.change_24h;
}

async function name(id) {
  return (await record(id)).name;
}

// FIELD(id, field): one value from the record. Dates come back as Excel date serials.
const DATES = new Set(["updated", "close_time"]);
const list = v => Array.isArray(v) ? v.map(x => typeof x === "object" && x !== null ? (x.url || x.name || JSON.stringify(x)) : x).join(", ") : v;
const FIELDS = {
  common: {
    name: r => r.name, prob: r => r.prob, status: r => r.status, volume_24h: r => r.volume_24h,
    open_interest: r => r.open_interest, updated: r => r.updated, close_time: r => r.close_time },
  question: {
    change_24h: r => r.change_24h, venues: r => list(r.venues), kind: r => r.kind, n_rungs: r => r.n_rungs,
    n_live: r => r.n_live, headline: r => r.headline && r.headline.name, headline_id: r => r.headline && r.headline.id },
  proposition: {
    change_24h: r => r.change_24h, venues: r => list(r.venues), n_markets: r => r.markets.length,
    question: r => r.question && r.question.name, question_id: r => r.question && r.question.id },
  market: {
    venue: r => r.venue, title: r => r.title, subtitle: r => r.subtitle, event_title: r => r.event_title,
    rules: r => r.rules, rules_secondary: r => r.rules_secondary, resolution_sources: r => list(r.resolution_sources),
    outcomes: r => list(r.outcomes), match: r => r.match, proposition_id: r => r.proposition },
};
async function field(id, fieldName) {
  const kind = kindOf(id), f = String(fieldName ?? "").trim().toLowerCase();
  const get = FIELDS[kind][f] || FIELDS.common[f];
  if (!get) throw fail(`unknown field "${fieldName}" for a ${kind}; one of: ${
    [...Object.keys(FIELDS.common), ...Object.keys(FIELDS[kind])].join(", ")}`, CustomFunctions.ErrorCode.invalidValue);
  const v = get(await record(id));
  if (v === null || v === undefined || v === "") throw fail(`no ${f} for this ${kind}`);
  if (DATES.has(f)) return dateCell(v);
  return typeof v === "string" ? v.slice(0, 32767) : v;   // Excel's cell text limit
}

async function questions(query, limit, sort, entity) {
  const r = await request("/questions", { q: query, limit: limit || 20, sort: sort || "move", entity });
  return [["id", "name", "prob", "change_24h", "rungs", "headline"],
          ...r.questions.map(q => [q.id, q.name, blank(q.prob), blank(q.change_24h), q.n_rungs,
                                   q.headline ? q.headline.name : ""])];
}

// Questions involving an entity, by name ("Iran") or id ("wd:Q794"): live first, most traded first.
async function entity(nameOrId, limit) {
  if (!nameOrId) throw fail("entity name is required", CustomFunctions.ErrorCode.invalidValue);
  const e = await request(`/entities/${encodeURIComponent(String(nameOrId).trim())}`, {});
  return [["id", "name", "prob", "change_24h", "status", "volume_24h", "rungs"],
          ...e.questions.slice(0, limit || 20).map(q => [q.id, q.name, blank(q.prob), blank(q.change_24h), q.status,
                                                         blank(q.volume_24h), q.n_rungs])];
}

// A question's ladder: one row per rung (proposition), latest deadline first, as in the API.
async function rungs(id) {
  if (kindOf(id) !== "question") throw fail("question id (q:...) is required", CustomFunctions.ErrorCode.invalidValue);
  const q = await record(id);
  const head = q.headline && q.headline.id;
  return [["id", "name", "deadline", "threshold", "prob", "change_24h", "status", "headline"],
          ...q.rungs.map(p => [p.id, p.name, p.window ? dateCell(p.window[1]) : "", thresholdText(p.threshold),
                               blank(p.prob), blank(p.change_24h), p.status, p.id === head])];
}

// The venue contracts behind a question (its live rungs) or a proposition: how each joined
// (same_as = blended into the probability, near_equivalent = similar terms, not blended) and its rules.
async function contracts(id) {
  const kind = kindOf(id);
  if (kind === "market") throw fail("CONTRACTS takes a q: or prop: id; use FIELD for one contract", CustomFunctions.ErrorCode.invalidValue);
  const rows = kind === "question"
    ? (await request(`/questions/${encodeURIComponent(String(id).trim())}/contracts`, {})).contracts
    : (await record(id)).markets.map(m => ({ ...m, rung: null }));
  if (!rows.length) throw fail("no live contracts");
  return [["id", "venue", "name", "rung", "relation", "prob", "volume_24h", "open_interest", "updated", "close_time", "rules"],
          ...rows.map(m => [m.id, m.venue, m.name, m.rung ? m.rung.name : "", m.relation, blank(m.prob), blank(m.volume_24h),
                            blank(m.open_interest), dateCell(m.updated), dateCell(m.close_time), (m.rules || "").slice(0, 32767)])];
}

// Restated history: the proposition's current contracts, applied to every past day.
async function history(id, start, end, interval) {
  if (kindOf(id) === "market") throw fail("HISTORY takes a q: or prop: id", CustomFunctions.ErrorCode.invalidValue);
  const from = isoTime(start), to = isoTime(end);
  if (from && to && from > to) throw fail("start must be on or before end", CustomFunctions.ErrorCode.invalidValue);
  id = String(id).trim();
  if (id.startsWith("q:")) {  // a question's history = its current headline rung's
    const q = await record(id);
    if (!q.headline) throw fail("question has no live rung");
    id = q.headline.id;
  }
  const r = await request(`/propositions/${encodeURIComponent(id)}/history`,
                          { start: from, end: to, interval: interval || "1d" });
  return [["date", "prob", "n_markets"], ...r.points.map(p => [excelDate(p.t), blank(p.prob), p.n_markets])];
}

CustomFunctions.associate("QUESTIONS", questions);
CustomFunctions.associate("ENTITY", entity);
CustomFunctions.associate("PROB", prob);
CustomFunctions.associate("CHANGE", change);
CustomFunctions.associate("NAME", name);
CustomFunctions.associate("FIELD", field);
CustomFunctions.associate("RUNGS", rungs);
CustomFunctions.associate("CONTRACTS", contracts);
CustomFunctions.associate("HISTORY", history);

// Shared with the task pane.
window.EventRisk = { request, clearCache: () => cache.clear() };
