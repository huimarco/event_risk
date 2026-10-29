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

async function questions(query, limit, sort, entity, factor, industry) {
  const r = await request("/questions", { q: query, limit: limit || 20, sort: sort || "move", entity, factor, industry });
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

// A deadline question as a term structure: per live deadline, the probability by then (market and
// monotone fit), the probability in that period, the same conditional on not before (hazard per
// period) and the annualized hazard rate.
async function curve(id) {
  if (kindOf(id) !== "question") throw fail("question id (q:...) is required", CustomFunctions.ErrorCode.invalidValue);
  const c = await request(`/questions/${encodeURIComponent(String(id).trim())}/curve`, {});
  if (!c.points.length) throw fail("no live deadlines after today");
  // "continues?" questions: the curve is of the state ending (1 - price), so label it that way
  const p = c.event === "ends" ? "prob_ended" : "prob";
  return [["deadline", p, "fitted", "period_start", "period_prob", "conditional_prob", "hazard_annual"],
          ...c.points.map(p => [dateCell(p.deadline), p.prob, p.fitted, dateCell(p.period_start), p.period_prob,
                                blank(p.conditional_prob), blank(p.hazard_annual)])];
}

// P(event by a date) — e.g. before a bond matures — from a deadline question's fitted curve.
async function probBy(id, when, extrapolate) {
  if (kindOf(id) !== "question") throw fail("question id (q:...) is required", CustomFunctions.ErrorCode.invalidValue);
  const d = isoTime(when);
  if (!d) throw fail("date is required", CustomFunctions.ErrorCode.invalidValue);
  const c = await request(`/questions/${encodeURIComponent(String(id).trim())}/curve`,
                          { date: d.slice(0, 10), extrapolate: extrapolate ? "true" : undefined });
  if (!c.prob_by || c.prob_by.prob === null) throw fail("date is past the last deadline (set extrapolate to TRUE to extend the last hazard)");
  return c.prob_by.prob;
}

// Factors a question is relevant to, with the measured sensitivity: the factor's move (in its
// units, % or bp) per +1.0 of probability, and the move if it resolved YES / NO now.
async function factorsOf(id) {
  const kind = kindOf(id);
  if (kind === "market") throw fail("FACTORS takes a q: or prop: id", CustomFunctions.ErrorCode.invalidValue);
  const r = await record(id);
  if (!r.factors.length) throw fail("no factors linked");
  return [["factor_id", "name", "units", "beta", "se", "per_10pts", "move_if_yes", "move_if_no", "n_events", "level"],
          ...r.factors.map(f => { const s = f.sensitivity || {};
            return [f.id, f.name, f.units || "", blank(s.beta), blank(s.se), blank(s.per_10pts), blank(s.move_if_yes),
                    blank(s.move_if_no), blank(s.n_events), blank(f.level)]; })];
}

// FACTOR(factor_id, [field]): the factor's latest level (default) or one catalog field.
const FACTOR_FIELDS = ["level", "level_date", "name", "layer", "family", "units", "series", "definition"];
async function factor(id, fieldName) {
  const f = String(fieldName ?? "level").trim().toLowerCase() || "level";
  if (!FACTOR_FIELDS.includes(f)) throw fail(`unknown field "${fieldName}"; one of: ${FACTOR_FIELDS.join(", ")}`,
                                             CustomFunctions.ErrorCode.invalidValue);
  const r = await request(`/factors/${encodeURIComponent(String(id).trim().replace(/^factor:/, ""))}`, { limit: 0 });
  const v = r[f];
  if (v === null || v === undefined) throw fail(`no ${f} for this factor`);
  return f === "level_date" ? dateCell(v) : v;
}

async function sensitivityOf(id, factorId) {
  const r = await record(id);
  const fid = String(factorId ?? "").trim().replace(/^factor:/, "");
  const f = r.factors.find(x => x.id === fid);
  if (!f) throw fail(`${fid} is not linked to this question`);
  if (!f.sensitivity) throw fail("not measured yet");
  return f;
}

// Factor move per +1.0 of the question's probability (shrunk estimate), in the factor's units.
async function sensitivity(id, factorId) {
  return (await sensitivityOf(id, factorId)).sensitivity.beta;
}

// Portfolio P&L if the question resolves now, computed in Excel: sum over factors of loading x the
// factor's move_if_yes (or move_if_no). Loadings are your holdings' P&L per unit of each factor's
// move (per 1% for % factors, per 1 bp for bp factors); they never leave the workbook.
async function stress(id, factorIds, loadings, outcome) {
  const which = String(outcome ?? "yes").trim().toLowerCase() || "yes";
  if (!["yes", "no"].includes(which)) throw fail('outcome must be "yes" or "no"', CustomFunctions.ErrorCode.invalidValue);
  const ids = factorIds.flat(), ls = loadings.flat();
  if (ids.length !== ls.length) throw fail("factor ids and loadings must be the same size", CustomFunctions.ErrorCode.invalidValue);
  const r = await record(id);
  const by = Object.fromEntries(r.factors.map(f => [f.id, f.sensitivity]));
  let total = 0;
  ids.forEach((f, i) => {
    const s = by[String(f ?? "").trim().replace(/^factor:/, "")];
    const l = Number(ls[i]);
    if (s && Number.isFinite(l) && s[`move_if_${which}`] !== null) total += l * s[`move_if_${which}`];
  });
  return total;
}

// INDUSTRIES([query], [sector]): browse GICS industries to find codes for ER.QUESTIONS industry filter.
// Returns id (gics:XXXXXX), name, code, sector, industry_group, n_issuers, n_factors.
async function industries(query, sector) {
  const r = await request("/industries", { sector: sector || undefined });
  const rows = query ? r.filter(x => x.name.toLowerCase().includes(String(query).toLowerCase())
                                  || (x.sector || "").toLowerCase().includes(String(query).toLowerCase())) : r;
  if (!rows.length) throw fail("no industries found");
  return [["id", "name", "code", "sector", "industry_group", "n_issuers", "n_factors"],
          ...rows.map(x => [x.id, x.name, x.code, x.sector || "", x.industry_group || "", x.n_issuers, x.n_factors])];
}

// ISSUERS([query], [limit]): search issuers by name fragment; returns id, name, country, sector,
// sub-industry and number of entity exposures. Use the id column in ER.EXPOSURES.
async function issuers(query, limit) {
  const r = await request("/issuers", { q: query || undefined });
  const rows = limit ? r.slice(0, Number(limit)) : r;
  if (!rows.length) throw fail("no issuers found");
  return [["id", "name", "country", "sector", "sub_industry", "n_exposures"],
          ...rows.map(x => [x.id, x.name, blank(x.country), blank(x.sector), blank(x.sub_industry), x.n_exposures])];
}

// EXPOSURES(issuer_id, [limit], [sort]): questions the issuer is exposed to, sorted by risk
// (|impact| × √(p(1−p))). score = relevance 0–1; impact = signed (+ = issuer benefits if YES);
// risk = uncertainty-weighted impact. issuer_id from ER.ISSUERS or the web (iss:Q...).
async function exposures(issuerId, limit, sort) {
  const r = await request(`/issuers/${encodeURIComponent(String(issuerId).trim())}/exposures`,
                          { limit: limit || 50, sort: sort || "risk" });
  if (!r.exposures.length) throw fail("no exposures found");
  return [["question_id", "name", "prob", "change_24h", "score", "impact", "direction", "risk"],
          ...r.exposures.map(e => [e.id, e.name, blank(e.prob), blank(e.change_24h),
                                   e.score, blank(e.impact), e.direction || "", blank(e.risk)])];
}

// SECURITIES(question_id, [limit]): top securities with a measured sensitivity to a question
// (from security_betas.py). beta = % move per +1.0 probability; move_if_yes/no = at the current price.
async function securities(id, limit) {
  if (kindOf(id) === "market") throw fail("SECURITIES takes a q: or prop: id", CustomFunctions.ErrorCode.invalidValue);
  const r = await record(id);
  const rows = (r.securities || []).slice(0, limit || 15);
  if (!rows.length) throw fail("no measured securities for this question");
  return [["ticker", "name", "issuer", "beta", "move_if_yes", "move_if_no", "n_events"],
          ...rows.map(s => {
            const sens = s.sensitivity || {};
            return [s.ticker, s.name, s.issuer ? s.issuer.name : "",
                    blank(sens.beta), blank(sens.move_if_yes), blank(sens.move_if_no), blank(sens.n_events)];
          })];
}

// LOADINGS(ticker): a security's factor loadings — weekly % move per unit factor move, controlling
// for the market (security_betas.py, 156-week window). |t| ≥ 3 rows listed first.
async function loadings(ticker) {
  const r = await request(`/securities/${encodeURIComponent(String(ticker).trim())}`, {});
  if (!r.loadings || !r.loadings.length) throw fail("no factor loadings for this security");
  return [["factor_id", "factor_name", "units", "beta", "se", "t", "significant"],
          ...r.loadings.map(l => [l.factor.id, l.factor.name, l.factor.units || "",
                                  blank(l.beta), blank(l.se), blank(l.t), l.significant])];
}

// SECURITY_EXPOSURES(ticker, [limit], [sort]): questions a security moves with (measured betas).
// sort: "risk" (default), "move" (|beta|), or "evidence" (n_events). found_by = factor/structural/both.
async function securityExposures(ticker, limit, sort) {
  const r = await request(`/securities/${encodeURIComponent(String(ticker).trim())}/exposures`,
                          { limit: limit || 50, sort: sort || "risk" });
  if (!r.exposures || !r.exposures.length) throw fail("no measured exposures for this security");
  return [["question_id", "name", "prob", "change_24h", "beta", "move_if_yes", "move_if_no", "risk", "found_by"],
          ...r.exposures.map(e => {
            const s = e.sensitivity || {};
            return [e.id, e.name, blank(e.prob), blank(e.change_24h), blank(s.beta),
                    blank(s.move_if_yes), blank(s.move_if_no), blank(e.risk), s.found_by || ""];
          })];
}

// FACTOR_SECURITIES(factor_id, [limit]): securities with a measured loading on a factor, |t| first.
// beta = weekly % move per 1% (or 1 bp) factor move, controlling for the market.
async function factorSecurities(factorId, limit) {
  const fid = String(factorId ?? "").trim().replace(/^factor:/, "");
  const r = await request(`/factors/${encodeURIComponent(fid)}/securities`, { limit: limit || 50 });
  if (!r.securities || !r.securities.length) throw fail(r.note || "no measured loadings for this factor");
  return [["ticker", "name", "issuer", "units", "beta", "se", "t", "significant"],
          ...r.securities.map(s => [s.security.ticker || s.security.name, s.security.name,
                                    s.issuer ? s.issuer.name : "", r.factor.units || "",
                                    blank(s.beta), blank(s.se), blank(s.t), s.significant])];
}

// FACTOR_HISTORY(factor_id, [start], [end]): daily levels of a factor (default: last year).
// Returns date and level in the factor's units (% for indices/FX, bp for credit spreads).
async function factorHistory(factorId, start, end) {
  const fid = String(factorId ?? "").trim().replace(/^factor:/, "");
  const from = isoTime(start), to = isoTime(end);
  const r = await request(`/factors/${encodeURIComponent(fid)}/history`,
                          { start: from ? from.slice(0, 10) : undefined, end: to ? to.slice(0, 10) : undefined });
  if (!r.points || !r.points.length) throw fail(r.note || "no price history for this factor");
  return [["date", "level"], ...r.points.map(p => [excelDate(p.date), p.level])];
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
CustomFunctions.associate("CURVE", curve);
CustomFunctions.associate("PROB_BY", probBy);
CustomFunctions.associate("FACTORS", factorsOf);
CustomFunctions.associate("FACTOR", factor);
CustomFunctions.associate("SENSITIVITY", sensitivity);
CustomFunctions.associate("STRESS", stress);
CustomFunctions.associate("INDUSTRIES", industries);
CustomFunctions.associate("ISSUERS", issuers);
CustomFunctions.associate("EXPOSURES", exposures);
CustomFunctions.associate("SECURITIES", securities);
CustomFunctions.associate("LOADINGS", loadings);
CustomFunctions.associate("SECURITY_EXPOSURES", securityExposures);
CustomFunctions.associate("FACTOR_SECURITIES", factorSecurities);
CustomFunctions.associate("FACTOR_HISTORY", factorHistory);

// Shared with the task pane.
window.EventRisk = { request, clearCache: () => cache.clear() };
