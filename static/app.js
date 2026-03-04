const modeChipEl = document.getElementById("modeChip");
const statusEl = document.getElementById("status");
const alertsEl = document.getElementById("alerts");
const visibleCountEl = document.getElementById("visibleCount");

const metricTotalEl = document.getElementById("metricTotal");
const metricCryptoEl = document.getElementById("metricCrypto");
const metricRegularEl = document.getElementById("metricRegular");
const metricSentEl = document.getElementById("metricSent");

const searchInputEl = document.getElementById("searchInput");
const formFilterEl = document.getElementById("formFilter");
const refreshBtn = document.getElementById("refreshBtn");
const startBtn = document.getElementById("startBtn");
const stopBtn = document.getElementById("stopBtn");

let mode = "api";
let cachedStatus = null;
let allAlerts = [];
const ALERT_ORDER = { UNKNOWN: 0, LOW: 1, MEDIUM: 2, HIGH: 3 };

function escapeHtml(value) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function fmtDate(value) {
  if (!value) return "n/a";
  const dt = new Date(value);
  return Number.isNaN(dt.getTime()) ? "n/a" : dt.toLocaleString();
}

function fmtNumber(value) {
  const num = Number(value || 0);
  return Number.isFinite(num) ? num.toLocaleString() : "0";
}

function synopsisPreview(text) {
  const raw = String(text || "").trim();
  if (!raw) return "No synopsis generated for this filing yet.";
  const flat = raw.replace(/\s+/g, " ");
  return flat.length > 340 ? `${flat.slice(0, 340)}...` : flat;
}

function normalizeAlertLevel(value) {
  const upper = String(value || "").toUpperCase();
  if (upper.includes("CRITICAL")) return "HIGH";
  for (const level of ["HIGH", "MEDIUM", "LOW"]) {
    if (upper.includes(level)) return level;
  }
  return "UNKNOWN";
}

function parseSynopsisSections(text) {
  const raw = String(text || "");
  if (!raw.trim()) {
    return { items: [], whyThisMatters: [], wireRecommendation: "UNKNOWN" };
  }

  const splitRegex = /^\s*Synopsis\s+\d+\s*$/gim;
  const matches = [...raw.matchAll(splitRegex)];
  const blocks = [];

  if (!matches.length) {
    blocks.push(raw);
  } else {
    for (let i = 0; i < matches.length; i += 1) {
      const start = matches[i].index + matches[i][0].length;
      const end = i + 1 < matches.length ? matches[i + 1].index : raw.length;
      blocks.push(raw.slice(start, end));
    }
  }

  const labels = ["Filer", "ETF Name", "Strategy", "IS ALERT WORTHY", "Why this matters", "Synopsis"];
  const items = blocks
    .map((block) => {
      const extract = (label) => {
        const next = labels.join("|");
        const regex = new RegExp(`^\\s*${label}\\s*:\\s*([\\s\\S]*?)(?=^\\s*(?:${next})\\s*:?\\s*|$)`, "im");
        const match = block.match(regex);
        return match ? match[1].replace(/\s+/g, " ").trim() : "";
      };
      const filer = extract("Filer") || "Unknown";
      const etfName = extract("ETF Name") || "Unknown";
      const strategy = extract("Strategy") || "Not available.";
      const isAlertWorthy = normalizeAlertLevel(extract("IS ALERT WORTHY"));
      if ((filer === "Unknown") && (etfName === "Unknown") && (strategy === "Not available.")) {
        return null;
      }
      return { filer, etf_name: etfName, strategy, is_alert_worthy: isAlertWorthy };
    })
    .filter(Boolean);

  let whyThisMatters = [];
  const headingMatch = raw.match(/^\s*Why this matters\s*:?\s*$/im);
  const bulletZone = headingMatch ? raw.slice(headingMatch.index + headingMatch[0].length) : raw;
  whyThisMatters = bulletZone
    .split("\n")
    .map((line) => line.match(/^\s*[-*•]\s+(.+)$/))
    .filter(Boolean)
    .map((m) => m[1].replace(/\s+/g, " ").trim())
    .filter(Boolean)
    .slice(0, 3);

  let wireRecommendation = "UNKNOWN";
  for (const item of items) {
    const level = normalizeAlertLevel(item.is_alert_worthy);
    if ((ALERT_ORDER[level] || 0) > (ALERT_ORDER[wireRecommendation] || 0)) {
      wireRecommendation = level;
    }
  }

  return { items, whyThisMatters, wireRecommendation };
}

function getStructuredSynopsis(alert) {
  const items = Array.isArray(alert.synopsis_items) ? alert.synopsis_items : [];
  const whyThisMatters = Array.isArray(alert.why_this_matters) ? alert.why_this_matters : [];
  const wireRecommendation = normalizeAlertLevel(alert.wire_recommendation);

  if (items.length || whyThisMatters.length || wireRecommendation !== "UNKNOWN") {
    let best = wireRecommendation;
    for (const item of items) {
      const level = normalizeAlertLevel(item.is_alert_worthy);
      if ((ALERT_ORDER[level] || 0) > (ALERT_ORDER[best] || 0)) best = level;
    }
    return { items, whyThisMatters, wireRecommendation: best };
  }

  return parseSynopsisSections(alert.synopsis);
}

function normalizeCik(cik) {
  const digits = String(cik || "").replace(/\D+/g, "");
  if (!digits) return "";
  return String(Number.parseInt(digits, 10));
}

function normalizeAccessionDigits(accession) {
  return String(accession || "").replace(/\D+/g, "");
}

function buildEdgarIndexUrl(cik, accession) {
  const cleanCik = normalizeCik(cik);
  const rawAccession = String(accession || "").trim();
  const cleanAccession = normalizeAccessionDigits(accession);
  if (!cleanCik || !cleanAccession || !rawAccession) return "";
  return `https://www.sec.gov/Archives/edgar/data/${cleanCik}/${cleanAccession}/${rawAccession}-index.html`;
}

function canonicalizeSecUrl(url, alert) {
  const value = String(url || "").trim();
  if (!value) return "";
  try {
    const parsed = new URL(value);
    if (!/sec\.gov$/i.test(parsed.hostname)) return "";

    const path = parsed.pathname || "";
    const lowerPath = path.toLowerCase();
    if (lowerPath === "/ix") {
      const docPath = parsed.searchParams.get("doc") || "";
      if (docPath.toLowerCase().startsWith("/archives/")) return parsed.toString();
      return "";
    }
    if (!lowerPath.includes("/archives/")) return "";

    const last = lowerPath.split("/").filter(Boolean).pop() || "";
    if (last === "index.html" || last === "index.htm") {
      return buildEdgarIndexUrl(alert.cik, alert.accession_number);
    }

    if (!last.includes(".")) {
      return buildEdgarIndexUrl(alert.cik, alert.accession_number);
    }
    return parsed.toString();
  } catch (_) {
    return "";
  }
}

function isUsableSecFilingUrl(url) {
  const value = String(url || "").trim();
  if (!value) return false;
  try {
    const parsed = new URL(value);
    if (!/sec\.gov$/i.test(parsed.hostname)) return false;
    const path = parsed.pathname || "";
    if (path === "/" || path === "") return false;
    const lowerPath = path.toLowerCase();
    if (lowerPath === "/ix") {
      const docPath = parsed.searchParams.get("doc") || "";
      return docPath.toLowerCase().startsWith("/archives/");
    }
    if (!lowerPath.includes("/archives/")) return false;

    const last = lowerPath.split("/").filter(Boolean).pop() || "";
    if (!last.includes(".")) return false;
    if (last === "index.html" || last === "index.htm") return false;
    return true;
  } catch (_) {
    return false;
  }
}

function toIxViewerUrl(url) {
  const value = String(url || "").trim();
  if (!value) return "";
  try {
    const parsed = new URL(value);
    if (!/sec\.gov$/i.test(parsed.hostname)) return "";
    const lowerPath = parsed.pathname.toLowerCase();
    if (lowerPath === "/ix") return parsed.toString();
    if (!lowerPath.includes("/archives/")) return "";
    const last = lowerPath.split("/").filter(Boolean).pop() || "";
    if (!last.includes(".")) return "";
    if (last === "index.html" || last === "index.htm") return "";
    return `https://www.sec.gov/ix?doc=${parsed.pathname}`;
  } catch (_) {
    return "";
  }
}

function resolveLinks(alert) {
  const candidates = [
    canonicalizeSecUrl(alert.primary_document_url, alert),
    canonicalizeSecUrl(alert.sec_filing_url, alert),
    canonicalizeSecUrl(alert.sec_index_url, alert),
  ];
  const best = candidates.find((item) => isUsableSecFilingUrl(item))
    || buildEdgarIndexUrl(alert.cik, alert.accession_number)
    || "#";

  const primaryCandidate = canonicalizeSecUrl(alert.primary_document_url, alert);
  const primary = isUsableSecFilingUrl(primaryCandidate) ? primaryCandidate : "";
  const viewer = toIxViewerUrl(best);

  return {
    secLink: viewer || best,
    primaryLink: primary && primary !== (viewer || best) ? primary : "",
  };
}

async function jsonFetch(url, method = "GET", body = null) {
  const opts = { method, headers: { "Content-Type": "application/json" } };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(url, opts);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `Request failed: ${res.status}`);
  }
  return res.json();
}

async function detectMode() {
  try {
    await jsonFetch("/api/health");
    mode = "api";
  } catch (_) {
    mode = "static";
  }
}

function setModeChip() {
  if (!modeChipEl) return;
  if (mode === "api") {
    modeChipEl.classList.add("live");
    modeChipEl.classList.remove("scheduled");
    modeChipEl.textContent = "Mode: API Live Stream";
    return;
  }
  modeChipEl.classList.add("scheduled");
  modeChipEl.classList.remove("live");
  modeChipEl.textContent = "Mode: GitHub Scheduled Poller";
}

async function fetchStatus() {
  if (mode === "api") {
    return jsonFetch("/api/status");
  }
  return jsonFetch("./data/status.json");
}

async function fetchAlerts() {
  const alerts = mode === "api" ? await jsonFetch("/api/alerts") : await jsonFetch("./data/alerts.json");
  return Array.isArray(alerts) ? alerts : [];
}

function updateMetrics(alerts) {
  const total = alerts.length;
  const crypto = alerts.filter((alert) => Boolean(alert.is_crypto)).length;
  const regular = alerts.filter((alert) => {
    const structured = getStructuredSynopsis(alert);
    return ["MEDIUM", "HIGH", "CRITICAL"].includes(structured.wireRecommendation);
  }).length;
  const sent = alerts.filter((alert) => Boolean(alert.email_sent)).length;

  if (metricTotalEl) metricTotalEl.textContent = fmtNumber(total);
  if (metricCryptoEl) metricCryptoEl.textContent = fmtNumber(crypto);
  if (metricRegularEl) metricRegularEl.textContent = fmtNumber(regular);
  if (metricSentEl) metricSentEl.textContent = fmtNumber(sent);
}

function updateStatusLine(status) {
  if (!statusEl) return;

  if (mode === "api") {
    const summary =
      `stream=${status.running ? "running" : "stopped"} ` +
      `connection=${status.connected ? "connected" : "disconnected"} ` +
      `processed=${fmtNumber(status.processed_count)} ` +
      `alerted=${fmtNumber(status.alerted_count)} ` +
      `last_event=${fmtDate(status.last_event_at)}`;
    statusEl.textContent = status.last_error ? `${summary} | error=${status.last_error}` : summary;
    return;
  }

  const summary =
    `scheduler=every_10m fetched=${fmtNumber(status.fetched_entries)} ` +
    `feed=${fmtNumber(status.feed_entries)} ` +
    `backfill=${fmtNumber(status.backfill_entries)} ` +
    `backfill_days=${fmtNumber(status.backfill_days)} ` +
    `repaired_links=${fmtNumber(status.repaired_links)} ` +
    `refreshed_synopsis=${fmtNumber(status.refreshed_synopsis)} ` +
    `new_alerts=${fmtNumber(status.new_alerts)} ` +
    `total_alerts=${fmtNumber(status.total_alerts)} ` +
    `last_run=${fmtDate(status.last_run)}`;
  statusEl.textContent = status.last_error ? `${summary} | error=${status.last_error}` : summary;
}

function filterAlerts() {
  const formFilter = formFilterEl ? formFilterEl.value : "ALL";
  const query = searchInputEl ? searchInputEl.value.trim().toLowerCase() : "";

  return allAlerts
    .filter((alert) => {
      const form = String(alert.form_type || "").toUpperCase();
      if (formFilter !== "ALL" && form !== formFilter) return false;
      if (!query) return true;

      const haystack = [
        alert.company_name,
        alert.cik,
        alert.accession_number,
        alert.synopsis,
        alert.form_type,
      ]
        .join(" ")
        .toLowerCase();

      return haystack.includes(query);
    })
    .sort((a, b) => {
      const aTime = new Date(a.created_at || a.updated || 0).getTime();
      const bTime = new Date(b.created_at || b.updated || 0).getTime();
      return bTime - aTime;
    });
}

function renderAlerts(alerts) {
  if (!alertsEl) return;

  if (!alerts.length) {
    alertsEl.innerHTML = '<div class="empty-state">No filings match the current filter.</div>';
    return;
  }

  alertsEl.innerHTML = alerts
    .map((alert) => {
      const form = escapeHtml(alert.form_type || "UNKNOWN");
      const company = escapeHtml(alert.company_name || "Unknown filer");
      const cik = escapeHtml(alert.cik || "n/a");
      const accession = escapeHtml(alert.accession_number || "n/a");
      const created = fmtDate(alert.created_at || alert.updated);
      const keywords = Array.isArray(alert.matched_keywords) && alert.matched_keywords.length
        ? escapeHtml(alert.matched_keywords.join(", "))
        : "none";
      const structured = getStructuredSynopsis(alert);
      const synopsis = escapeHtml(synopsisPreview(alert.synopsis));
      const links = resolveLinks(alert);
      const secLink = escapeHtml(links.secLink);
      const primaryLink = links.primaryLink ? escapeHtml(links.primaryLink) : "";
      const hasError = Boolean(alert.error);
      const wireBadge = structured.wireRecommendation !== "UNKNOWN"
        ? `<span class="tag wire ${structured.wireRecommendation.toLowerCase()}">WIRE ${escapeHtml(structured.wireRecommendation)}</span>`
        : "";

      const structuredHtml = structured.items.length
        ? `
          <div class="synopsis-structured">
            ${structured.items.map((item, index) => `
              <section class="synopsis-item">
                <p class="synopsis-item-title">Synopsis ${index + 1}</p>
                <p><strong>Filer:</strong> ${escapeHtml(item.filer || "Unknown")}</p>
                <p><strong>ETF Name:</strong> ${escapeHtml(item.etf_name || "Unknown")}</p>
                <p><strong>Strategy:</strong> ${escapeHtml(item.strategy || "Not available.")}</p>
                <p><strong>Is Alert Worthy:</strong> ${escapeHtml(normalizeAlertLevel(item.is_alert_worthy))}</p>
              </section>
            `).join("")}
            ${structured.whyThisMatters.length ? `
              <section class="why-matters">
                <p class="synopsis-item-title">Why This Matters</p>
                <ul>
                  ${structured.whyThisMatters.map((bullet) => `<li>${escapeHtml(bullet)}</li>`).join("")}
                </ul>
              </section>
            ` : ""}
          </div>
        `
        : `<p class="synopsis">${synopsis}</p>`;

      return `
      <article class="filing-card">
        <div class="filing-top">
          <div>
            <h3 class="filing-title">${company}</h3>
            <p class="filing-meta">CIK ${cik} | ACCESSION ${accession} | FILED ${created}</p>
          </div>
          <div class="tag-row">
            <span class="tag form">${form}</span>
            ${alert.is_crypto ? '<span class="tag crypto">CRYPTO</span>' : ""}
            ${wireBadge}
            <span class="tag ${alert.email_sent ? "sent" : "error"}">${alert.email_sent ? "EMAIL SENT" : "EMAIL ISSUE"}</span>
          </div>
        </div>
        <p class="filing-meta">Matched Keywords: ${keywords}</p>
        ${structuredHtml}
        <div class="link-row">
          <a class="link-btn" href="${secLink}" target="_blank" rel="noreferrer">Open SEC Filing</a>
          ${primaryLink ? `<a class="link-btn secondary" href="${primaryLink}" target="_blank" rel="noreferrer">Open Primary Doc</a>` : ""}
        </div>
        ${hasError ? `<p class="error-line">Error: ${escapeHtml(alert.error)}</p>` : ""}
      </article>`;
    })
    .join("");
}

function applyFiltersAndRender() {
  const filtered = filterAlerts();
  if (visibleCountEl) {
    visibleCountEl.textContent = fmtNumber(filtered.length);
  }
  renderAlerts(filtered);
}

async function startListener() {
  if (mode !== "api") return;
  await jsonFetch("/api/start", "POST");
  await refreshAll();
}

async function stopListener() {
  if (mode !== "api") return;
  await jsonFetch("/api/stop", "POST");
  await refreshAll();
}

async function refreshAll() {
  try {
    const [status, alerts] = await Promise.all([fetchStatus(), fetchAlerts()]);
    cachedStatus = status;
    allAlerts = alerts;
    updateStatusLine(cachedStatus);
    updateMetrics(allAlerts);
    applyFiltersAndRender();
  } catch (err) {
    if (statusEl) statusEl.textContent = `Dashboard refresh failed: ${err.message}`;
  }
}

function bindEvents() {
  if (refreshBtn) refreshBtn.addEventListener("click", refreshAll);
  if (searchInputEl) searchInputEl.addEventListener("input", applyFiltersAndRender);
  if (formFilterEl) formFilterEl.addEventListener("change", applyFiltersAndRender);
  if (startBtn) startBtn.addEventListener("click", startListener);
  if (stopBtn) stopBtn.addEventListener("click", stopListener);
}

function applyModeSpecificUI() {
  if (mode === "api") return;
  if (startBtn) startBtn.disabled = true;
  if (stopBtn) stopBtn.disabled = true;
}

async function init() {
  await detectMode();
  setModeChip();
  applyModeSpecificUI();
  bindEvents();
  await refreshAll();

  const interval = mode === "api" ? 8000 : 60000;
  setInterval(refreshAll, interval);
}

init();
