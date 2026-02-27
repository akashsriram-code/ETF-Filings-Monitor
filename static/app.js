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
  const regular = alerts.filter((alert) => ["485APOS", "485BPOS"].includes((alert.form_type || "").toUpperCase())).length;
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
      const synopsis = escapeHtml(synopsisPreview(alert.synopsis));
      const links = resolveLinks(alert);
      const secLink = escapeHtml(links.secLink);
      const primaryLink = links.primaryLink ? escapeHtml(links.primaryLink) : "";
      const hasError = Boolean(alert.error);

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
            <span class="tag ${alert.email_sent ? "sent" : "error"}">${alert.email_sent ? "EMAIL SENT" : "EMAIL ISSUE"}</span>
          </div>
        </div>
        <p class="filing-meta">Matched Keywords: ${keywords}</p>
        <p class="synopsis">${synopsis}</p>
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
