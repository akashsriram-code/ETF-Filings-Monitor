from __future__ import annotations

import argparse
import json
import os
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse
from xml.etree import ElementTree as ET

import httpx
from bs4 import BeautifulSoup

SYSTEM_INSTRUCTION = (
    "You are assisting a financial reporter. Output concise, factual filing summaries. "
    "Never include SEC website navigation or .gov boilerplate text."
)
TARGET_FORMS = {"485APOS", "485BPOS", "S-1"}
CRYPTO_KEYWORDS = ["Bitcoin", "Ethereum", "Digital Asset", "Spot", "Coinbase Custody"]
NARRATIVE_MARKERS = [
    "summary prospectus",
    "fund summary",
    "investment objective",
    "principal investment strategy",
    "principal investment strategies",
    "investment strategy",
    "principal risks",
    "fees and expenses",
    "management",
]
NARRATIVE_KEYWORDS = [
    "fund",
    "etf",
    "investment",
    "strategy",
    "objective",
    "portfolio",
    "index",
    "benchmark",
    "risk",
    "expense",
    "advisor",
    "custodian",
    "bitcoin",
    "ethereum",
    "digital asset",
]
NOISE_TOKENS = [
    "us-gaap",
    "xbrl",
    "xbrli",
    "xbrldi",
    "contextref",
    "unitref",
    "xmlns",
    "schema",
    "defref",
]
GENERIC_FUND_NAMES = {
    "the fund",
    "the funds",
    "fund",
    "funds",
    "trust",
    "etf",
}
FEED_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&count=100&output=atom"

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
STATE_PATH = DATA_DIR / "state.json"
ALERTS_PATH = DATA_DIR / "alerts.json"
STATUS_PATH = DATA_DIR / "status.json"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_form_type(form_type: str) -> str:
    return "".join(form_type.upper().split())


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def build_index_url(cik: str, accession_number: str) -> str:
    clean_cik = str(int("".join(ch for ch in cik if ch.isdigit())))
    clean_accession = "".join(ch for ch in accession_number if ch.isdigit())
    return f"https://www.sec.gov/Archives/edgar/data/{clean_cik}/{clean_accession}/{accession_number}-index.html"


def quarter_for_date(day: date) -> int:
    return ((day.month - 1) // 3) + 1


def master_index_url_for_date(day: date) -> str:
    return f"https://www.sec.gov/Archives/edgar/daily-index/{day.year}/QTR{quarter_for_date(day)}/master.{day:%Y%m%d}.idx"


def extract_accession_from_filename(filename: str) -> str:
    # Example filename: edgar/data/320193/0000320193-24-000123.txt
    match = re.search(r"/(\d{10}-\d{2}-\d{6})\.(?:txt|nc)$", filename, flags=re.IGNORECASE)
    if match:
        return match.group(1)
    return ""


def parse_master_index_line(line: str) -> dict[str, str] | None:
    parts = line.strip().split("|")
    if len(parts) < 5:
        return None

    cik, company_name, form_type, date_filed, filename = [part.strip() for part in parts[:5]]
    normalized_form = normalize_form_type(form_type)
    accession_number = extract_accession_from_filename(filename)
    filing_link = f"https://www.sec.gov/Archives/{filename}" if filename else ""

    return {
        "form_type": normalized_form,
        "company_name": company_name,
        "cik": cik,
        "accession_number": accession_number,
        "filing_link": filing_link,
        "updated": date_filed,
    }


def fetch_master_index_entries(user_agent: str, day: date) -> list[dict[str, str]]:
    headers = {"User-Agent": user_agent}
    url = master_index_url_for_date(day)
    with httpx.Client(timeout=30, follow_redirects=True, headers=headers) as client:
        response = client.get(url)

    if response.status_code == 404:
        return []
    response.raise_for_status()

    entries: list[dict[str, str]] = []
    for raw_line in response.text.splitlines():
        if "|" not in raw_line:
            continue
        parsed = parse_master_index_line(raw_line)
        if parsed:
            entries.append(parsed)
    return entries


def fetch_backfill_entries(user_agent: str, backfill_days: int) -> list[dict[str, str]]:
    if backfill_days <= 0:
        return []

    today = datetime.now(timezone.utc).date()
    merged: list[dict[str, str]] = []
    for offset in range(backfill_days):
        day = today - timedelta(days=offset)
        day_entries = fetch_master_index_entries(user_agent, day)
        merged.extend(day_entries)
    return merged


def dedupe_entries(entries: list[dict[str, str]]) -> list[dict[str, str]]:
    seen_keys: set[str] = set()
    deduped: list[dict[str, str]] = []

    for entry in entries:
        accession = entry.get("accession_number", "").strip()
        cik = entry.get("cik", "").strip()
        fallback_key = f"{entry.get('form_type', '')}|{entry.get('company_name', '')}|{entry.get('filing_link', '')}"
        key = accession or f"{cik}:{fallback_key}"
        if key in seen_keys:
            continue
        seen_keys.add(key)
        deduped.append(entry)

    return deduped


def normalize_alert_links(alert: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(alert)
    cik = str(normalized.get("cik", "")).strip()
    accession = str(normalized.get("accession_number", "")).strip()
    canonical_index_url = build_index_url(cik, accession) if cik and accession else ""
    index_url = canonical_index_url or normalized.get("sec_index_url") or ""
    primary = normalized.get("primary_document_url")
    sec_filing = normalized.get("sec_filing_url")

    if not is_valid_archive_url(primary):
        primary = ""
    if not is_valid_archive_url(sec_filing):
        sec_filing = to_ix_url(primary) if is_valid_archive_url(primary) else index_url

    normalized["sec_index_url"] = index_url
    normalized["primary_document_url"] = primary or None
    normalized["sec_filing_url"] = sec_filing or index_url
    return normalized


def needs_synopsis_refresh(alert: dict[str, Any]) -> bool:
    synopsis = (alert.get("synopsis") or "").strip()
    if not synopsis:
        return True
    lower = synopsis.lower()
    signals = [
        "fund name: not clearly stated",
        "strategy: sec.gov",
        "skip to search field",
        "official websites use .gov",
    ]
    return any(signal in lower for signal in signals)


def repair_existing_alert_links(
    alerts: list[dict[str, Any]],
    user_agent: str,
    gemini_api_key: str,
    gemini_model: str,
    max_repairs: int = 30,
) -> tuple[list[dict[str, Any]], int, int]:
    repaired_count = 0
    refreshed_synopsis_count = 0
    repaired_alerts: list[dict[str, Any]] = []

    for alert in alerts:
        current = normalize_alert_links(alert)
        needs_repair = not is_valid_archive_url(current.get("primary_document_url")) or not is_valid_archive_url(
            current.get("sec_filing_url")
        )

        if needs_repair and repaired_count < max_repairs:
            cik = str(current.get("cik", "")).strip()
            accession = str(current.get("accession_number", "")).strip()
            form_type = str(current.get("form_type", "")).strip()
            if cik and accession:
                index_url = build_index_url(cik, accession)
                try:
                    primary_url, filing_text = fetch_primary_document(index_url, user_agent, form_type=form_type)
                    if is_valid_archive_url(primary_url):
                        current["sec_index_url"] = index_url
                        current["primary_document_url"] = primary_url
                        current["sec_filing_url"] = to_ix_url(primary_url)
                        repaired_count += 1

                    if needs_synopsis_refresh(current):
                        is_crypto = bool(current.get("is_crypto")) or normalize_form_type(form_type) == "S-1"
                        current["synopsis"] = generate_synopsis(filing_text, gemini_api_key, gemini_model, is_crypto)
                        refreshed_synopsis_count += 1
                except Exception:
                    pass

        repaired_alerts.append(current)

    return repaired_alerts, repaired_count, refreshed_synopsis_count


def fetch_feed_entries(user_agent: str) -> list[dict[str, str]]:
    headers = {"User-Agent": user_agent}
    with httpx.Client(timeout=30, follow_redirects=True, headers=headers) as client:
        response = client.get(FEED_URL)
        response.raise_for_status()

    root = ET.fromstring(response.text)
    ns = {"atom": "http://www.w3.org/2005/Atom"}
    entries: list[dict[str, str]] = []

    for entry in root.findall("atom:entry", ns):
        link_el = entry.find("atom:link", ns)
        title_el = entry.find("atom:title", ns)
        category_el = entry.find("atom:category", ns)
        updated_el = entry.find("atom:updated", ns)

        link = link_el.attrib.get("href", "").strip() if link_el is not None else ""
        title = (title_el.text or "").strip() if title_el is not None else ""
        category_term = category_el.attrib.get("term", "").strip() if category_el is not None else ""
        updated = (updated_el.text or "").strip() if updated_el is not None else ""

        form_type = normalize_form_type(category_term or title.split("-", maxsplit=1)[0].strip())
        company_name, cik = extract_company_and_cik_from_title(title)
        accession_number = extract_accession_from_link(link)

        entries.append(
            {
                "form_type": form_type,
                "company_name": company_name,
                "cik": cik,
                "accession_number": accession_number,
                "filing_link": link,
                "updated": updated,
            }
        )

    return entries


def extract_company_and_cik_from_title(title: str) -> tuple[str, str]:
    # Example: "485BPOS - Example ETF Trust (0001234567) (Filer)"
    match = re.search(r"^\s*[^-]+-\s*(.+?)\s*\((\d{7,10})\)", title)
    if not match:
        return title.strip(), ""
    return match.group(1).strip(), match.group(2).strip()


def extract_accession_from_link(link: str) -> str:
    dashed = re.search(r"/(\d{10}-\d{2}-\d{6})-index\.htm", link, flags=re.IGNORECASE)
    if dashed:
        return dashed.group(1)

    raw = re.search(r"/data/\d+/(\d{18,})/", link, flags=re.IGNORECASE)
    if raw:
        digits = raw.group(1)
        if len(digits) >= 18:
            return f"{digits[:10]}-{digits[10:12]}-{digits[12:18]}"

    return ""


def select_primary_document_url(index_url: str, index_html: str, form_type: str | None = None) -> str:
    soup = BeautifulSoup(index_html, "html.parser")

    def normalize_sec_doc_url(href: str) -> str | None:
        candidate = href.strip()
        if not candidate:
            return None

        parsed = urlparse(candidate)
        if parsed.path.startswith("/ixviewer/ix.html"):
            query = parse_qs(parsed.query)
            doc = (query.get("doc") or [""])[0]
            if doc.startswith("/Archives/"):
                return f"https://www.sec.gov{doc}"
        if parsed.path == "/ix":
            query = parse_qs(parsed.query)
            doc = (query.get("doc") or [""])[0]
            if doc.startswith("/Archives/"):
                return f"https://www.sec.gov{doc}"

        if candidate.startswith("/ixviewer/ix.html?doc=/Archives/"):
            doc = candidate.split("doc=", 1)[1]
            return f"https://www.sec.gov{doc}"

        absolute = candidate if candidate.startswith(("http://", "https://")) else urljoin(index_url, candidate)
        low = absolute.lower()
        if low in {"https://www.sec.gov", "https://www.sec.gov/", "http://www.sec.gov", "http://www.sec.gov/"}:
            return None
        if "/archives/" not in low:
            return None
        if low.endswith("/index.html") or low.endswith("/index.htm"):
            return None
        if not low.endswith((".htm", ".html", ".txt", ".xml")):
            return None
        return absolute

    normalized_form = normalize_form_type(form_type or "")
    form_token = re.sub(r"[^a-z0-9]+", "", normalized_form.lower())

    # Prefer document table rows first; they map to the filing's actual documents.
    for table in soup.select("table.tableFile"):
        preferred_rows = []
        fallback_rows = []
        for row in table.select("tr"):
            cols = row.select("td")
            if not cols:
                continue
            type_cell = cols[3].get_text(" ", strip=True) if len(cols) >= 4 else ""
            if normalized_form and normalize_form_type(type_cell).startswith(normalized_form):
                preferred_rows.append(row)
            else:
                fallback_rows.append(row)

        for row in preferred_rows + fallback_rows:
            link = row.select_one("a[href]")
            if link:
                resolved = normalize_sec_doc_url(link.get("href", ""))
                if resolved:
                    return resolved

    # SEC archive directory listing fallback (when index URL resolves to folder listing).
    best_candidate: str | None = None
    best_score = -1.0
    for row in soup.select("tr"):
        link = row.select_one("a[href]")
        if not link:
            continue
        resolved = normalize_sec_doc_url(link.get("href", ""))
        if not resolved:
            continue
        filename = resolved.rsplit("/", 1)[-1].lower()
        if "index" in filename or "header" in filename or "filingsummary" in filename:
            continue

        score = 0.0
        if filename.endswith((".htm", ".html")):
            score += 30.0
        elif filename.endswith(".txt"):
            score += 10.0

        compact_name = re.sub(r"[^a-z0-9]+", "", filename)
        if form_token and form_token in compact_name:
            score += 120.0
        if "ex" in compact_name:
            score -= 5.0

        cells = row.select("td")
        if len(cells) >= 2:
            size_text = cells[1].get_text(" ", strip=True).replace(",", "")
            if size_text.isdigit():
                score += min(int(size_text) / 100000.0, 50.0)

        if score > best_score:
            best_score = score
            best_candidate = resolved

    if best_candidate:
        return best_candidate

    for link in soup.select("a[href]"):
        resolved = normalize_sec_doc_url(link.get("href", ""))
        if resolved:
            return resolved
    return index_url


def fetch_primary_document(index_url: str, user_agent: str, form_type: str | None = None) -> tuple[str, str]:
    headers = {"User-Agent": user_agent}
    with httpx.Client(timeout=30, follow_redirects=True, headers=headers) as client:
        index_response = client.get(index_url)
        if index_response.status_code == 404 and index_url.endswith("-index.html"):
            fallback_index_url = index_url.rsplit("/", 1)[0] + "/index.html"
            index_response = client.get(fallback_index_url)
            index_response.raise_for_status()
            index_url = fallback_index_url
        else:
            index_response.raise_for_status()
        primary_url = select_primary_document_url(index_url, index_response.text, form_type=form_type)

        primary_response = client.get(primary_url)
        primary_response.raise_for_status()
        soup = BeautifulSoup(primary_response.text, "html.parser")
        text = soup.get_text(separator=" ", strip=True)
        text = clean_extracted_text(text)
        return primary_url, text


def is_valid_archive_url(url: str | None) -> bool:
    candidate = (url or "").strip()
    if not candidate:
        return False
    parsed = urlparse(candidate)
    host = parsed.netloc.lower()
    path = parsed.path.lower()

    if not host.endswith("sec.gov"):
        return False
    if "/archives/" not in path:
        return False

    last_segment = path.rstrip("/").split("/")[-1]
    if "." not in last_segment:
        return False
    if last_segment in {"index.html", "index.htm"}:
        return False
    return True


def to_ix_url(url: str | None) -> str:
    candidate = (url or "").strip()
    if not candidate:
        return ""
    parsed = urlparse(candidate)
    if parsed.path == "/ix":
        return candidate
    if is_valid_archive_url(candidate):
        return f"https://www.sec.gov/ix?doc={parsed.path}"
    return candidate


def clean_extracted_text(text: str) -> str:
    cleaned = " ".join(text.split())
    boilerplate_patterns = [
        r"SEC\.gov\s*\|\s*Home",
        r"Skip to main content",
        r"An official website of the United States government",
        r"Here's how you know",
        r"Official websites use \.gov",
        r"A \.gov website belongs to an official government organization in the United States",
    ]
    for pattern in boilerplate_patterns:
        cleaned = re.sub(pattern, " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(?:us-gaap|xbrli|xbrldi|dei|link|xlink)\b[:\w\-]*", " ", cleaned, flags=re.IGNORECASE)
    return " ".join(cleaned.split())


def extract_narrative_text(text: str, max_chars: int = 25_000) -> str:
    cleaned = clean_extracted_text(text)
    if not cleaned:
        return ""

    lower = cleaned.lower()
    windows: list[str] = []
    seen: set[tuple[int, int]] = set()
    for marker in NARRATIVE_MARKERS:
        for match in re.finditer(re.escape(marker), lower):
            start = max(0, match.start() - 1_500)
            end = min(len(cleaned), match.end() + 5_000)
            key = (start, end)
            if key in seen:
                continue
            seen.add(key)
            windows.append(cleaned[start:end])
            if len(windows) >= 8:
                break
        if len(windows) >= 8:
            break

    source = " ".join(windows).strip() if windows else cleaned[: max_chars * 2]
    sentences = re.split(r"(?<=[.!?])\s+", source)
    scored: list[tuple[float, int, str]] = []
    for idx, sentence in enumerate(sentences):
        s = sentence.strip()
        if len(s) < 50 or len(s) > 600:
            continue
        s_lower = s.lower()
        if any(token in s_lower for token in NOISE_TOKENS):
            continue

        digits = sum(ch.isdigit() for ch in s)
        digit_ratio = digits / max(len(s), 1)
        if digit_ratio > 0.25:
            continue

        score = 0.0
        for kw in NARRATIVE_KEYWORDS:
            if kw in s_lower:
                score += 2.0
        score += max(0.0, min(len(s), 260) / 260.0)
        scored.append((score, idx, s))

    if not scored:
        return source[:max_chars]

    top = sorted(scored, key=lambda item: item[0], reverse=True)[:40]
    ordered = [item[2] for item in sorted(top, key=lambda item: item[1])]
    narrative = " ".join(ordered).strip()
    if len(narrative) < 1_200:
        narrative = source[:max_chars]
    return narrative[:max_chars]


def crypto_gate(form_type: str, filing_text: str) -> tuple[bool, list[str], bool]:
    if form_type in {"485APOS", "485BPOS"}:
        return True, [], False
    if form_type != "S-1":
        return False, [], False

    searchable_text = filing_text[:10_000].lower()
    matched_keywords = [keyword for keyword in CRYPTO_KEYWORDS if keyword.lower() in searchable_text]
    return bool(matched_keywords), matched_keywords, True


def extract_first(patterns: list[str], text: str, default: str = "Unknown") -> str:
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            value = match.group(1).strip(" .;:,")
            if value:
                return value
    return default


def sanitize_name(value: str, default: str = "Unknown") -> str:
    cleaned = " ".join(value.split()).strip(" .;:,")
    cleaned = re.sub(r"^(?:The\s+)?Prospectus\s+for\s+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^(?:The\s+)?Statement of Additional Information\s+for\s+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^(?:Class\s+[A-Z0-9]+\s+)+", "", cleaned, flags=re.IGNORECASE)
    if not cleaned:
        return default
    if len(cleaned) > 120:
        return default
    if cleaned[0].islower():
        return default
    bad_fragments = ["reports and certain other information", "skip to", "official website"]
    if any(fragment in cleaned.lower() for fragment in bad_fragments):
        return default
    if cleaned.lower() in GENERIC_FUND_NAMES:
        return default
    return cleaned


def collapse_to_single_fund_name(value: str) -> str:
    parts = re.split(r",| and ", value)
    for part in parts:
        candidate = part.strip(" .;:,")
        if re.search(r"\b(Fund|Trust|ETF)\b", candidate):
            sanitized = sanitize_name(candidate)
            if sanitized != "Unknown":
                return sanitized
    return sanitize_name(value)


def extract_fund_name(text: str) -> str:
    direct = extract_first(
        [
            r"(?:Fund Name|Series Name|Name of Fund)\s*[:\-]\s*([^\n\r:]{3,120}?)(?=\s(?:Ticker|Ticker Symbol|Expense Ratio|Strategy|$))",
            r"\b([A-Z][A-Za-z0-9&,\-\.]*(?:\s+[A-Z][A-Za-z0-9&,\-\.]*){0,8}\s(?:Fund|Trust|ETF)(?:,\s*Inc\.)?)\b",
        ],
        text,
        default="Unknown",
    )
    direct_clean = sanitize_name(direct)
    if direct_clean != "Unknown":
        return collapse_to_single_fund_name(direct_clean)

    prospectus_block = extract_first(
        [
            r"Prospectus dated [^.]{0,120} for ([^.]{20,500})\.",
            r"read in conjunction with[^.]{0,80}for ([^.]{20,500})\.",
        ],
        text,
        default="",
    )
    if prospectus_block:
        for match in re.finditer(r"([A-Z][A-Za-z0-9&,\-\. ]{2,80}\s(?:Fund|Trust|ETF))", prospectus_block):
            candidate = sanitize_name(match.group(1))
            if candidate != "Unknown":
                return collapse_to_single_fund_name(candidate)

    return "Unknown"


def fund_context(text: str, fund_name: str, window: int = 12_000) -> str:
    if not text:
        return ""
    if fund_name and fund_name != "Unknown":
        idx = text.lower().find(fund_name.lower())
        if idx >= 0:
            start = max(0, idx - 800)
            end = min(len(text), idx + window)
            return text[start:end]
    return text[:window]


def extract_ticker(text: str, fund_name: str) -> str:
    context = fund_context(text, fund_name)
    if fund_name and fund_name != "Unknown":
        row_match = re.search(
            rf"{re.escape(fund_name)}(?:\s*\([^)]{{1,120}}\))?\s+([A-Z\?]{{2,6}})\s+([A-Z\?]{{1,6}})\s+([A-Z\?]{{1,6}})\s+([A-Z\?]{{2,6}})",
            context,
            flags=re.IGNORECASE,
        )
        if row_match:
            class_i = row_match.group(4).upper()
            if class_i != "?":
                return class_i
            for idx in [1, 2, 3]:
                v = row_match.group(idx).upper()
                if v != "?":
                    return v

    for pattern in [
        r"Ticker Symbols?\s*[:\-]\s*([^\n]{5,500})",
        r"(?:Ticker|Ticker Symbol)\s*[:\-]\s*([A-Z]{1,6})",
    ]:
        match = re.search(pattern, context, flags=re.IGNORECASE)
        if not match:
            continue
        value = match.group(1).strip()
        class_i = re.search(r"Class\s+I[—–\-:\s]*([A-Z]{2,6})", value, flags=re.IGNORECASE)
        if class_i:
            return class_i.group(1).upper()
        symbols = re.findall(r"\b[A-Z]{2,6}\b", value)
        if symbols:
            return symbols[0].upper()
    return "Unknown"


def extract_expense_ratio(text: str, fund_name: str) -> str:
    context = fund_context(text, fund_name)
    match = re.search(
        r"Total Annual Fund Operating Expenses After Fee Waivers[^%]{0,120}?([0-9]+(?:\.[0-9]+)?\s*%)",
        context,
        flags=re.IGNORECASE,
    )
    if match:
        return match.group(1).replace(" ", "")
    match = re.search(
        r"Total Annual Fund Operating Expenses[^%]{0,120}?([0-9]+(?:\.[0-9]+)?\s*%)",
        context,
        flags=re.IGNORECASE,
    )
    if match:
        return match.group(1).replace(" ", "")
    match = re.search(r"Expense Ratio\s*[:\-]?\s*([0-9]+(?:\.[0-9]+)?\s*%)", context, flags=re.IGNORECASE)
    if match:
        return match.group(1).replace(" ", "")
    return "Unknown"


def extract_strategy_hint(text: str, fund_name: str) -> str:
    context = fund_context(text, fund_name, window=20_000)
    objective = extract_first(
        [
            r"Investment Objective\s+(.{30,600}?)(?=\s+Fees and Expenses|\s+Principal Investment Strateg(?:y|ies)|\s+Principal Risks)",
        ],
        context,
        default="",
    )
    principal = extract_first(
        [
            r"Principal Investment Strateg(?:y|ies)\s+(.{30,900}?)(?=\s+Principal Risks|\s+Portfolio Managers|\s+Management|\s+Purchase and Sale)",
        ],
        context,
        default="",
    )
    bits = []
    if objective:
        bits.append(normalize_strategy_text(objective))
    if principal:
        bits.append(normalize_strategy_text(principal))
    merged = " ".join(bits).strip()
    return normalize_strategy_text(merged) if merged else "Not available."


def normalize_strategy_text(value: str) -> str:
    text = " ".join(value.split())
    if not text:
        return "Not available."
    chunks = re.split(r"(?<=[.!?])\s+", text)
    if len(chunks) < 2:
        chunks = re.split(r"[;:]\s+", text)
    chosen = " ".join(chunks[:2]).strip()
    if len(chosen) > 420:
        chosen = chosen[:420].rstrip() + "..."
    return chosen or "Not available."


def normalize_summary(summary: str, is_crypto: bool, hints: dict[str, str] | None = None) -> str:
    lines = [line.strip() for line in summary.splitlines() if line.strip()]
    parsed: dict[str, str] = {}
    for line in lines:
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        parsed[key.strip().lower()] = value.strip()

    hints = hints or {}
    fund_name = sanitize_name(parsed.get("fund name", "Unknown"))
    if fund_name == "Unknown" and hints.get("fund_name"):
        fund_name = sanitize_name(hints.get("fund_name", "Unknown"))

    ticker = parsed.get("ticker", "Unknown").strip() or "Unknown"
    if ticker == "Unknown" and hints.get("ticker"):
        ticker = hints["ticker"]

    expense_ratio = parsed.get("expense ratio", "Unknown").strip() or "Unknown"
    if expense_ratio == "Unknown" and hints.get("expense_ratio"):
        expense_ratio = hints["expense_ratio"]
    strategy = normalize_strategy_text(parsed.get("strategy", ""))
    if (
        strategy == "Not available."
        or "statement of additional information" in strategy.lower()
        or "should be read in conjunction with" in strategy.lower()
    ) and hints.get("strategy"):
        strategy = normalize_strategy_text(hints["strategy"])

    output = [
        f"Fund Name: {fund_name}",
        f"Ticker: {ticker}",
        f"Expense Ratio: {expense_ratio}",
        f"Strategy: {strategy}",
    ]
    if is_crypto:
        custodian = sanitize_name(parsed.get("custodian", "Unknown"))
        output.append(f"Custodian: {custodian}")
    return "\n".join(output)


def extract_structured_fields(text: str, is_crypto: bool) -> dict[str, str]:
    fund_name = extract_fund_name(text)
    ticker = extract_ticker(text, fund_name)
    expense_ratio = extract_expense_ratio(text, fund_name)
    strategy = extract_strategy_hint(text, fund_name)
    custodian = extract_first(
        [
            r"(?:Custodian|Crypto Custodian)\s*[:\-]\s*([A-Za-z0-9&,\-\. ]{3,120})",
            r"(Coinbase Custody)",
        ],
        text,
        default="Unknown" if is_crypto else "N/A",
    )
    return {
        "fund_name": fund_name,
        "ticker": ticker,
        "expense_ratio": expense_ratio,
        "custodian": custodian,
        "strategy": strategy,
    }


def build_chunks(text: str, chunk_size: int = 7000, overlap: int = 600, max_chunks: int = 3) -> list[str]:
    chunks: list[str] = []
    cursor = 0
    n = len(text)
    while cursor < n and len(chunks) < max_chunks:
        end = min(cursor + chunk_size, n)
        chunk = text[cursor:end].strip()
        if chunk:
            chunks.append(chunk)
        if end >= n:
            break
        cursor = max(end - overlap, 0)
    return chunks


def is_low_quality_summary(summary: str) -> bool:
    if not summary.strip() or len(summary.strip()) < 80:
        return True
    lower = summary.lower()
    bad_signals = [
        "skip to search field",
        "official websites use .gov",
        "sec.gov | home",
        "an official website of the united states government",
    ]
    if any(signal in lower for signal in bad_signals):
        return True
    if lower.count("not found") >= 2:
        return True
    return False


def _call_gemini(model_name: str, api_key: str, prompt: str) -> str:
    import google.generativeai as genai

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(model_name=model_name, system_instruction=SYSTEM_INSTRUCTION)
    response = model.generate_content(prompt)
    return (getattr(response, "text", "") or "").strip()


def generate_synopsis(filing_text: str, gemini_api_key: str, gemini_model: str, is_crypto: bool) -> str:
    source_text = clean_extracted_text(filing_text.strip())
    text = extract_narrative_text(filing_text.strip())
    if not source_text:
        return "No filing text available."
    if not gemini_api_key:
        return fallback_synopsis(source_text, is_crypto)

    try:
        fields = extract_structured_fields(source_text, is_crypto)
        chunks = build_chunks(text, chunk_size=5000, overlap=500, max_chunks=2)
        chunk_summaries: list[str] = []
        for idx, chunk in enumerate(chunks, start=1):
            chunk_prompt = (
                "Summarize the investment strategy and key filing facts in 2 short sentences. "
                "Exclude SEC site boilerplate.\n\n"
                f"Chunk {idx}:\n{chunk}"
            )
            chunk_summary = _call_gemini(gemini_model, gemini_api_key, chunk_prompt)
            if chunk_summary:
                chunk_summaries.append(chunk_summary)

        final_prompt = (
            "Return exactly these lines:\n"
            "Fund Name: <value>\n"
            "Ticker: <value or Unknown>\n"
            "Expense Ratio: <value or Unknown>\n"
            "Strategy: <exactly 2 sentences>\n"
            + ("Custodian: <value or Unknown>\n" if is_crypto else "")
            + "Do not include SEC.gov navigation text.\n\n"
            f"Extracted hints:\n"
            f"- Fund Name hint: {fields['fund_name']}\n"
            f"- Ticker hint: {fields['ticker']}\n"
            f"- Expense Ratio hint: {fields['expense_ratio']}\n"
            f"- Strategy hint: {fields['strategy']}\n"
            + (f"- Custodian hint: {fields['custodian']}\n" if is_crypto else "")
            + "\nChunk summaries:\n"
            + "\n".join(f"- {item}" for item in chunk_summaries)
        )
        summary = _call_gemini(gemini_model, gemini_api_key, final_prompt)
        if is_low_quality_summary(summary):
            summary = _call_gemini(gemini_model, gemini_api_key, final_prompt + "\n\nRetry with cleaner output.")
        if not summary:
            return fallback_synopsis(source_text, is_crypto)
        return normalize_summary(summary, is_crypto, hints=fields)
    except Exception:
        return fallback_synopsis(source_text, is_crypto)


def fallback_synopsis(text: str, is_crypto: bool) -> str:
    cleaned = extract_narrative_text(text)
    fields = extract_structured_fields(cleaned, is_crypto)
    sentences = re.split(r"(?<=[.!?])\s+", cleaned)
    preview = fields.get("strategy", "").strip() or " ".join(sentences[:2]).strip() or cleaned[:450]
    lines = [
        f"Fund Name: {fields['fund_name']}",
        f"Ticker: {fields['ticker']}",
        f"Expense Ratio: {fields['expense_ratio']}",
        f"Strategy: {preview}",
    ]
    if is_crypto:
        lines.append(f"Custodian: {fields['custodian']}")
    return normalize_summary("\n".join(lines), is_crypto, hints=fields)


def send_resend_email(
    resend_api_key: str,
    from_email: str,
    to_email: str,
    subject: str,
    body: str,
    dry_run: bool,
) -> tuple[bool, str | None]:
    if dry_run:
        return True, None
    if not resend_api_key:
        return False, "RESEND_API_KEY is missing."

    payload = {"from": from_email, "to": [to_email], "subject": subject, "text": body}
    headers = {"Authorization": f"Bearer {resend_api_key}", "Content-Type": "application/json"}
    with httpx.Client(timeout=30, follow_redirects=True) as client:
        response = client.post("https://api.resend.com/emails", headers=headers, json=payload)
    if response.status_code >= 400:
        return False, f"Resend error {response.status_code}: {response.text}"
    return True, None


def run_once(dry_run: bool = False, backfill_days: int = 0) -> int:
    user_agent = os.getenv("SEC_USER_AGENT", "").strip()
    reporter_email = os.getenv("REPORTER_EMAIL", "").strip()
    resend_from_email = os.getenv("RESEND_FROM_EMAIL", "").strip()
    resend_api_key = os.getenv("RESEND_API_KEY", "").strip()
    gemini_api_key = os.getenv("GEMINI_API_KEY", "").strip()
    gemini_model = os.getenv("GEMINI_MODEL", "gemini-1.5-pro").strip()

    required = {
        "SEC_USER_AGENT": user_agent,
        "REPORTER_EMAIL": reporter_email,
        "RESEND_FROM_EMAIL": resend_from_email,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

    state = load_json(STATE_PATH, {"seen_accessions": [], "last_run": None, "last_error": None})
    existing_alerts = [normalize_alert_links(item) for item in load_json(ALERTS_PATH, [])]
    seen = set(state.get("seen_accessions", []))

    fetched_entries = 0
    feed_entries_count = 0
    backfill_entries_count = 0
    repaired_links_count = 0
    refreshed_synopsis_count = 0
    new_alerts: list[dict[str, Any]] = []
    last_error: str | None = None

    try:
        existing_alerts, repaired_links_count, refreshed_synopsis_count = repair_existing_alert_links(
            existing_alerts,
            user_agent=user_agent,
            gemini_api_key=gemini_api_key,
            gemini_model=gemini_model,
        )

        feed_entries = fetch_feed_entries(user_agent)
        feed_entries_count = len(feed_entries)

        backfill_entries = fetch_backfill_entries(user_agent, backfill_days)
        backfill_entries_count = len(backfill_entries)

        entries = dedupe_entries(feed_entries + backfill_entries)
        fetched_entries = len(entries)
        for entry in entries:
            form_type = normalize_form_type(entry.get("form_type", ""))
            if form_type not in TARGET_FORMS:
                continue

            accession_number = entry.get("accession_number", "")
            cik = entry.get("cik", "")
            company_name = entry.get("company_name", "")
            filing_link = entry.get("filing_link", "")
            if not accession_number or not cik:
                continue
            if accession_number in seen:
                continue

            index_url = build_index_url(cik, accession_number)

            try:
                primary_doc_url, filing_text = fetch_primary_document(index_url, user_agent, form_type=form_type)
                if not is_valid_archive_url(primary_doc_url):
                    primary_doc_url = filing_link if is_valid_archive_url(filing_link) else index_url
            except Exception as exc:
                last_error = f"Failed to fetch filing content for {accession_number}: {exc}"
                continue

            should_alert, matched_keywords, is_crypto = crypto_gate(form_type, filing_text)
            if not should_alert:
                continue

            synopsis = generate_synopsis(filing_text, gemini_api_key, gemini_model, is_crypto)
            subject = f"[ETF ALERT] {form_type} Filed by {company_name}"
            body = f"{synopsis}\n\nSEC Link: {index_url}"
            email_sent, email_error = send_resend_email(
                resend_api_key=resend_api_key,
                from_email=resend_from_email,
                to_email=reporter_email,
                subject=subject,
                body=body,
                dry_run=dry_run,
            )

            alert = {
                "created_at": now_iso(),
                "form_type": form_type,
                "cik": cik,
                "company_name": company_name,
                "accession_number": accession_number,
                "sec_index_url": index_url,
                "sec_filing_url": to_ix_url(primary_doc_url) if is_valid_archive_url(primary_doc_url) else index_url,
                "primary_document_url": primary_doc_url,
                "matched_keywords": matched_keywords,
                "is_crypto": is_crypto,
                "synopsis": synopsis,
                "email_sent": email_sent,
                "error": email_error,
            }
            new_alerts.append(alert)

            # Keep retry behavior for email failures by not marking as seen.
            if email_sent or dry_run:
                seen.add(accession_number)
            elif email_error:
                last_error = email_error

    except Exception as exc:
        last_error = str(exc)

    merged_alerts = (new_alerts + existing_alerts)[:200]
    state_payload = {
        "seen_accessions": list(seen)[-5000:],
        "last_run": now_iso(),
        "last_error": last_error,
    }
    status_payload = {
        "last_run": state_payload["last_run"],
        "last_error": last_error,
        "fetched_entries": fetched_entries,
        "feed_entries": feed_entries_count,
        "backfill_entries": backfill_entries_count,
        "backfill_days": backfill_days,
        "repaired_links": repaired_links_count,
        "refreshed_synopsis": refreshed_synopsis_count,
        "new_alerts": len(new_alerts),
        "total_alerts": len(merged_alerts),
        "mode": "github-pages-scheduled-poller",
    }

    save_json(ALERTS_PATH, merged_alerts)
    save_json(STATE_PATH, state_payload)
    save_json(STATUS_PATH, status_payload)
    print(
        f"Fetched entries: {fetched_entries} (feed={feed_entries_count}, "
        f"backfill={backfill_entries_count}, backfill_days={backfill_days}); "
        f"new alerts: {len(new_alerts)}; repaired_links: {repaired_links_count}; "
        f"refreshed_synopsis: {refreshed_synopsis_count}"
    )
    if last_error:
        print(f"Last error: {last_error}")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Poll SEC current filings and alert on ETF forms.")
    parser.add_argument("--dry-run", action="store_true", help="Run without sending emails.")
    parser.add_argument(
        "--backfill-days",
        type=int,
        default=int(os.getenv("BACKFILL_DAYS", "0") or "0"),
        help="Include SEC daily master indexes for the last N days.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return run_once(dry_run=args.dry_run, backfill_days=max(args.backfill_days, 0))


if __name__ == "__main__":
    raise SystemExit(main())
