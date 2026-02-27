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
    "Summarize this ETF filing for a financial reporter. Include: Fund Name, "
    "Ticker (if present), Expense Ratio, and a 2-sentence breakdown of the "
    "investment strategy. If it's a crypto ETF, specifically highlight the custodian."
)
TARGET_FORMS = {"485APOS", "485BPOS", "S-1"}
CRYPTO_KEYWORDS = ["Bitcoin", "Ethereum", "Digital Asset", "Spot", "Coinbase Custody"]
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
    return f"https://www.sec.gov/Archives/edgar/data/{clean_cik}/{clean_accession}/index.html"


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

        if candidate.startswith("/ixviewer/ix.html?doc=/Archives/"):
            doc = candidate.split("doc=", 1)[1]
            return f"https://www.sec.gov{doc}"

        absolute = candidate if candidate.startswith(("http://", "https://")) else urljoin(index_url, candidate)
        low = absolute.lower()
        if low in {"https://www.sec.gov", "https://www.sec.gov/", "http://www.sec.gov", "http://www.sec.gov/"}:
            return None
        if "/index.html" in low and low.rstrip("/").endswith("/index.html"):
            return None
        if not low.endswith((".htm", ".html", ".txt", ".xml")):
            return None
        return absolute

    normalized_form = normalize_form_type(form_type or "")

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

    for link in soup.select("a[href]"):
        resolved = normalize_sec_doc_url(link.get("href", ""))
        if resolved:
            return resolved
    return index_url


def fetch_primary_document(index_url: str, user_agent: str, form_type: str | None = None) -> tuple[str, str]:
    headers = {"User-Agent": user_agent}
    with httpx.Client(timeout=30, follow_redirects=True, headers=headers) as client:
        index_response = client.get(index_url)
        index_response.raise_for_status()
        primary_url = select_primary_document_url(index_url, index_response.text, form_type=form_type)

        primary_response = client.get(primary_url)
        primary_response.raise_for_status()
        soup = BeautifulSoup(primary_response.text, "html.parser")
        text = soup.get_text(separator=" ", strip=True)
        text = clean_extracted_text(text)
        return primary_url, text


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
    return " ".join(cleaned.split())


def crypto_gate(form_type: str, filing_text: str) -> tuple[bool, list[str], bool]:
    if form_type in {"485APOS", "485BPOS"}:
        return True, [], False
    if form_type != "S-1":
        return False, [], False

    searchable_text = filing_text[:10_000].lower()
    matched_keywords = [keyword for keyword in CRYPTO_KEYWORDS if keyword.lower() in searchable_text]
    return bool(matched_keywords), matched_keywords, True


def generate_synopsis(filing_text: str, gemini_api_key: str, gemini_model: str, is_crypto: bool) -> str:
    text = filing_text.strip()
    if not text:
        return "No filing text available."
    if not gemini_api_key:
        return fallback_synopsis(text, is_crypto)

    try:
        import google.generativeai as genai

        genai.configure(api_key=gemini_api_key)
        model = genai.GenerativeModel(model_name=gemini_model, system_instruction=SYSTEM_INSTRUCTION)
        prompt = (
            f"Crypto ETF context: {'yes' if is_crypto else 'no'}\n\n"
            "Filing text:\n"
            f"{text[:25_000]}"
        )
        response = model.generate_content(prompt)
        summary = (getattr(response, "text", "") or "").strip()
        return summary or fallback_synopsis(text, is_crypto)
    except Exception:
        return fallback_synopsis(text, is_crypto)


def fallback_synopsis(text: str, is_crypto: bool) -> str:
    preview = " ".join(text.split())[:450]
    lines = [
        "Fund Name: Not clearly stated",
        "Ticker: Not found",
        "Expense Ratio: Not found",
        f"Strategy: {preview}",
    ]
    if is_crypto:
        lines.append("Custodian: Review filing text for named custodian.")
    return "\n".join(lines)


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
    existing_alerts = load_json(ALERTS_PATH, [])
    seen = set(state.get("seen_accessions", []))

    fetched_entries = 0
    feed_entries_count = 0
    backfill_entries_count = 0
    new_alerts: list[dict[str, Any]] = []
    last_error: str | None = None

    try:
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
            if not accession_number or not cik:
                continue
            if accession_number in seen:
                continue

            index_url = build_index_url(cik, accession_number)

            try:
                primary_doc_url, filing_text = fetch_primary_document(index_url, user_agent, form_type=form_type)
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
                "sec_filing_url": primary_doc_url,
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
        f"new alerts: {len(new_alerts)}"
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
