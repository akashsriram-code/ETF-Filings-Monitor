import re
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from app.config import Settings


def _clean_cik_for_path(cik: str) -> str:
    digits = "".join(ch for ch in cik if ch.isdigit())
    if not digits:
        return cik.strip()
    return str(int(digits))


def _clean_accession_for_path(accession_number: str) -> str:
    return "".join(ch for ch in accession_number if ch.isdigit())


def build_sec_index_url(cik: str, accession_number: str) -> str:
    clean_cik = _clean_cik_for_path(cik)
    clean_accession = _clean_accession_for_path(accession_number)
    return f"https://www.sec.gov/Archives/edgar/data/{clean_cik}/{clean_accession}/index.html"


def _select_primary_document_url(index_url: str, index_html: str, form_type: str | None = None) -> str:
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
        if "/archives/" not in low:
            return None
        if low.endswith("/index.html") or low.endswith("/index.htm"):
            return None
        if not low.endswith((".htm", ".html", ".txt", ".xml")):
            return None
        return absolute

    normalized_form = "".join((form_type or "").upper().split())

    for table in soup.select("table.tableFile"):
        preferred_rows = []
        fallback_rows = []
        for row in table.select("tr"):
            cols = row.select("td")
            if not cols:
                continue
            type_cell = cols[3].get_text(" ", strip=True) if len(cols) >= 4 else ""
            normalized_type = "".join(type_cell.upper().split())
            if normalized_form and normalized_type.startswith(normalized_form):
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


def _extract_text_from_html(html_content: str) -> str:
    soup = BeautifulSoup(html_content, "html.parser")
    cleaned = soup.get_text(separator=" ", strip=True)
    for pattern in [
        r"SEC\.gov\s*\|\s*Home",
        r"Skip to main content",
        r"An official website of the United States government",
        r"Here's how you know",
        r"Official websites use \.gov",
        r"A \.gov website belongs to an official government organization in the United States",
    ]:
        cleaned = re.sub(pattern, " ", cleaned, flags=re.IGNORECASE)
    return " ".join(cleaned.split())


async def _render_pdf_with_playwright(target_url: str, output_path: Path) -> None:
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(target_url, wait_until="networkidle", timeout=90_000)
        await page.pdf(path=str(output_path), format="Letter", print_background=True)
        await browser.close()


async def collect_sec_artifacts(
    cik: str,
    accession_number: str,
    settings: Settings,
    form_type: str | None = None,
) -> dict[str, str | None]:
    index_url = build_sec_index_url(cik, accession_number)
    headers = {"User-Agent": settings.sec_user_agent}
    timeout = settings.request_timeout_seconds

    result: dict[str, str | None] = {
        "index_url": index_url,
        "primary_document_url": None,
        "primary_text": None,
        "pdf_path": None,
        "error": None,
    }

    if not accession_number:
        result["error"] = "Missing accession number in stream header."
        return result

    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        index_response = await client.get(index_url, headers=headers)
        index_response.raise_for_status()
        primary_url = _select_primary_document_url(index_url, index_response.text, form_type=form_type)
        result["primary_document_url"] = primary_url

        primary_response = await client.get(primary_url, headers=headers)
        primary_response.raise_for_status()
        primary_html = primary_response.text
        result["primary_text"] = _extract_text_from_html(primary_html)

    settings.pdf_output_dir.mkdir(parents=True, exist_ok=True)
    safe_cik = re.sub(r"[^0-9A-Za-z]+", "_", cik) or "unknown_cik"
    safe_accession = re.sub(r"[^0-9A-Za-z]+", "_", accession_number) or "unknown_accession"
    pdf_path = settings.pdf_output_dir / f"{safe_cik}_{safe_accession}.pdf"

    await _render_pdf_with_playwright(result["primary_document_url"] or index_url, pdf_path)
    result["pdf_path"] = str(pdf_path.resolve())
    return result
