import re
from pathlib import Path
from urllib.parse import urljoin

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


def _select_primary_document_url(index_url: str, index_html: str) -> str:
    soup = BeautifulSoup(index_html, "html.parser")

    for link in soup.select("a[href]"):
        href = link.get("href", "").strip()
        if not href:
            continue
        lower_href = href.lower()
        if lower_href.endswith((".htm", ".html")) and not lower_href.endswith("/index.html"):
            if lower_href.startswith("http://") or lower_href.startswith("https://"):
                return href
            if lower_href.startswith("/"):
                return f"https://www.sec.gov{href}"
            return urljoin(index_url, href)

    return index_url


def _extract_text_from_html(html_content: str) -> str:
    soup = BeautifulSoup(html_content, "html.parser")
    return soup.get_text(separator=" ", strip=True)


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
        primary_url = _select_primary_document_url(index_url, index_response.text)
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
