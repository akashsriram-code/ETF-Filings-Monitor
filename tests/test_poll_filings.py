from datetime import date

from scripts.poll_filings import (
    clean_extracted_text,
    crypto_gate,
    extract_accession_from_filename,
    extract_accession_from_link,
    extract_company_and_cik_from_title,
    master_index_url_for_date,
    parse_master_index_line,
    select_primary_document_url,
)


def test_extract_company_and_cik_from_title() -> None:
    company, cik = extract_company_and_cik_from_title("485BPOS - Example ETF Trust (0001234567) (Filer)")
    assert company == "Example ETF Trust"
    assert cik == "0001234567"


def test_extract_accession_from_link() -> None:
    link = "https://www.sec.gov/Archives/edgar/data/1234567/000123456726000111/0001234567-26-000111-index.htm"
    accession = extract_accession_from_link(link)
    assert accession == "0001234567-26-000111"


def test_crypto_gate_for_s1_requires_keyword() -> None:
    should_alert, keywords, is_crypto = crypto_gate("S-1", "Spot Bitcoin exposure with Coinbase Custody")
    assert should_alert is True
    assert is_crypto is True
    assert "Bitcoin" in keywords


def test_extract_accession_from_filename() -> None:
    filename = "edgar/data/1234567/0001234567-26-000222.txt"
    accession = extract_accession_from_filename(filename)
    assert accession == "0001234567-26-000222"


def test_parse_master_index_line() -> None:
    line = "1234567|Example ETF Trust|485BPOS|2026-02-20|edgar/data/1234567/0001234567-26-000333.txt"
    parsed = parse_master_index_line(line)
    assert parsed is not None
    assert parsed["form_type"] == "485BPOS"
    assert parsed["cik"] == "1234567"
    assert parsed["accession_number"] == "0001234567-26-000333"
    assert parsed["filing_link"].startswith("https://www.sec.gov/Archives/")


def test_master_index_url_for_date() -> None:
    url = master_index_url_for_date(date(2026, 2, 20))
    assert url.endswith("/2026/QTR1/master.20260220.idx")


def test_select_primary_document_url_prefers_ixviewer_doc_link() -> None:
    html = """
    <html><body>
      <table class="tableFile">
        <tr><th>Seq</th><th>Description</th><th>Document</th><th>Type</th></tr>
        <tr>
          <td>1</td><td>Main</td>
          <td><a href="/ixviewer/ix.html?doc=/Archives/edgar/data/123/0000000000-26-000001.htm">doc</a></td>
          <td>485BPOS</td>
        </tr>
      </table>
    </body></html>
    """
    url = select_primary_document_url("https://www.sec.gov/Archives/edgar/data/123/000000000026000001/index.html", html, "485BPOS")
    assert url == "https://www.sec.gov/Archives/edgar/data/123/0000000000-26-000001.htm"


def test_select_primary_document_url_rejects_sec_home_index() -> None:
    html = """
    <html><body>
      <a href="/index.htm">SEC Home</a>
      <a href="https://www.sec.gov/index.htm">SEC Home Absolute</a>
    </body></html>
    """
    index_url = "https://www.sec.gov/Archives/edgar/data/123/000000000026000001/index.html"
    url = select_primary_document_url(index_url, html, "485BPOS")
    assert url == index_url


def test_clean_extracted_text_removes_sec_boilerplate() -> None:
    dirty = (
        "SEC.gov | Home Skip to main content An official website of the United States government "
        "Here's how you know Official websites use .gov Real filing content starts here."
    )
    cleaned = clean_extracted_text(dirty)
    assert "SEC.gov | Home" not in cleaned
    assert "Skip to main content" not in cleaned
    assert "Real filing content starts here." in cleaned
