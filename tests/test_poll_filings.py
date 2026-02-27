from datetime import date

from scripts.poll_filings import (
    build_index_url,
    clean_extracted_text,
    crypto_gate,
    extract_accession_from_filename,
    extract_accession_from_link,
    extract_company_and_cik_from_title,
    is_valid_archive_url,
    master_index_url_for_date,
    parse_master_index_line,
    select_primary_document_url,
    to_ix_url,
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


def test_build_index_url_uses_accession_index_page() -> None:
    url = build_index_url("000820892", "0001193125-26-079024")
    assert url.endswith("/820892/000119312526079024/0001193125-26-079024-index.html")


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


def test_select_primary_document_url_from_directory_listing_prefers_real_doc() -> None:
    html = """
    <html><body><table>
      <tr><td><a href="0001193125-26-079024-index-headers.html">headers</a></td><td>1234</td></tr>
      <tr><td><a href="d86423d485bpos.htm">main</a></td><td>5091852</td></tr>
      <tr><td><a href="d86423d485bpos_htm.xml">xml</a></td><td>779371</td></tr>
    </table></body></html>
    """
    index_url = "https://www.sec.gov/Archives/edgar/data/820892/000119312526079024/index.html"
    url = select_primary_document_url(index_url, html, "485BPOS")
    assert url.endswith("/d86423d485bpos.htm")


def test_clean_extracted_text_removes_sec_boilerplate() -> None:
    dirty = (
        "SEC.gov | Home Skip to main content An official website of the United States government "
        "Here's how you know Official websites use .gov Real filing content starts here."
    )
    cleaned = clean_extracted_text(dirty)
    assert "SEC.gov | Home" not in cleaned
    assert "Skip to main content" not in cleaned
    assert "Real filing content starts here." in cleaned


def test_is_valid_archive_url_rejects_directory_index() -> None:
    assert not is_valid_archive_url("https://www.sec.gov/Archives/edgar/data/1293967/000089418926006377/index.html")
    assert is_valid_archive_url("https://www.sec.gov/Archives/edgar/data/1293967/000089418926006377/0000894189-26-006377-index.html")


def test_to_ix_url_wraps_archive_document() -> None:
    url = "https://www.sec.gov/Archives/edgar/data/820892/000119312526079024/d86423d485bpos.htm"
    ix = to_ix_url(url)
    assert ix == "https://www.sec.gov/ix?doc=/Archives/edgar/data/820892/000119312526079024/d86423d485bpos.htm"
