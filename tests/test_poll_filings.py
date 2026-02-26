from datetime import date

from scripts.poll_filings import (
    crypto_gate,
    extract_accession_from_filename,
    extract_accession_from_link,
    extract_company_and_cik_from_title,
    master_index_url_for_date,
    parse_master_index_line,
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
