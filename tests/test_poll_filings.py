from scripts.poll_filings import crypto_gate, extract_accession_from_link, extract_company_and_cik_from_title


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
