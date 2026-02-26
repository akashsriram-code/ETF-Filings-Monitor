from app.filters import evaluate_filing_gate
from app.parser import process_pds_stream


def test_process_pds_stream_extracts_submission_fields() -> None:
    payload = b"""
<SEC-DOCUMENT>
<SUBMISSION>
FORM-TYPE: 485BPOS
CIK: 0001234567
COMPANY-NAME: Example ETF Trust
ACCESSION-NUMBER: 0001234567-26-000321
</SUBMISSION>
</SEC-DOCUMENT>
"""
    filings, remainder = process_pds_stream(payload)

    assert len(filings) == 1
    filing = filings[0]
    assert filing["form_type"] == "485BPOS"
    assert filing["cik"] == "0001234567"
    assert filing["company_name"] == "Example ETF Trust"
    assert filing["accession_number"] == "0001234567-26-000321"
    assert remainder.strip() == b""


def test_process_pds_stream_keeps_partial_document_for_next_chunk() -> None:
    part_1 = b"<SEC-DOCUMENT><SUBMISSION>FORM-TYPE: 485APOS\nCIK: 0000001"
    filings, remainder = process_pds_stream(part_1)
    assert len(filings) == 0
    assert remainder == part_1

    part_2 = (
        remainder
        + b"\nCOMPANY-NAME: Demo ETF\nACCESSION-NUMBER: 0000001-26-000111"
        + b"</SUBMISSION></SEC-DOCUMENT>"
    )
    filings_2, remainder_2 = process_pds_stream(part_2)
    assert len(filings_2) == 1
    assert filings_2[0]["form_type"] == "485APOS"
    assert remainder_2 == b""


def test_s1_crypto_keyword_gate() -> None:
    should_alert, matched_keywords, is_crypto = evaluate_filing_gate(
        "S-1",
        "This filing introduces a Spot Bitcoin strategy with Coinbase Custody.",
        ["Bitcoin", "Ethereum", "Digital Asset", "Spot", "Coinbase Custody"],
    )
    assert should_alert is True
    assert is_crypto is True
    assert "Bitcoin" in matched_keywords
    assert "Spot" in matched_keywords
