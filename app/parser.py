import re
from typing import Any

DOC_START = b"<SEC-DOCUMENT>"
DOC_END = b"</SEC-DOCUMENT>"
MAX_REMAINDER_BYTES = 2_000_000

SUBMISSION_BLOCK_RE = re.compile(r"<SUBMISSION>(.*?)</SUBMISSION>", re.IGNORECASE | re.DOTALL)

FIELD_PATTERNS: dict[str, list[str]] = {
    "form_type": [
        r"<FORM-TYPE>\s*([^<\r\n]+)",
        r"<TYPE>\s*([^<\r\n]+)",
        r"FORM-TYPE:\s*([^\r\n]+)",
        r"CONFORMED SUBMISSION TYPE:\s*([^\r\n]+)",
    ],
    "cik": [
        r"<CIK>\s*([^<\r\n]+)",
        r"CIK:\s*([0-9]+)",
        r"CENTRAL INDEX KEY:\s*([0-9]+)",
    ],
    "company_name": [
        r"<COMPANY-NAME>\s*([^<\r\n]+)",
        r"COMPANY-NAME:\s*([^\r\n]+)",
        r"COMPANY CONFORMED NAME:\s*([^\r\n]+)",
        r"<CONFORMED-NAME>\s*([^<\r\n]+)",
    ],
    "accession_number": [
        r"<ACCESSION-NUMBER>\s*([^<\r\n]+)",
        r"ACCESSION-NUMBER:\s*([^\r\n]+)",
        r"ACCESSION NUMBER:\s*([^\r\n]+)",
    ],
}


def _extract_submission_block(document_text: str) -> str:
    match = SUBMISSION_BLOCK_RE.search(document_text)
    if match:
        return match.group(1)

    parts = re.split(r"<DOCUMENT>", document_text, flags=re.IGNORECASE, maxsplit=1)
    return parts[0] if parts else document_text


def _extract_field(text: str, field_name: str) -> str:
    patterns = FIELD_PATTERNS[field_name]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return ""


def parse_submission(document_text: str) -> dict[str, Any] | None:
    submission_text = _extract_submission_block(document_text)
    form_type = _extract_field(submission_text, "form_type")
    cik = _extract_field(submission_text, "cik")
    company_name = _extract_field(submission_text, "company_name")
    accession_number = _extract_field(submission_text, "accession_number")

    if not (form_type or cik or company_name):
        return None

    return {
        "form_type": form_type,
        "cik": cik,
        "company_name": company_name,
        "accession_number": accession_number,
        "raw_submission": submission_text,
        "raw_text": document_text,
    }


def process_pds_stream(buffer: bytes | str) -> tuple[list[dict[str, Any]], bytes]:
    if isinstance(buffer, str):
        data = buffer.encode("utf-8", errors="ignore")
    else:
        data = buffer

    filings: list[dict[str, Any]] = []
    cursor = 0

    while True:
        start = data.find(DOC_START, cursor)
        if start == -1:
            break

        end = data.find(DOC_END, start)
        if end == -1:
            break

        end += len(DOC_END)
        document_bytes = data[start:end]
        document_text = document_bytes.decode("utf-8", errors="ignore")
        filing = parse_submission(document_text)
        if filing:
            filings.append(filing)

        cursor = end

    remainder = data[cursor:] if cursor else data
    if len(remainder) > MAX_REMAINDER_BYTES:
        remainder = remainder[-MAX_REMAINDER_BYTES:]

    return filings, remainder
