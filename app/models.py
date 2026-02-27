from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field


class ParsedFiling(BaseModel):
    form_type: str = ""
    cik: str = ""
    company_name: str = ""
    accession_number: str = ""
    raw_submission: str = ""
    raw_text: str = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class AlertRecord(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    form_type: str
    cik: str
    company_name: str
    accession_number: str
    sec_index_url: str
    sec_filing_url: str | None = None
    primary_document_url: str | None = None
    matched_keywords: list[str] = Field(default_factory=list)
    is_crypto: bool = False
    synopsis: str = ""
    pdf_path: str | None = None
    email_sent: bool = False
    error: str | None = None
    debug: dict[str, Any] = Field(default_factory=dict)


class StreamStatus(BaseModel):
    running: bool
    connected: bool
    pds_host: str
    pds_port: int
    processed_count: int
    alerted_count: int
    last_error: str | None
    last_event_at: datetime | None


class IngestPayload(BaseModel):
    payload: str
