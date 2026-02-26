import asyncio
from collections import deque
from datetime import datetime, timezone
from typing import Any

from app.config import Settings
from app.emailer import send_email_alert
from app.filters import evaluate_filing_gate
from app.models import AlertRecord, ParsedFiling, StreamStatus
from app.parser import process_pds_stream
from app.sec_client import build_sec_index_url, collect_sec_artifacts
from app.summarizer import generate_synopsis


class FilingEngine:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._buffer = bytearray()
        self._alerts: deque[AlertRecord] = deque(maxlen=settings.alerts_retention)

        self._running = False
        self._connected = False
        self._processed_count = 0
        self._alerted_count = 0
        self._last_error: str | None = None
        self._last_event_at: datetime | None = None

        self._stream_task: asyncio.Task[None] | None = None
        self._active_processing_tasks: set[asyncio.Task[None]] = set()
        self._state_lock = asyncio.Lock()

    async def start(self) -> None:
        if self._stream_task and not self._stream_task.done():
            return
        self._running = True
        self._stream_task = asyncio.create_task(self._stream_loop(), name="pds-stream-loop")

    async def stop(self) -> None:
        self._running = False
        self._connected = False

        if self._stream_task and not self._stream_task.done():
            self._stream_task.cancel()
            try:
                await self._stream_task
            except asyncio.CancelledError:
                pass

        if self._active_processing_tasks:
            tasks = list(self._active_processing_tasks)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._active_processing_tasks.clear()

    async def get_status(self) -> StreamStatus:
        async with self._state_lock:
            return StreamStatus(
                running=self._running,
                connected=self._connected,
                pds_host=self.settings.pds_host,
                pds_port=self.settings.pds_port,
                processed_count=self._processed_count,
                alerted_count=self._alerted_count,
                last_error=self._last_error,
                last_event_at=self._last_event_at,
            )

    async def get_alerts(self) -> list[AlertRecord]:
        async with self._state_lock:
            return list(self._alerts)

    async def ingest_chunk(self, chunk: bytes) -> int:
        self._buffer.extend(chunk)
        filings, remainder = process_pds_stream(bytes(self._buffer))
        self._buffer = bytearray(remainder)

        for filing_data in filings:
            task = asyncio.create_task(self._process_filing(filing_data))
            self._active_processing_tasks.add(task)
            task.add_done_callback(self._active_processing_tasks.discard)

        return len(filings)

    async def ingest_payload(self, payload: str) -> int:
        return await self.ingest_chunk(payload.encode("utf-8", errors="ignore"))

    async def _stream_loop(self) -> None:
        while self._running:
            writer: asyncio.StreamWriter | None = None
            try:
                reader, writer = await asyncio.open_connection(
                    self.settings.pds_host,
                    self.settings.pds_port,
                )
                async with self._state_lock:
                    self._connected = True
                    self._last_error = None

                while self._running:
                    chunk = await reader.read(self.settings.pds_chunk_size)
                    if not chunk:
                        raise ConnectionError("PDS stream closed connection.")
                    await self.ingest_chunk(chunk)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                async with self._state_lock:
                    self._connected = False
                    self._last_error = str(exc)
                if self._running:
                    await asyncio.sleep(self.settings.pds_reconnect_seconds)
            finally:
                if writer:
                    writer.close()
                    await writer.wait_closed()
                async with self._state_lock:
                    self._connected = False

    async def _process_filing(self, filing_data: dict[str, Any]) -> None:
        filing = ParsedFiling(**filing_data)

        async with self._state_lock:
            self._processed_count += 1
            self._last_event_at = datetime.now(timezone.utc)

        should_alert, matched_keywords, is_crypto = evaluate_filing_gate(
            filing.form_type,
            filing.raw_text,
            self.settings.crypto_keywords,
        )
        if not should_alert:
            return

        sec_index_url = build_sec_index_url(filing.cik, filing.accession_number)
        primary_document_url: str | None = None
        pdf_path: str | None = None
        primary_text = filing.raw_text
        artifact_error: str | None = None

        try:
            artifacts = await collect_sec_artifacts(filing.cik, filing.accession_number, self.settings)
            sec_index_url = artifacts.get("index_url") or sec_index_url
            primary_document_url = artifacts.get("primary_document_url")
            primary_text = artifacts.get("primary_text") or filing.raw_text
            pdf_path = artifacts.get("pdf_path")
            artifact_error = artifacts.get("error")
        except Exception as exc:
            artifact_error = str(exc)

        synopsis = await generate_synopsis(primary_text, is_crypto, self.settings)
        subject = f"[ETF ALERT] {filing.form_type} Filed by {filing.company_name}"
        body = f"{synopsis}\n\nSEC Link: {sec_index_url}"

        email_sent, email_error = await send_email_alert(
            settings=self.settings,
            to_email=self.settings.reporter_email,
            subject=subject,
            body=body,
            attachment_path=pdf_path,
        )

        error_text = email_error or artifact_error
        alert = AlertRecord(
            form_type=filing.form_type,
            cik=filing.cik,
            company_name=filing.company_name,
            accession_number=filing.accession_number,
            sec_index_url=sec_index_url,
            primary_document_url=primary_document_url,
            matched_keywords=matched_keywords,
            is_crypto=is_crypto,
            synopsis=synopsis,
            pdf_path=pdf_path,
            email_sent=email_sent,
            error=error_text,
            debug={
                "artifact_error": artifact_error,
                "email_error": email_error,
            },
        )

        async with self._state_lock:
            self._alerts.appendleft(alert)
            self._alerted_count += 1
            if error_text:
                self._last_error = error_text
