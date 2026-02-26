import asyncio
import base64
import mimetypes
import smtplib
from email.message import EmailMessage
from pathlib import Path

import httpx

from app.config import Settings


def _build_message(
    from_email: str,
    to_email: str,
    subject: str,
    body: str,
    attachment_path: str | None,
) -> EmailMessage:
    message = EmailMessage()
    message["From"] = from_email
    message["To"] = to_email
    message["Subject"] = subject
    message.set_content(body)

    if attachment_path:
        path = Path(attachment_path)
        if path.exists():
            mime_type, _ = mimetypes.guess_type(path.name)
            maintype, subtype = ("application", "pdf")
            if mime_type and "/" in mime_type:
                maintype, subtype = mime_type.split("/", 1)
            data = path.read_bytes()
            message.add_attachment(data, maintype=maintype, subtype=subtype, filename=path.name)

    return message


def _send_via_smtp(
    settings: Settings,
    to_email: str,
    subject: str,
    body: str,
    attachment_path: str | None,
) -> tuple[bool, str | None]:
    if not settings.smtp_host:
        return False, "SMTP is not configured."

    message = _build_message(
        from_email=settings.from_email,
        to_email=to_email,
        subject=subject,
        body=body,
        attachment_path=attachment_path,
    )

    with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=30) as server:
        if settings.smtp_use_tls:
            server.starttls()
        if settings.smtp_username:
            server.login(settings.smtp_username, settings.smtp_password)
        server.send_message(message)

    return True, None


async def _send_via_resend(
    settings: Settings,
    to_email: str,
    subject: str,
    body: str,
    attachment_path: str | None,
) -> tuple[bool, str | None]:
    from_email = settings.resend_from_email or settings.from_email
    payload: dict[str, object] = {
        "from": from_email,
        "to": [to_email],
        "subject": subject,
        "text": body,
    }

    if attachment_path:
        path = Path(attachment_path)
        if path.exists():
            payload["attachments"] = [
                {
                    "content": base64.b64encode(path.read_bytes()).decode("utf-8"),
                    "filename": path.name,
                }
            ]

    headers = {
        "Authorization": f"Bearer {settings.resend_api_key}",
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(
            "https://api.resend.com/emails",
            headers=headers,
            json=payload,
        )
    if response.status_code >= 400:
        return False, f"Resend error {response.status_code}: {response.text}"

    return True, None


async def send_email_alert(
    settings: Settings,
    to_email: str,
    subject: str,
    body: str,
    attachment_path: str | None,
) -> tuple[bool, str | None]:
    try:
        if settings.resend_api_key:
            return await _send_via_resend(settings, to_email, subject, body, attachment_path)
        return await asyncio.to_thread(
            _send_via_smtp,
            settings,
            to_email,
            subject,
            body,
            attachment_path,
        )
    except Exception as exc:
        return False, str(exc)
