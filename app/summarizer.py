import asyncio
import re

from app.config import Settings

SYSTEM_INSTRUCTION = (
    "Summarize this ETF filing for a financial reporter. Include: Fund Name, "
    "Ticker (if present), Expense Ratio, and a 2-sentence breakdown of the "
    "investment strategy. If it's a crypto ETF, specifically highlight the custodian."
)


def _fallback_summary(text: str, is_crypto: bool) -> str:
    cleaned = " ".join(text.split())
    sentences = re.split(r"(?<=[.!?])\s+", cleaned)
    preview = " ".join(sentences[:2]).strip()
    if not preview:
        preview = cleaned[:450].strip()
    if not preview:
        preview = "No filing text was available for summary."

    lines = [
        "Fund Name: Not clearly stated",
        "Ticker: Not found",
        "Expense Ratio: Not found",
        f"Strategy: {preview}",
    ]
    if is_crypto:
        lines.append("Custodian: Review filing text for named custodian (not automatically extracted).")
    return "\n".join(lines)


def _call_gemini(model_name: str, api_key: str, filing_text: str, is_crypto: bool) -> str:
    import google.generativeai as genai

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(
        model_name=model_name,
        system_instruction=SYSTEM_INSTRUCTION,
    )
    prompt = (
        f"Crypto ETF context: {'yes' if is_crypto else 'no'}\n\n"
        "Filing text:\n"
        f"{filing_text[:25_000]}"
    )
    response = model.generate_content(prompt)
    response_text = (getattr(response, "text", "") or "").strip()
    if not response_text:
        raise RuntimeError("Gemini returned an empty response.")
    return response_text


async def generate_synopsis(filing_text: str, is_crypto: bool, settings: Settings) -> str:
    if not filing_text.strip():
        return _fallback_summary(filing_text, is_crypto)

    if not settings.gemini_api_key:
        return _fallback_summary(filing_text, is_crypto)

    try:
        return await asyncio.to_thread(
            _call_gemini,
            settings.gemini_model,
            settings.gemini_api_key,
            filing_text,
            is_crypto,
        )
    except Exception:
        return _fallback_summary(filing_text, is_crypto)
