import asyncio
import re

from app.config import Settings

SYSTEM_INSTRUCTION = (
    "You are assisting a financial reporter. Output concise, factual filing summaries. "
    "Never include SEC website navigation or .gov boilerplate text."
)

BOILERPLATE_PATTERNS = [
    r"SEC\.gov\s*\|\s*Home",
    r"Skip to search field",
    r"Skip to main content",
    r"An official website of the United States government",
    r"Here's how you know",
    r"Official websites use \.gov",
    r"A \.gov website belongs to an official government organization in the United States",
    r"Secure \.gov websites use HTTPS",
]

NARRATIVE_MARKERS = [
    "summary prospectus",
    "fund summary",
    "investment objective",
    "principal investment strategy",
    "principal investment strategies",
    "investment strategy",
    "principal risks",
    "fees and expenses",
    "management",
]

NARRATIVE_KEYWORDS = [
    "fund",
    "etf",
    "investment",
    "strategy",
    "objective",
    "portfolio",
    "index",
    "benchmark",
    "risk",
    "expense",
    "advisor",
    "custodian",
    "bitcoin",
    "ethereum",
    "digital asset",
]

NOISE_TOKENS = [
    "us-gaap",
    "xbrl",
    "xbrli",
    "xbrldi",
    "contextref",
    "unitref",
    "xmlns",
    "schema",
    "defref",
]

GENERIC_FUND_NAMES = {
    "the fund",
    "the funds",
    "fund",
    "funds",
    "trust",
    "etf",
}


def _clean_text(text: str) -> str:
    cleaned = " ".join(text.split())
    for pattern in BOILERPLATE_PATTERNS:
        cleaned = re.sub(pattern, " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(?:us-gaap|xbrli|xbrldi|dei|link|xlink)\b[:\w\-]*", " ", cleaned, flags=re.IGNORECASE)
    return " ".join(cleaned.split())


def _extract_narrative_text(text: str, max_chars: int = 25_000) -> str:
    cleaned = _clean_text(text)
    if not cleaned:
        return ""

    lower = cleaned.lower()
    windows: list[str] = []
    seen: set[tuple[int, int]] = set()
    for marker in NARRATIVE_MARKERS:
        for match in re.finditer(re.escape(marker), lower):
            start = max(0, match.start() - 1_500)
            end = min(len(cleaned), match.end() + 5_000)
            key = (start, end)
            if key in seen:
                continue
            seen.add(key)
            windows.append(cleaned[start:end])
            if len(windows) >= 8:
                break
        if len(windows) >= 8:
            break

    source = " ".join(windows).strip() if windows else cleaned[: max_chars * 2]
    sentences = re.split(r"(?<=[.!?])\s+", source)
    scored: list[tuple[float, int, str]] = []
    for idx, sentence in enumerate(sentences):
        s = sentence.strip()
        if len(s) < 50 or len(s) > 600:
            continue
        s_lower = s.lower()
        if any(token in s_lower for token in NOISE_TOKENS):
            continue

        digits = sum(ch.isdigit() for ch in s)
        digit_ratio = digits / max(len(s), 1)
        if digit_ratio > 0.25:
            continue

        score = 0.0
        for kw in NARRATIVE_KEYWORDS:
            if kw in s_lower:
                score += 2.0
        score += max(0.0, min(len(s), 260) / 260.0)
        scored.append((score, idx, s))

    if not scored:
        return source[:max_chars]

    top = sorted(scored, key=lambda item: item[0], reverse=True)[:40]
    ordered = [item[2] for item in sorted(top, key=lambda item: item[1])]
    narrative = " ".join(ordered).strip()
    if len(narrative) < 1_200:
        narrative = source[:max_chars]
    return narrative[:max_chars]


def _extract_first(patterns: list[str], text: str, default: str = "Unknown") -> str:
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            value = match.group(1).strip(" .;:,")
            if value:
                return value
    return default


def _sanitize_name(value: str, default: str = "Unknown") -> str:
    cleaned = " ".join(value.split()).strip(" .;:,")
    cleaned = re.sub(r"^(?:The\s+)?Prospectus\s+for\s+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^(?:The\s+)?Statement of Additional Information\s+for\s+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^(?:Class\s+[A-Z0-9]+\s+)+", "", cleaned, flags=re.IGNORECASE)
    if not cleaned:
        return default
    if len(cleaned) > 120:
        return default
    if cleaned[0].islower():
        return default
    bad_fragments = ["reports and certain other information", "skip to", "official website"]
    if any(fragment in cleaned.lower() for fragment in bad_fragments):
        return default
    if cleaned.lower() in GENERIC_FUND_NAMES:
        return default
    return cleaned


def _collapse_to_single_fund_name(value: str) -> str:
    parts = re.split(r",| and ", value)
    for part in parts:
        candidate = part.strip(" .;:,")
        if re.search(r"\b(Fund|Trust|ETF)\b", candidate):
            sanitized = _sanitize_name(candidate)
            if sanitized != "Unknown":
                return sanitized
    return _sanitize_name(value)


def _extract_fund_name(text: str) -> str:
    direct = _extract_first(
        [
            r"(?:Fund Name|Series Name|Name of Fund)\s*[:\-]\s*([^\n\r:]{3,120}?)(?=\s(?:Ticker|Ticker Symbol|Expense Ratio|Strategy|$))",
            r"\b([A-Z][A-Za-z0-9&,\-\.]*(?:\s+[A-Z][A-Za-z0-9&,\-\.]*){0,8}\s(?:Fund|Trust|ETF)(?:,\s*Inc\.)?)\b",
        ],
        text,
        default="Unknown",
    )
    direct_clean = _sanitize_name(direct)
    if direct_clean != "Unknown":
        return _collapse_to_single_fund_name(direct_clean)

    prospectus_block = _extract_first(
        [
            r"Prospectus dated [^.]{0,120} for ([^.]{20,500})\.",
            r"read in conjunction with[^.]{0,80}for ([^.]{20,500})\.",
        ],
        text,
        default="",
    )
    if prospectus_block:
        for match in re.finditer(r"([A-Z][A-Za-z0-9&,\-\. ]{2,80}\s(?:Fund|Trust|ETF))", prospectus_block):
            candidate = _sanitize_name(match.group(1))
            if candidate != "Unknown":
                return _collapse_to_single_fund_name(candidate)

    return "Unknown"


def _fund_context(text: str, fund_name: str, window: int = 12_000) -> str:
    if not text:
        return ""
    if fund_name and fund_name != "Unknown":
        idx = text.lower().find(fund_name.lower())
        if idx >= 0:
            start = max(0, idx - 800)
            end = min(len(text), idx + window)
            return text[start:end]
    return text[:window]


def _extract_ticker(text: str, fund_name: str) -> str:
    context = _fund_context(text, fund_name)
    if fund_name and fund_name != "Unknown":
        row_match = re.search(
            rf"{re.escape(fund_name)}(?:\s*\([^)]{{1,120}}\))?\s+([A-Z\?]{{2,6}})\s+([A-Z\?]{{1,6}})\s+([A-Z\?]{{1,6}})\s+([A-Z\?]{{2,6}})",
            context,
            flags=re.IGNORECASE,
        )
        if row_match:
            class_i = row_match.group(4).upper()
            if class_i != "?":
                return class_i
            for idx in [1, 2, 3]:
                v = row_match.group(idx).upper()
                if v != "?":
                    return v

    for pattern in [
        r"Ticker Symbols?\s*[:\-]\s*([^\n]{5,500})",
        r"(?:Ticker|Ticker Symbol)\s*[:\-]\s*([A-Z]{1,6})",
    ]:
        match = re.search(pattern, context, flags=re.IGNORECASE)
        if not match:
            continue
        value = match.group(1).strip()
        # Prefer Class I when available, otherwise first symbol.
        class_i = re.search(r"Class\s+I[—–\-:\s]*([A-Z]{2,6})", value, flags=re.IGNORECASE)
        if class_i:
            return class_i.group(1).upper()
        symbols = re.findall(r"\b[A-Z]{2,6}\b", value)
        if symbols:
            return symbols[0].upper()
    return "Unknown"


def _extract_expense_ratio(text: str, fund_name: str) -> str:
    context = _fund_context(text, fund_name)
    # Prefer post-waiver value when present.
    match = re.search(
        r"Total Annual Fund Operating Expenses After Fee Waivers[^%]{0,120}?([0-9]+(?:\.[0-9]+)?\s*%)",
        context,
        flags=re.IGNORECASE,
    )
    if match:
        return match.group(1).replace(" ", "")
    match = re.search(
        r"Total Annual Fund Operating Expenses[^%]{0,120}?([0-9]+(?:\.[0-9]+)?\s*%)",
        context,
        flags=re.IGNORECASE,
    )
    if match:
        return match.group(1).replace(" ", "")
    match = re.search(r"Expense Ratio\s*[:\-]?\s*([0-9]+(?:\.[0-9]+)?\s*%)", context, flags=re.IGNORECASE)
    if match:
        return match.group(1).replace(" ", "")
    return "Unknown"


def _extract_strategy_hint(text: str, fund_name: str) -> str:
    context = _fund_context(text, fund_name, window=20_000)
    objective = _extract_first(
        [
            r"Investment Objective\s+(.{30,600}?)(?=\s+Fees and Expenses|\s+Principal Investment Strateg(?:y|ies)|\s+Principal Risks)",
        ],
        context,
        default="",
    )
    principal = _extract_first(
        [
            r"Principal Investment Strateg(?:y|ies)\s+(.{30,900}?)(?=\s+Principal Risks|\s+Portfolio Managers|\s+Management|\s+Purchase and Sale)",
        ],
        context,
        default="",
    )
    bits = []
    if objective:
        bits.append(_normalize_strategy_text(objective))
    if principal:
        bits.append(_normalize_strategy_text(principal))
    merged = " ".join(bits).strip()
    return _normalize_strategy_text(merged) if merged else "Not available."


def _normalize_strategy_text(value: str) -> str:
    text = " ".join(value.split())
    if not text:
        return "Not available."
    chunks = re.split(r"(?<=[.!?])\s+", text)
    if len(chunks) < 2:
        chunks = re.split(r"[;:]\s+", text)
    chosen = " ".join(chunks[:2]).strip()
    if len(chosen) > 420:
        chosen = chosen[:420].rstrip() + "..."
    return chosen or "Not available."


def _normalize_summary(summary: str, is_crypto: bool, hints: dict[str, str] | None = None) -> str:
    lines = [line.strip() for line in summary.splitlines() if line.strip()]
    parsed: dict[str, str] = {}
    for line in lines:
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        parsed[key.strip().lower()] = value.strip()

    hints = hints or {}
    fund_name = _sanitize_name(parsed.get("fund name", "Unknown"))
    if fund_name == "Unknown" and hints.get("fund_name"):
        fund_name = _sanitize_name(hints.get("fund_name", "Unknown"))

    ticker = parsed.get("ticker", "Unknown").strip() or "Unknown"
    if ticker == "Unknown" and hints.get("ticker"):
        ticker = hints["ticker"]

    expense_ratio = parsed.get("expense ratio", "Unknown").strip() or "Unknown"
    if expense_ratio == "Unknown" and hints.get("expense_ratio"):
        expense_ratio = hints["expense_ratio"]
    strategy = _normalize_strategy_text(parsed.get("strategy", ""))
    if (
        strategy == "Not available."
        or "statement of additional information" in strategy.lower()
        or "should be read in conjunction with" in strategy.lower()
    ) and hints.get("strategy"):
        strategy = _normalize_strategy_text(hints["strategy"])

    output = [
        f"Fund Name: {fund_name}",
        f"Ticker: {ticker}",
        f"Expense Ratio: {expense_ratio}",
        f"Strategy: {strategy}",
    ]
    if is_crypto:
        custodian = _sanitize_name(parsed.get("custodian", "Unknown"))
        output.append(f"Custodian: {custodian}")
    return "\n".join(output)


def _extract_structured_fields(text: str, is_crypto: bool) -> dict[str, str]:
    fund_name = _extract_fund_name(text)
    ticker = _extract_ticker(text, fund_name)
    expense_ratio = _extract_expense_ratio(text, fund_name)
    strategy = _extract_strategy_hint(text, fund_name)
    custodian = _extract_first(
        [
            r"(?:Custodian|Crypto Custodian)\s*[:\-]\s*([A-Za-z0-9&,\-\. ]{3,120})",
            r"(Coinbase Custody)",
        ],
        text,
        default="Unknown" if is_crypto else "N/A",
    )
    return {
        "fund_name": fund_name,
        "ticker": ticker,
        "expense_ratio": expense_ratio,
        "custodian": custodian,
        "strategy": strategy,
    }


def _build_chunks(text: str, chunk_size: int = 7000, overlap: int = 600, max_chunks: int = 3) -> list[str]:
    chunks: list[str] = []
    cursor = 0
    n = len(text)
    while cursor < n and len(chunks) < max_chunks:
        end = min(cursor + chunk_size, n)
        chunk = text[cursor:end].strip()
        if chunk:
            chunks.append(chunk)
        if end >= n:
            break
        cursor = max(end - overlap, 0)
    return chunks


def _is_low_quality_summary(summary: str) -> bool:
    if not summary.strip() or len(summary.strip()) < 80:
        return True
    lower = summary.lower()
    bad_signals = [
        "skip to search field",
        "official websites use .gov",
        "sec.gov | home",
        "an official website of the united states government",
    ]
    if any(signal in lower for signal in bad_signals):
        return True
    if lower.count("not found") >= 2:
        return True
    return False


def _fallback_summary(text: str, is_crypto: bool) -> str:
    narrative = _extract_narrative_text(text)
    fields = _extract_structured_fields(_clean_text(text), is_crypto)
    sentences = re.split(r"(?<=[.!?])\s+", narrative)
    preview = fields.get("strategy", "").strip() or " ".join(sentences[:2]).strip() or narrative[:450].strip()
    if not preview:
        preview = "No filing text was available for summary."

    lines = [
        f"Fund Name: {fields['fund_name']}",
        f"Ticker: {fields['ticker']}",
        f"Expense Ratio: {fields['expense_ratio']}",
        f"Strategy: {preview}",
    ]
    if is_crypto:
        lines.append(f"Custodian: {fields['custodian']}")
    return _normalize_summary("\n".join(lines), is_crypto, hints=fields)


def _call_gemini(model_name: str, api_key: str, prompt: str) -> str:
    import google.generativeai as genai

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(model_name=model_name, system_instruction=SYSTEM_INSTRUCTION)
    response = model.generate_content(prompt)
    response_text = (getattr(response, "text", "") or "").strip()
    if not response_text:
        raise RuntimeError("Gemini returned an empty response.")
    return response_text


def _synthesize_with_gemini(model_name: str, api_key: str, filing_text: str, is_crypto: bool) -> str:
    cleaned_full = _clean_text(filing_text)
    narrative = _extract_narrative_text(filing_text)
    fields = _extract_structured_fields(cleaned_full, is_crypto)
    chunks = _build_chunks(narrative, chunk_size=5000, overlap=500, max_chunks=2)

    chunk_summaries: list[str] = []
    for idx, chunk in enumerate(chunks, start=1):
        chunk_prompt = (
            "Summarize the investment strategy and key filing facts in 2 short sentences. "
            "Exclude SEC site boilerplate.\n\n"
            f"Chunk {idx}:\n{chunk}"
        )
        chunk_summaries.append(_call_gemini(model_name, api_key, chunk_prompt))

    final_prompt = (
        "Return exactly these lines:\n"
        "Fund Name: <value>\n"
        "Ticker: <value or Unknown>\n"
        "Expense Ratio: <value or Unknown>\n"
        "Strategy: <exactly 2 sentences>\n"
        + ("Custodian: <value or Unknown>\n" if is_crypto else "")
        + "Do not include SEC.gov navigation text.\n\n"
        f"Extracted hints:\n"
        f"- Fund Name hint: {fields['fund_name']}\n"
        f"- Ticker hint: {fields['ticker']}\n"
        f"- Expense Ratio hint: {fields['expense_ratio']}\n"
        f"- Strategy hint: {fields['strategy']}\n"
        + (f"- Custodian hint: {fields['custodian']}\n" if is_crypto else "")
        + "\nChunk summaries:\n"
        + "\n".join(f"- {item}" for item in chunk_summaries)
    )
    summary = _call_gemini(model_name, api_key, final_prompt)
    if _is_low_quality_summary(summary):
        retry_prompt = final_prompt + "\n\nRetry with cleaner output and no boilerplate text."
        summary = _call_gemini(model_name, api_key, retry_prompt)
    return _normalize_summary(summary, is_crypto, hints=fields)


async def generate_synopsis(filing_text: str, is_crypto: bool, settings: Settings) -> str:
    if not filing_text.strip():
        return _fallback_summary(filing_text, is_crypto)

    if not settings.gemini_api_key:
        return _fallback_summary(filing_text, is_crypto)

    try:
        summary = await asyncio.to_thread(
            _synthesize_with_gemini,
            settings.gemini_model,
            settings.gemini_api_key,
            filing_text,
            is_crypto,
        )
        if _is_low_quality_summary(summary):
            return _fallback_summary(filing_text, is_crypto)
        return summary
    except Exception:
        return _fallback_summary(filing_text, is_crypto)
