import asyncio
import re

import httpx

from app.config import Settings

SYSTEM_INSTRUCTION = (
    "You are assisting a financial reporter. Output concise, factual filing summaries. "
    "Never include SEC website navigation or .gov boilerplate text. "
    "Return only three fields from ETF filings: Filer, ETF Name, and Strategy."
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
    "advisor",
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

NAME_NOISE_FRAGMENTS = [
    "table of contents",
    "fund summary",
    "additional information about the fund",
    "fees and expenses",
    "summary prospectus",
    "statement of additional information",
    "principal risks",
    "portfolio managers",
    "skip to",
    "official website",
    "sec.gov",
]

STRATEGY_NOISE_FRAGMENTS = [
    "table of contents",
    "annual fund operating expenses",
    "fees and expenses",
    "distribution and service",
    "example assumes that your investment",
    "skip to main content",
    "official websites use .gov",
]


def _is_generic_fund_name(value: str) -> bool:
    lower = " ".join(value.lower().split())
    if not lower:
        return True
    generic_signals = [
        "the fund",
        "the funds",
        "fund is an etf",
        "is an etf",
        "unknown",
        "not clearly stated",
        "table of contents",
    ]
    return lower in GENERIC_FUND_NAMES or any(sig in lower for sig in generic_signals)


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


def _sanitize_name(value: str, default: str = "Unknown", *, allow_generic: bool = False) -> str:
    cleaned = " ".join(value.split()).strip(" .;:,")
    cleaned = re.sub(
        r"^(?:Filer|Filer Name|Company Name|Company Conformed Name|ETF Name|Fund Name|Series Name|Name of Fund)\s*[:\-]\s*",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"^(?:Table of Contents|Contents)\s+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^(?:The\s+)?Prospectus\s+for\s+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^(?:The\s+)?Statement of Additional Information\s+for\s+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^(?:Class\s+[A-Z0-9]+\s+)+", "", cleaned, flags=re.IGNORECASE)
    cleaned = cleaned.strip(" .;:,")
    if not cleaned:
        return default
    if len(cleaned) < 3 or len(cleaned) > 140:
        return default
    if cleaned[0].islower():
        return default
    lower = cleaned.lower()
    bad_fragments = ["reports and certain other information", "should be read in conjunction with", "unknown"]
    if any(fragment in lower for fragment in bad_fragments):
        return default
    if any(fragment in lower for fragment in NAME_NOISE_FRAGMENTS):
        return default
    if re.search(r"\b(seeks|invests|tracks|is an etf|is an exchange-traded fund)\b", lower):
        return default
    if sum(ch.isdigit() for ch in cleaned) / max(len(cleaned), 1) > 0.25:
        return default
    if not allow_generic and _is_generic_fund_name(cleaned):
        return default
    return cleaned


def _candidate_score_etf_name(value: str) -> int:
    lower = value.lower()
    score = 0
    if " etf" in lower:
        score += 5
    if " fund" in lower:
        score += 3
    if " trust" in lower:
        score += 1
    if 2 <= len(value.split()) <= 12:
        score += 2
    if _is_generic_fund_name(value):
        score -= 20
    return score


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
    scored_candidates: list[tuple[int, int, str]] = []

    def add_candidate(raw_value: str, pattern_weight: int, idx: int) -> None:
        sanitized = _collapse_to_single_fund_name(_sanitize_name(raw_value))
        if sanitized == "Unknown":
            return
        score = pattern_weight + _candidate_score_etf_name(sanitized)
        scored_candidates.append((score, idx, sanitized))

    patterns: list[tuple[str, int]] = [
        (
            r"(?:ETF Name|Fund Name|Series Name|Name of Fund)\s*[:\-]\s*([^\n\r:]{3,140}?)(?=\s(?:Ticker|Ticker Symbol|Strategy|Investment Objective|Principal|Fees|Annual|$))",
            12,
        ),
        (
            r"\b([A-Z][A-Za-z0-9&,\-\.']*(?:\s+[A-Z][A-Za-z0-9&,\-\.']*){1,10}\s(?:ETF|Fund|Trust))\s*\(\s*(?:the\s+)?[\"']?Fund[\"']?\s*\)",
            11,
        ),
        (
            r"\b([A-Z][A-Za-z0-9&,\-\.']*(?:\s+[A-Z][A-Za-z0-9&,\-\.']*){1,10}\s(?:ETF|Fund|Trust))\s+seeks\b",
            10,
        ),
        (
            r"\b([A-Z][A-Za-z0-9&,\-\.']*(?:\s+[A-Z][A-Za-z0-9&,\-\.']*){1,10}\s(?:ETF|Fund|Trust)(?:,\s*Inc\.)?)\b",
            6,
        ),
    ]
    for pattern, weight in patterns:
        for idx, match in enumerate(re.finditer(pattern, text, flags=re.IGNORECASE)):
            add_candidate(match.group(1), weight, idx)

    prospectus_block = _extract_first(
        [
            r"Prospectus dated [^.]{0,120} for ([^.]{20,500})\.",
            r"read in conjunction with[^.]{0,80}for ([^.]{20,500})\.",
        ],
        text,
        default="",
    )
    if prospectus_block:
        for idx, match in enumerate(
            re.finditer(r"([A-Z][A-Za-z0-9&,\-\. ']{2,90}\s(?:Fund|Trust|ETF))", prospectus_block)
        ):
            add_candidate(match.group(1), 9, idx)

    if not scored_candidates:
        return "Unknown"
    best = sorted(scored_candidates, key=lambda item: (-item[0], item[1], len(item[2])))[0]
    return best[2]


def _extract_filer_name(text: str, fallback: str = "Unknown") -> str:
    extracted = _extract_first(
        [
            r"<COMPANY-NAME>\s*([^<\r\n]{3,140})",
            r"COMPANY-NAME:\s*([^\r\n]{3,140}?)(?=\s(?:CIK|ETF Name|Fund Name|Series Name|Investment Objective|Principal|Ticker|$)|$)",
            r"COMPANY CONFORMED NAME:\s*([^\r\n]{3,140}?)(?=\s(?:CIK|ETF Name|Fund Name|Series Name|Investment Objective|Principal|Ticker|$)|$)",
            r"(?:Filer Name|Name of Registrant|Registrant Name|Company Name)\s*[:\-]\s*([^\r\n]{3,140}?)(?=\s(?:CIK|ETF Name|Fund Name|Series Name|Investment Objective|Principal|Ticker|$)|$)",
        ],
        text,
        default="",
    )
    candidate = _sanitize_name(extracted, allow_generic=True) if extracted else "Unknown"
    if candidate != "Unknown":
        return candidate
    fallback_clean = _sanitize_name(fallback, allow_generic=True)
    return fallback_clean if fallback_clean != "Unknown" else "Unknown"


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
        class_i = re.search(r"Class\s+I[^\w]*([A-Z]{2,6})", value, flags=re.IGNORECASE)
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
            r"Investment Objective\s+(.{30,700}?)(?=\s+Fees and Expenses|\s+Principal Investment Strateg(?:y|ies)|\s+Principal Risks|\s+Performance)",
        ],
        context,
        default="",
    )
    principal = _extract_first(
        [
            r"Principal Investment Strateg(?:y|ies)\s+(.{30,1100}?)(?=\s+Principal Risks|\s+Portfolio Managers|\s+Management|\s+Purchase and Sale|\s+Fund Performance)",
        ],
        context,
        default="",
    )
    bits: list[str] = []
    if objective:
        bits.append(objective)
    if principal:
        bits.append(principal)
    merged = _normalize_strategy_text(" ".join(bits))
    if merged != "Not available.":
        return merged

    sentences = re.split(r"(?<=[.!?])\s+", context)
    scored: list[tuple[float, int, str]] = []
    for idx, sentence in enumerate(sentences):
        s = " ".join(sentence.split()).strip()
        if len(s) < 45 or len(s) > 420:
            continue
        lower = s.lower()
        if any(fragment in lower for fragment in STRATEGY_NOISE_FRAGMENTS):
            continue
        if sum(ch.isdigit() for ch in s) / max(len(s), 1) > 0.12:
            continue
        score = 0.0
        if fund_name != "Unknown" and fund_name.lower() in lower:
            score += 2.0
        if " seeks " in f" {lower} " or lower.startswith("seeks "):
            score += 4.0
        if "investment objective" in lower:
            score += 4.0
        if "principal investment strategy" in lower or "investment strategy" in lower:
            score += 3.0
        if "invests" in lower or "index" in lower or "portfolio" in lower:
            score += 2.0
        if score > 0:
            scored.append((score, idx, s))

    if not scored:
        return "Not available."

    top = sorted(scored, key=lambda item: item[0], reverse=True)[:4]
    ordered = [item[2] for item in sorted(top, key=lambda item: item[1])][:2]
    return _normalize_strategy_text(" ".join(ordered))


def _normalize_strategy_text(value: str) -> str:
    text = " ".join(value.split())
    if not text:
        return "Not available."
    text = re.sub(r"(?i)\b(?:table of contents|fund summary)\b", " ", text)
    chunks = re.split(r"(?<=[.!?])\s+", text)
    selected: list[str] = []
    for chunk in chunks:
        sentence = " ".join(chunk.split()).strip(" .")
        if len(sentence) < 35 or len(sentence) > 420:
            continue
        lower = sentence.lower()
        if any(fragment in lower for fragment in STRATEGY_NOISE_FRAGMENTS):
            continue
        if not re.search(r"\b(seeks?|invests?|objective|strategy|index|portfolio|exposure|tracks?)\b", lower):
            continue
        selected.append(sentence)
        if len(selected) == 2:
            break

    if not selected and chunks:
        fallback = " ".join(chunks[:2]).strip()
        if fallback and not any(fragment in fallback.lower() for fragment in STRATEGY_NOISE_FRAGMENTS):
            selected = [fallback]

    chosen = " ".join(selected).strip()
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
    raw_filer_name = parsed.get("filer", parsed.get("filer name", parsed.get("company name", "Unknown")))
    filer_name = _sanitize_name(raw_filer_name, allow_generic=True)
    hint_filer_name = _sanitize_name(hints.get("filer_name", "Unknown"), allow_generic=True)
    if hint_filer_name != "Unknown" and filer_name == "Unknown":
        filer_name = hint_filer_name

    raw_etf_name = parsed.get("etf name", parsed.get("fund name", "Unknown"))
    etf_name = _sanitize_name(raw_etf_name)
    hint_etf_name = _sanitize_name(hints.get("etf_name", hints.get("fund_name", "Unknown")))
    if hint_etf_name != "Unknown" and (etf_name == "Unknown" or _is_generic_fund_name(raw_etf_name)):
        etf_name = hint_etf_name

    strategy = _normalize_strategy_text(parsed.get("strategy", ""))
    if (
        strategy == "Not available."
        or strategy.lower().startswith("by using")
        or "statement of additional information" in strategy.lower()
        or "should be read in conjunction with" in strategy.lower()
    ) and hints.get("strategy"):
        strategy = _normalize_strategy_text(hints["strategy"])

    output = [
        f"Filer: {filer_name}",
        f"ETF Name: {etf_name}",
        f"Strategy: {strategy}",
    ]
    return "\n".join(output)


def _extract_structured_fields(text: str, is_crypto: bool, filer_name_hint: str = "Unknown") -> dict[str, str]:
    _ = is_crypto
    filer_name = _extract_filer_name(text, fallback=filer_name_hint)
    etf_name = _extract_fund_name(text)
    strategy = _extract_strategy_hint(text, etf_name)
    return {
        "filer_name": filer_name,
        "etf_name": etf_name,
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
    if not summary.strip():
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
    required = ["filer:", "etf name:", "strategy:"]
    if all(label in lower for label in required):
        has_unknown_names = "filer: unknown" in lower and "etf name: unknown" in lower
        has_no_strategy = "strategy: not available." in lower
        if has_unknown_names and has_no_strategy:
            return True
        return False
    if len(summary.strip()) < 80:
        return True
    if lower.count("not found") >= 2:
        return True
    return False


def _fallback_summary(text: str, is_crypto: bool, filer_name_hint: str = "Unknown") -> str:
    narrative = _extract_narrative_text(text)
    fields = _extract_structured_fields(_clean_text(text), is_crypto, filer_name_hint=filer_name_hint)
    sentences = re.split(r"(?<=[.!?])\s+", narrative)
    preview = fields.get("strategy", "").strip() or " ".join(sentences[:2]).strip() or narrative[:450].strip()
    if not preview:
        preview = "No filing text was available for summary."

    lines = [
        f"Filer: {fields['filer_name']}",
        f"ETF Name: {fields['etf_name']}",
        f"Strategy: {preview}",
    ]
    return _normalize_summary("\n".join(lines), is_crypto, hints=fields)


def _extract_openarena_answer(payload: dict) -> str:
    result = payload.get("result") or {}
    answer = result.get("answer")
    if isinstance(answer, str):
        return answer.strip()
    if isinstance(answer, dict):
        # OpenArena often returns { "<node_name>": "<answer>" }.
        for value in answer.values():
            if isinstance(value, str) and value.strip():
                return value.strip()
    return ""


def _call_openarena(
    base_url: str,
    bearer_token: str,
    workflow_id: str,
    prompt: str,
    timeout_seconds: int,
) -> str:
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
    }
    payload = {
        "query": prompt,
        "workflow_id": workflow_id,
        "is_persistence_allowed": False,
    }
    with httpx.Client(timeout=timeout_seconds, follow_redirects=True, headers=headers) as client:
        response = client.post(f"{base_url.rstrip('/')}/v2/inference", json=payload)
        response.raise_for_status()
        response_json = response.json()
    answer = _extract_openarena_answer(response_json if isinstance(response_json, dict) else {})
    if not answer:
        raise RuntimeError("OpenArena returned an empty answer.")
    return answer


def _synthesize_with_openarena(
    base_url: str,
    bearer_token: str,
    workflow_id: str,
    timeout_seconds: int,
    filing_text: str,
    is_crypto: bool,
    filer_name_hint: str,
) -> str:
    cleaned_full = _clean_text(filing_text)
    narrative = _extract_narrative_text(filing_text)
    fields = _extract_structured_fields(cleaned_full, is_crypto, filer_name_hint=filer_name_hint)
    chunks = _build_chunks(narrative, chunk_size=5000, overlap=500, max_chunks=2)

    chunk_summaries: list[str] = []
    for idx, chunk in enumerate(chunks, start=1):
        chunk_prompt = (
            "Summarize the investment strategy and key filing facts in 2 short sentences. "
            "Exclude SEC site boilerplate.\n\n"
            f"Chunk {idx}:\n{chunk}"
        )
        chunk_summaries.append(
            _call_openarena(base_url, bearer_token, workflow_id, chunk_prompt, timeout_seconds)
        )

    final_prompt = (
        "Strict extraction task. Ignore generic placeholders, headings, and table-of-contents text.\n"
        "Return exactly these lines:\n"
        "Filer: <value or Unknown>\n"
        "ETF Name: <value or Unknown>\n"
        "Strategy: <exactly 2 sentences>\n"
        + "Do not include SEC.gov navigation text.\n"
        + "Do NOT use generic names like 'The Fund' unless no specific name exists.\n"
        + "Do not include any extra lines.\n\n"
        f"Extracted hints:\n"
        f"- Filer hint: {fields['filer_name']}\n"
        f"- ETF Name hint: {fields['etf_name']}\n"
        f"- Strategy hint: {fields['strategy']}\n"
        + "\nChunk summaries:\n"
        + "\n".join(f"- {item}" for item in chunk_summaries)
    )
    summary = _call_openarena(base_url, bearer_token, workflow_id, final_prompt, timeout_seconds)
    if _is_low_quality_summary(summary):
        retry_prompt = final_prompt + "\n\nRetry with cleaner output and no boilerplate text."
        summary = _call_openarena(base_url, bearer_token, workflow_id, retry_prompt, timeout_seconds)
    return _normalize_summary(summary, is_crypto, hints=fields)


async def generate_synopsis(
    filing_text: str,
    is_crypto: bool,
    settings: Settings,
    filer_name: str = "Unknown",
) -> str:
    if not filing_text.strip():
        return _fallback_summary(filing_text, is_crypto, filer_name_hint=filer_name)

    if not settings.openarena_bearer_token or not settings.openarena_workflow_id:
        return _fallback_summary(filing_text, is_crypto, filer_name_hint=filer_name)

    try:
        summary = await asyncio.to_thread(
            _synthesize_with_openarena,
            settings.openarena_base_url,
            settings.openarena_bearer_token,
            settings.openarena_workflow_id,
            settings.openarena_timeout_seconds,
            filing_text,
            is_crypto,
            filer_name,
        )
        if _is_low_quality_summary(summary):
            return _fallback_summary(filing_text, is_crypto, filer_name_hint=filer_name)
        return summary
    except Exception:
        return _fallback_summary(filing_text, is_crypto, filer_name_hint=filer_name)
