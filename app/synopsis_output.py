import re
from typing import Any

_ALERT_ORDER = {
    "LOW": 1,
    "MEDIUM": 2,
    "HIGH": 3,
}

_KNOWN_LEVELS = tuple(_ALERT_ORDER.keys())


def _clean_line(value: str) -> str:
    return " ".join((value or "").strip().split())


def normalize_alert_level(value: str) -> str:
    text = _clean_line(value).upper()
    if not text:
        return "UNKNOWN"
    if re.search(r"\bCRITICAL\b", text):
        return "HIGH"
    for level in _KNOWN_LEVELS:
        if re.search(rf"\b{level}\b", text):
            return level
    return "UNKNOWN"


def _extract_field(block: str, label: str) -> str:
    other_labels = r"(?:Filer|ETF Name|ETF\s+\d+|Strategy|IS ALERT WORTHY|Why this matters|Synopsis\s+\d+)"
    pattern = rf"(?ims)^\s*{label}\s*:\s*(.+?)(?=^\s*{other_labels}\s*:?\s*|\Z)"
    match = re.search(pattern, block)
    if not match:
        return ""
    return _clean_line(match.group(1))


def _split_synopsis_blocks(text: str) -> list[str]:
    matches = list(re.finditer(r"(?im)^\s*Synopsis\s+\d+\s*$", text))
    if not matches:
        return [text]

    blocks: list[str] = []
    for idx, match in enumerate(matches):
        start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        blocks.append(text[start:end].strip())
    return blocks


def _parse_numbered_etf_sections(text: str) -> list[dict[str, str]]:
    matches = list(re.finditer(r"(?im)^\s*ETF\s+\d+\s*:\s*(.+?)\s*$", text))
    if not matches:
        return []

    filer = _extract_field(text, "Filer") or "Unknown"
    items: list[dict[str, str]] = []
    for idx, match in enumerate(matches):
        etf_name = _clean_line(match.group(1))
        section_start = match.end()
        section_end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        section = text[section_start:section_end]

        strategy = _extract_field(section, "Strategy") or "Not available."
        alert_level = normalize_alert_level(_extract_field(section, "IS ALERT WORTHY"))
        if not etf_name:
            continue
        items.append(
            {
                "filer": filer,
                "etf_name": etf_name,
                "strategy": strategy,
                "is_alert_worthy": alert_level,
            }
        )
    return items


def _extract_why_this_matters(text: str, max_bullets: int = 3) -> list[str]:
    heading = re.search(r"(?ims)^\s*(?:Why this matters)\s*:?\s*$", text)
    search_text = text[heading.end() :] if heading else text
    bullets: list[str] = []
    for line in search_text.splitlines():
        match = re.match(r"^\s*[-*•]\s+(.+)$", line)
        if not match:
            continue
        cleaned = _clean_line(match.group(1))
        if cleaned:
            bullets.append(cleaned)
        if len(bullets) >= max_bullets:
            break
    return bullets


def _best_wire_level(items: list[dict[str, str]]) -> str:
    best = "UNKNOWN"
    for item in items:
        level = normalize_alert_level(item.get("is_alert_worthy", ""))
        if _ALERT_ORDER.get(level, 0) > _ALERT_ORDER.get(best, 0):
            best = level
    return best


def parse_synopsis_output(text: str) -> dict[str, Any]:
    raw = (text or "").strip()
    if not raw:
        return {"items": [], "why_this_matters": [], "wire_recommendation": "UNKNOWN"}

    items: list[dict[str, str]] = _parse_numbered_etf_sections(raw)
    if items:
        return {
            "items": items,
            "why_this_matters": _extract_why_this_matters(raw),
            "wire_recommendation": _best_wire_level(items),
        }

    for block in _split_synopsis_blocks(raw):
        filer = _extract_field(block, "Filer")
        etf_name = _extract_field(block, "ETF Name")
        strategy = _extract_field(block, "Strategy")
        alert_level = normalize_alert_level(_extract_field(block, "IS ALERT WORTHY"))
        if not (filer or etf_name or strategy):
            continue
        items.append(
            {
                "filer": filer or "Unknown",
                "etf_name": etf_name or "Unknown",
                "strategy": strategy or "Not available.",
                "is_alert_worthy": alert_level,
            }
        )

    return {
        "items": items,
        "why_this_matters": _extract_why_this_matters(raw),
        "wire_recommendation": _best_wire_level(items),
    }


def format_synopsis_output(items: list[dict[str, str]], why_this_matters: list[str]) -> str:
    if not items:
        return ""

    lines: list[str] = []
    for idx, item in enumerate(items, start=1):
        if idx > 1:
            lines.append("")
        lines.append(f"Synopsis {idx}")
        lines.append(f"Filer: {_clean_line(item.get('filer', 'Unknown')) or 'Unknown'}")
        lines.append(f"ETF Name: {_clean_line(item.get('etf_name', 'Unknown')) or 'Unknown'}")
        lines.append(f"Strategy: {_clean_line(item.get('strategy', 'Not available.')) or 'Not available.'}")
        lines.append(
            f"IS ALERT WORTHY: {normalize_alert_level(item.get('is_alert_worthy', 'UNKNOWN'))}"
        )

    if why_this_matters:
        lines.append("")
        lines.append("Why this matters:")
        for bullet in why_this_matters[:3]:
            cleaned = _clean_line(bullet)
            if cleaned:
                lines.append(f"- {cleaned}")

    return "\n".join(lines).strip()


def format_email_body(synopsis: str, sec_link: str) -> str:
    parsed = parse_synopsis_output(synopsis)
    lines: list[str] = []
    if parsed["wire_recommendation"] != "UNKNOWN":
        lines.append(f"Wire Recommendation: {parsed['wire_recommendation']}")
        lines.append("")
    lines.append(
        format_synopsis_output(
            parsed["items"],
            parsed["why_this_matters"],
        )
        or synopsis.strip()
    )
    lines.append("")
    lines.append(f"SEC Link: {sec_link}")
    return "\n".join(lines).strip()
