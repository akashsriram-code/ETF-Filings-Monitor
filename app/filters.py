TARGET_FORMS = {"485APOS", "485BPOS", "S-1"}


def normalize_form_type(form_type: str) -> str:
    return "".join(form_type.upper().split())


def evaluate_filing_gate(
    form_type: str,
    raw_text: str,
    crypto_keywords: list[str],
) -> tuple[bool, list[str], bool]:
    normalized_form = normalize_form_type(form_type)
    if normalized_form not in TARGET_FORMS:
        return False, [], False

    if normalized_form in {"485APOS", "485BPOS"}:
        return True, [], False

    searchable_text = raw_text[:10_000].lower()
    matched_keywords = [keyword for keyword in crypto_keywords if keyword.lower() in searchable_text]
    return bool(matched_keywords), matched_keywords, True
