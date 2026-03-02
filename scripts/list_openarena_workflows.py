from __future__ import annotations

import argparse
import json
import os
from typing import Any

import httpx


DEFAULT_BASE_URL = "https://aiopenarena.gcs.int.thomsonreuters.com"


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def fetch_workflow_page(
    client: httpx.Client,
    base_url: str,
    next_token: str | None,
    page_size: int,
    only_accessible: bool,
) -> dict[str, Any]:
    params: dict[str, Any] = {"page_size": page_size}
    if only_accessible:
        params["is_accessible_by_me"] = "true"
    if next_token:
        params["next_token"] = next_token

    response = client.get(f"{base_url}/v2/workflow_trimmed", params=params)
    response.raise_for_status()
    payload = response.json()

    if isinstance(payload, list):
        return {"items": payload, "pagination": {}}
    if isinstance(payload, dict):
        return payload
    return {"items": [], "pagination": {}}


def score_workflow(item: dict[str, Any], keywords: list[str]) -> int:
    blob = " ".join(
        [
            normalize_text(item.get("title")),
            normalize_text(item.get("description")),
            " ".join(str(x) for x in (item.get("tags") or [])),
        ]
    ).lower()
    score = 0
    for kw in keywords:
        if kw in blob:
            score += 1
    return score


def main() -> int:
    parser = argparse.ArgumentParser(description="List accessible Thomson Reuters OpenArena workflows.")
    parser.add_argument("--base-url", default=os.getenv("OPENARENA_BASE_URL", DEFAULT_BASE_URL).strip())
    parser.add_argument("--token", default=os.getenv("OPENARENA_BEARER_TOKEN", "").strip())
    parser.add_argument("--page-size", type=int, default=50)
    parser.add_argument("--max-pages", type=int, default=5)
    parser.add_argument("--title-contains", action="append", default=[])
    parser.add_argument("--only-accessible", action="store_true", default=True)
    parser.add_argument("--json", action="store_true", help="Print full result JSON.")
    parser.add_argument(
        "--pick-keywords",
        default="etf,filing",
        help="Comma-separated keywords to score and auto-pick best workflow.",
    )
    args = parser.parse_args()

    token = args.token.strip()
    if not token:
        raise RuntimeError("Missing token. Set OPENARENA_BEARER_TOKEN or pass --token.")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    all_items: list[dict[str, Any]] = []
    next_token: str | None = None
    with httpx.Client(headers=headers, timeout=30, follow_redirects=True) as client:
        for _ in range(max(args.max_pages, 1)):
            page = fetch_workflow_page(
                client=client,
                base_url=args.base_url.rstrip("/"),
                next_token=next_token,
                page_size=max(args.page_size, 1),
                only_accessible=bool(args.only_accessible),
            )
            items = page.get("items") or []
            all_items.extend(item for item in items if isinstance(item, dict))

            pagination = page.get("pagination") or {}
            next_token = normalize_text(pagination.get("next_token")) or None
            if not next_token:
                break

    filtered = all_items
    for needle in [x.strip().lower() for x in args.title_contains if x.strip()]:
        filtered = [w for w in filtered if needle in normalize_text(w.get("title")).lower()]

    if args.json:
        print(json.dumps(filtered, indent=2))
    else:
        print(f"Found {len(filtered)} workflow(s).")
        for item in filtered:
            print(
                f"- {normalize_text(item.get('workflow_id'))} | "
                f"{normalize_text(item.get('title'))} | "
                f"status={normalize_text(item.get('workflow_status'))} | "
                f"public={normalize_text(item.get('is_public'))}"
            )

    keywords = [x.strip().lower() for x in args.pick_keywords.split(",") if x.strip()]
    if filtered and keywords:
        ranked = sorted(filtered, key=lambda w: score_workflow(w, keywords), reverse=True)
        best = ranked[0]
        print("\nSuggested workflow:")
        print(f"OPENARENA_WORKFLOW_ID={normalize_text(best.get('workflow_id'))}")
        print(f"title={normalize_text(best.get('title'))}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
