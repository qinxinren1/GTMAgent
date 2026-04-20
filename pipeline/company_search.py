"""
Company discovery via Exa (company category + highlights).

Requires: EXA_API_KEY in the environment (or a .env file loaded by callers).

Usage:
  export EXA_API_KEY=...
  python -m pipeline.company_search
  python -m pipeline.company_search -q "your query" -l NL

JSON path: see OUTPUT_JSON below.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict
from typing import Any

from dotenv import load_dotenv
from exa_py import Exa
from exa_py.api import SearchResponse

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_OUTPUT_BASE = os.environ.get("AVERY_OUTPUT_DIR", os.path.join(_REPO_ROOT, "output"))
OUTPUT_JSON = os.path.join(_OUTPUT_BASE, "stage1_company_results.json")

DEFAULT_QUERY = (
    "startup in Europe with series A / series B funding and 50 to 200 employees, who are actively hiring 2025 2026"
)

load_dotenv()

EXA_API_KEY = os.environ.get("EXA_API_KEY", "")


def search_companies(
    query: str | None = None,
    *,
    location: str | None = None,
    num_results: int = 10,
    category: str = "company",
    search_type: str = "auto",
    highlights: bool = True,
) -> SearchResponse[Any]:
    client = Exa(EXA_API_KEY)
    q = (query or DEFAULT_QUERY).strip()
    kwargs: dict[str, Any] = {
        "category": category,
        "num_results": num_results,
        "type": search_type,
        "contents": {"highlights": highlights},
    }
    code = str(location).strip().upper()
    kwargs["user_location"] = code

    return client.search(q, **kwargs)


def response_to_json(response: SearchResponse[Any]) -> dict[str, Any]:
    """Convert ``SearchResponse`` to JSON-serializable dict (``dataclasses.asdict``)."""
    return asdict(response)


def main() -> None:
    p = argparse.ArgumentParser(description="Exa company search (Avery prospecting)")
    p.add_argument(
        "--query",
        "-q",
        default=DEFAULT_QUERY,
        help="Search query string",
    )
    p.add_argument(
        "--location",
        "-l",
        default=None,
        metavar="CC",
        help="Single ISO country code (Europe), e.g. NL, DE. Optional. See --list-locations.",
    )
    args = p.parse_args()


    try:
        result = search_companies(args.query, location=args.location)
    except ValueError as e:
        print(e, file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Exa search failed: {e}", file=sys.stderr)
        sys.exit(2)

    os.makedirs(os.path.dirname(OUTPUT_JSON) or ".", exist_ok=True)
    payload = response_to_json(result)
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    print(f"Wrote {OUTPUT_JSON}", file=sys.stderr)


if __name__ == "__main__":
    main()
