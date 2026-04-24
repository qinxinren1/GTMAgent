"""
Stage 1 — Company discovery via Exa.

Searches for companies matching a query, then writes them to the database.
Existing companies (by name) are updated, not duplicated.

Usage:
  python -m pipeline.company_search
  python -m pipeline.company_search -q "your query" -n 20
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Any

from dotenv import load_dotenv
from exa_py import Exa

from pipeline.db import get_connection, init_db, upsert_company

load_dotenv()

EXA_API_KEY = os.environ.get("EXA_API_KEY", "")

DEFAULT_QUERY = (
    "startup in Europe with series A / series B funding and 50 to 200 employees during 2025 2026"
)


def search_companies(
    query: str | None = None,
    *,
    location: str | None = None,
    num_results: int = 10,
) -> list[dict[str, Any]]:
    client = Exa(EXA_API_KEY)
    q = (query or DEFAULT_QUERY).strip()
    kwargs: dict[str, Any] = {
        "category": "company",
        "num_results": num_results,
        "type": "auto",
        "contents": {"highlights": True},
    }
    if location:
        kwargs["user_location"] = location.strip().upper()

    response = client.search(q, **kwargs)

    companies = []
    for r in response.results:
        entities = getattr(r, "entities", None) or []
        if not entities:
            continue
        ent = entities[0]
        props = ent.get("properties", {}) if isinstance(ent, dict) else getattr(ent, "properties", None)
        if props is None:
            continue

        def _g(obj, key, default=None):
            if isinstance(obj, dict):
                return obj.get(key, default)
            return getattr(obj, key, default)

        url = getattr(r, "url", "")
        domain = url.replace("https://", "").replace("http://", "").rstrip("/")
        workforce = _g(props, "workforce") or {}
        financials = _g(props, "financials") or {}
        funding_round = _g(financials, "funding_latest_round") or {}
        hq = _g(props, "headquarters") or {}

        companies.append({
            "name": _g(props, "name") or getattr(r, "title", domain),
            "domain": domain,
            "url": url,
            "description": _g(props, "description", ""),
            "founded_year": _g(props, "founding_date"),
            "hq_city": _g(hq, "city", ""),
            "hq_country": _g(hq, "country", ""),
            "employees": _g(workforce, "total"),
            "funding_total": _g(financials, "funding_total"),
            "latest_round": _g(funding_round, "name"),
            "latest_amount": _g(funding_round, "amount"),
        })

    return companies


def save_to_db(companies: list[dict[str, Any]]) -> int:
    conn = get_connection()
    init_db(conn)
    saved = 0
    for c in companies:
        upsert_company(conn, **c)
        saved += 1
        print(f"  ✓ {c['name']} ({c['domain']}) — {c.get('employees') or '?'} emp, {c.get('latest_round') or '?'}")
    conn.close()
    return saved


def run_company_search(
    query: str | None = None,
    location: str | None = None,
    num_results: int = 10,
) -> list[dict[str, Any]]:
    print(f"[Stage 1 / Exa] Searching for companies...")
    companies = search_companies(query, location=location, num_results=num_results)
    print(f"  Found {len(companies)} companies\n")
    saved = save_to_db(companies)
    print(f"\nDone: {saved} companies saved to database")
    return companies


def main() -> None:
    p = argparse.ArgumentParser(description="Exa company search (stage 1)")
    p.add_argument("--query", "-q", default=DEFAULT_QUERY)
    p.add_argument("--location", "-l", default=None, metavar="CC")
    p.add_argument("--num", "-n", type=int, default=10)
    args = p.parse_args()

    try:
        run_company_search(args.query, location=args.location, num_results=args.num)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
