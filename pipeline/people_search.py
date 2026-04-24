"""
Stage 2 — People search via Avery MCP server.

Reads companies from the database (only those without prospects yet),
searches for people matching target job titles, and writes results to DB.

Usage:
  python -m pipeline.people_search
  python -m pipeline.people_search --titles "CEO" "CTO" "Head of HR"
  python -m pipeline.people_search --all   # re-search all companies
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
from typing import Any

from dotenv import load_dotenv
from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

from pipeline.db import (
    get_companies,
    get_companies_without_prospects,
    get_connection,
    init_db,
    upsert_prospect,
)

load_dotenv()

AVERY_MCP_URL = os.environ.get(
    "AVERY_MCP_URL",
    "https://search-mcp-production-a02b.up.railway.app/mcp",
)
AVERY_MCP_AUTH_TOKEN = os.environ.get("AVERY_MCP_AUTH_TOKEN", "")

MCP_SERVER_PARAMS = StdioServerParameters(
    command="npx",
    args=[
        "-y", "mcp-remote",
        AVERY_MCP_URL,
        "--header", f"Authorization: Bearer {AVERY_MCP_AUTH_TOKEN}",
    ],
)

DEFAULT_TITLES = [
    "CEO",
    "CTO",
    "Head of HR",
    "Head of People",
    "VP People",
    "Chief People Officer",
]

EXCLUDED_ROLE_PATTERNS = [
    r"\bintern\b", r"\binternship\b", r"\bstudent\b", r"\btrainee\b",
    r"\bpraktikant\b", r"\bwerkstudent(?:in)?\b", r"\bworking\s+student\b",
    r"\balternant(?:e)?\b", r"\bstagiaire\b", r"\bapprentice?\b",
]
_EXCLUDED_RE = re.compile("|".join(EXCLUDED_ROLE_PATTERNS), re.IGNORECASE)


def _is_excluded(person: dict[str, Any]) -> bool:
    role = (person.get("currentRole") or {}).get("role") or ""
    return bool(_EXCLUDED_RE.search(role))


def _belongs_to_company(person: dict[str, Any], linkedin_id: str) -> bool:
    if not linkedin_id:
        return True
    return (person.get("currentRole") or {}).get("companyLinkedinId") == linkedin_id


async def resolve_linkedin_id(session: ClientSession, name: str, domain: str) -> str:
    query = domain or name
    try:
        result = await session.call_tool("search_companies", arguments={"query": query, "limit": 3})
        raw = json.loads(result.content[0].text if result.content else "{}")
        companies = raw.get("companies", [])
        if companies:
            return companies[0].get("linkedInId", "")
    except Exception as exc:
        print(f"    ⚠ resolve linkedInId for {name}: {exc}")
    return ""


async def _fetch_page(
    session: ClientSession, company_name: str, linkedin_id: str = "", page: int = 1,
) -> tuple[list[dict[str, Any]], int, bool]:
    arguments: dict[str, Any] = {
        "onlyCurrentExperience": True, "limit": 100, "page": page, "view": "summary",
    }
    if linkedin_id:
        arguments["companies"] = [{"name": company_name, "linkedInId": linkedin_id}]
    else:
        arguments["companyKeywords"] = [company_name]
        arguments["exactCompanyMatch"] = True

    result = await session.call_tool("search_people", arguments=arguments)
    data = json.loads(result.content[0].text if result.content else "{}")
    people = data.get("people", [])
    pagination = data.get("pagination", {})
    return people, pagination.get("totalResults", len(people)), pagination.get("hasMore", False)


MAX_PEOPLE_PER_COMPANY = 200

async def search_all_people(
    session: ClientSession, company_name: str, linkedin_id: str = "",
) -> tuple[list[dict[str, Any]], int]:
    all_people: list[dict[str, Any]] = []
    page = 1
    while True:
        people, total, has_more = await _fetch_page(session, company_name, linkedin_id, page)
        all_people.extend(people)
        if len(all_people) >= MAX_PEOPLE_PER_COMPANY:
            print(f"    ⚠ capped at {MAX_PEOPLE_PER_COMPANY} people (total {total})")
            all_people = all_people[:MAX_PEOPLE_PER_COMPANY]
            break
        if not has_more or not people:
            break
        page += 1
    return all_people, total


def _classify_prospect_type(person: dict[str, Any], company_name: str) -> str:
    role = ((person.get("currentRole") or {}).get("role") or "").lower()
    comp = ((person.get("currentRole") or {}).get("companyName") or "").lower()
    if comp and comp != company_name.lower():
        return "agency"
    if any(kw in role for kw in ["freelance", "fractional", "consultant", "interim"]):
        return "rpo"
    return "inhouse"


def save_people_to_db(
    company_id: int, company_name: str, people: list[dict[str, Any]],
) -> int:
    conn = get_connection()
    init_db(conn)
    saved = 0
    for p in people:
        ext_id = p.get("compositeId", p.get("name", ""))
        if not ext_id:
            continue
        loc = (p.get("location") or {}).get("current", {})
        role = (p.get("currentRole") or {}).get("role") or ""
        upsert_prospect(
            conn,
            company_id=company_id,
            external_id=ext_id,
            name=p.get("name", ""),
            first_name=p.get("firstName", ""),
            last_name=p.get("lastName", ""),
            role=role,
            prospect_type=_classify_prospect_type(p, company_name),
            city=loc.get("city", ""),
            country=loc.get("country", ""),
            linkedin_url=p.get("linkedin", ""),
            email=None,
            email_status=None,
            response="none",
            notes="",
        )
        saved += 1
    conn.close()
    return saved


async def run_people_async(search_all: bool = False) -> int:
    conn = get_connection()
    init_db(conn)
    if search_all:
        companies = get_companies(conn)
    else:
        companies = get_companies_without_prospects(conn)
    conn.close()

    if not companies:
        print("[Stage 2] No new companies to search.")
        return 0

    print(f"[Stage 2 / Avery] {len(companies)} companies to search\n")
    total_saved = 0

    async with stdio_client(MCP_SERVER_PARAMS) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            for c in companies:
                name, domain = c["name"], c.get("domain", "")
                li_id = await resolve_linkedin_id(session, name, domain)
                print(f"  {name} ({domain})" + (f" linkedInId={li_id}" if li_id else " (keyword fallback)"))

                try:
                    people, total = await search_all_people(session, name, li_id)
                    print(f"    fetched {len(people)} people (total {total})")

                    matched = [p for p in people if _belongs_to_company(p, li_id)]
                    filtered = [p for p in matched if not _is_excluded(p)]
                    if len(filtered) < len(people):
                        print(f"    filtered: {len(people)} → {len(filtered)}")

                    saved = save_people_to_db(c["id"], name, filtered)
                    total_saved += saved
                    print(f"    saved {saved} prospects to DB")
                except Exception as exc:
                    print(f"    ⚠ {name}: {exc}")
                    continue

    print(f"\nDone: {total_saved} prospects saved")
    return total_saved


def run_people(search_all: bool = False) -> int:
    return asyncio.run(run_people_async(search_all))


def main() -> None:
    p = argparse.ArgumentParser(description="Avery people search (stage 2)")
    p.add_argument("--all", action="store_true", help="Re-search all companies, not just new ones")
    p.add_argument("--titles", "-t", nargs="+", default=None, metavar="TITLE")
    args = p.parse_args()
    run_people(search_all=args.all)


if __name__ == "__main__":
    main()
