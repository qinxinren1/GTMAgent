"""
Stage 2 — Avery people search by company name

For each company from stage1, calls the avery-search MCP server's
search_people tool to find people matching target job titles.

Usage:
  python -m pipeline.people_search
  python -m pipeline.people_search --titles "CEO" "CTO" "Head of HR" --limit 25
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

load_dotenv()

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_OUTPUT_BASE = os.environ.get("AVERY_OUTPUT_DIR", os.path.join(_REPO_ROOT, "output"))

STAGE1_JSON = os.path.join(_OUTPUT_BASE, "stage1_company_results.json")
OUTPUT_JSON = os.path.join(_OUTPUT_BASE, "stage2_people_output.json")

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
    r"\bintern\b",
    r"\binternship\b",
    r"\bstudent\b",
    r"\btrainee\b",
    r"\bpraktikant\b",
    r"\bwerkstudent(?:in)?\b",
    r"\bworking\s+student\b",
    r"\balternant(?:e)?\b",
    r"\bstagiaire\b",
    r"\bapprentice?\b",
]

_EXCLUDED_RE = re.compile("|".join(EXCLUDED_ROLE_PATTERNS), re.IGNORECASE)


def _is_excluded(person: dict[str, Any]) -> bool:
    role = (person.get("currentRole") or {}).get("role", "")
    return bool(_EXCLUDED_RE.search(role))


def _belongs_to_company(person: dict[str, Any], linkedin_id: str) -> bool:
    if not linkedin_id:
        return True
    person_company_id = (person.get("currentRole") or {}).get("companyLinkedinId")
    return person_company_id == linkedin_id


def load_companies(path: str = STAGE1_JSON) -> list[dict[str, Any]]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return data.get("results", []) if isinstance(data, dict) else data


def extract_company_meta(raw: dict[str, Any]) -> dict[str, str]:
    """Pull (name, domain) from an Exa stage1 result item."""
    url = raw.get("url", "")
    domain = url.replace("https://", "").replace("http://", "").rstrip("/")

    entities = raw.get("entities") or []
    if entities:
        name = entities[0].get("properties", {}).get("name") or raw.get("title", domain)
    else:
        name = raw.get("title", domain)

    return {"name": name, "domain": domain}



async def resolve_linkedin_id(
    session: ClientSession,
    name: str,
    domain: str,
) -> str:
    """Call search_companies with the domain (or name) to get the LinkedIn ID."""
    query = domain or name
    try:
        result = await session.call_tool(
            "search_companies", arguments={"query": query, "limit": 3}
        )
        raw = json.loads(result.content[0].text if result.content else "{}")
        companies = raw.get("companies", [])
        if companies:
            return companies[0].get("linkedInId", "")
    except Exception as exc:
        print(f"    ⚠ resolve linkedInId for {name}: {exc}")
    return ""


async def _fetch_page(
    session: ClientSession,
    company_name: str,
    linkedin_id: str = "",
    page: int = 1,
) -> tuple[list[dict[str, Any]], int, bool]:
    """Fetch one page (up to 100) of people. Returns (people, total, has_more)."""
    arguments: dict[str, Any] = {
        "onlyCurrentExperience": True,
        "limit": 100,
        "page": page,
        "view": "summary",
    }
    if linkedin_id:
        arguments["companies"] = [{"name": company_name, "linkedInId": linkedin_id}]
    else:
        arguments["companyKeywords"] = [company_name]
        arguments["exactCompanyMatch"] = True

    result = await session.call_tool("search_people", arguments=arguments)

    raw_text = result.content[0].text if result.content else "{}"
    if os.environ.get("DEBUG"):
        print(f"    [debug] page {page}: {raw_text[:300]}")
    data = json.loads(raw_text)

    people = data.get("people", [])
    pagination = data.get("pagination", {})
    total = pagination.get("totalResults", len(people))
    has_more = pagination.get("hasMore", False)
    return people, total, has_more


async def search_all_people(
    session: ClientSession,
    company_name: str,
    linkedin_id: str = "",
) -> tuple[list[dict[str, Any]], int]:
    """Fetch ALL people at a company by auto-paginating. Returns (all_people, total)."""
    all_people: list[dict[str, Any]] = []
    page = 1

    while True:
        people, total, has_more = await _fetch_page(session, company_name, linkedin_id, page)
        all_people.extend(people)
        if not has_more or not people:
            break
        page += 1

    return all_people, total


async def run_people_async(
    input_path: str = STAGE1_JSON,
    output_path: str = OUTPUT_JSON,
    job_titles: list[str] | None = None,
) -> list[dict[str, Any]]:
    companies = load_companies(input_path)
    effective_titles = job_titles or DEFAULT_TITLES

    print(f"[Stage 2 / Avery] {len(companies)} companies")
    print(f"  Job titles: {effective_titles}\n")

    out: list[dict[str, Any]] = []

    async with stdio_client(MCP_SERVER_PARAMS) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            for raw in companies:
                meta = extract_company_meta(raw)
                name, domain = meta["name"], meta["domain"]

                li_id = await resolve_linkedin_id(session, name, domain)
                if li_id:
                    print(f"  {name} ({domain}): linkedInId={li_id}")
                else:
                    print(f"  {name} ({domain}): no linkedInId, falling back to companyKeywords")

                try:
                    people, total = await search_all_people(session, name, li_id)
                    print(f"    fetched {len(people)} people (total {total})")
                except Exception as exc:
                    print(f"  ⚠ {name}: {exc}")
                    people, total = [], 0

                matched = [p for p in people if _belongs_to_company(p, li_id)]
                if len(matched) < len(people):
                    print(f"    company filter: {len(people)} → {len(matched)} (removed {len(people) - len(matched)} wrong company)")
                filtered = [p for p in matched if not _is_excluded(p)]
                if len(filtered) < len(matched):
                    print(f"    role filter: excluded {len(matched) - len(filtered)} (intern/student/trainee)")

                out.append({
                    "company": name,
                    "domain": domain,
                    "total_in_avery": total,
                    "people": filtered,
                })

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    n_people = sum(len(x["people"]) for x in out)
    print(f"\nDone: {len(out)} companies, {n_people} people → {output_path}")
    return out


def run_people(
    input_path: str = STAGE1_JSON,
    output_path: str = OUTPUT_JSON,
    job_titles: list[str] | None = None,
) -> list[dict[str, Any]]:
    return asyncio.run(run_people_async(input_path, output_path, job_titles))


def main() -> None:
    p = argparse.ArgumentParser(description="Avery people search (stage 2)")
    p.add_argument("--input", "-i", default=STAGE1_JSON)
    p.add_argument("--output", "-o", default=OUTPUT_JSON)
    p.add_argument("--titles", "-t", nargs="+", default=None, metavar="TITLE")
    args = p.parse_args()

    run_people(
        input_path=args.input,
        output_path=args.output,
        job_titles=args.titles,
    )


if __name__ == "__main__":
    main()
