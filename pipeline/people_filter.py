"""
Stage 3 — LLM-based ICP filtering.

Reads prospects from the database that haven't been filtered yet,
uses Claude to decide which ones match the ICP, and removes non-matches.

Usage:
  python -m pipeline.people_filter
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Any

import anthropic
from dotenv import load_dotenv

from pipeline.db import get_connection, init_db

load_dotenv()

AWS_REGION = os.environ.get("AWS_REGION", "eu-central-1")
MODEL = os.environ.get("ANTHROPIC_MODEL", "eu.anthropic.claude-opus-4-6-v1")

SYSTEM_PROMPT = """\
You are an ICP (Ideal Customer Profile) filter for a recruiting/HR tech product.

Your task: given a list of people at a company, select ONLY those who match the target persona:
- Founders / Co-founders / CEO
- Head of People / HR / Chief People Officer / VP People
- Head of Talent Acquisition / TA Lead
- Recruiters / Talent Partners / Hiring Managers
- HR Generalists / People Operations (at smaller companies)

Context that matters:
- At early-stage companies (Series A, <70 employees), founders often handle hiring directly — include them.
- At later-stage companies (Series B+, 70+ employees), founders are less relevant unless their title suggests direct involvement in hiring/people.
- Always include anyone whose role clearly relates to people, HR, talent, or recruiting regardless of company stage.

Respond with ONLY a JSON array of indices (0-based) of people who match. No explanation.
Example: [0, 3, 7]
If nobody matches, respond with: []"""


def _get_unfiltered_prospects(conn) -> dict[str, list[dict[str, Any]]]:
    rows = conn.execute("""
        SELECT p.id, p.name, p.role, p.company_id,
               c.name AS company_name, c.employees, c.latest_round
        FROM prospects p
        JOIN companies c ON p.company_id = c.id
        LEFT JOIN messages m ON p.id = m.prospect_id
        WHERE m.id IS NULL
        ORDER BY c.name, p.name
    """).fetchall()

    by_company: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        company = r["company_name"]
        by_company.setdefault(company, []).append(dict(r))
    return by_company


def filter_company(
    client: anthropic.AnthropicBedrock,
    company_name: str,
    prospects: list[dict[str, Any]],
) -> list[int]:
    if not prospects:
        return []

    employees = prospects[0].get("employees", "unknown")
    funding = prospects[0].get("latest_round", "unknown")

    lines = [f"Company: {company_name} ({employees} employees, {funding})", ""]
    for i, p in enumerate(prospects):
        lines.append(f"{i}. {p['name']} — {p.get('role', 'unknown')}")

    response = client.messages.create(
        model=MODEL,
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": "\n".join(lines)}],
    )

    text = response.content[0].text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
    try:
        indices = json.loads(text)
    except json.JSONDecodeError:
        print(f"    ⚠ failed to parse LLM response for {company_name}: {text[:200]}")
        return list(range(len(prospects)))

    if not isinstance(indices, list):
        return list(range(len(prospects)))

    return [i for i in indices if isinstance(i, int) and 0 <= i < len(prospects)]


def run_filter() -> int:
    conn = get_connection()
    init_db(conn)

    by_company = _get_unfiltered_prospects(conn)
    if not by_company:
        print("[Stage 3] No unfiltered prospects found.")
        conn.close()
        return 0

    total_prospects = sum(len(v) for v in by_company.values())
    print(f"[Stage 3 / LLM Filter] {total_prospects} prospects across {len(by_company)} companies\n")

    client = anthropic.AnthropicBedrock(aws_region=AWS_REGION)
    total_kept = 0
    total_removed = 0

    for company_name, prospects in by_company.items():
        keep_indices = filter_company(client, company_name, prospects)
        keep_ids = {prospects[i]["id"] for i in keep_indices}
        remove_ids = [p["id"] for p in prospects if p["id"] not in keep_ids]

        for p in prospects:
            if p["id"] in keep_ids:
                print(f"  ✓ {p['name']} — {p.get('role', '?')}")

        if remove_ids:
            placeholders = ",".join("?" * len(remove_ids))
            conn.execute(f"DELETE FROM prospects WHERE id IN ({placeholders})", remove_ids)
            conn.commit()

        kept = len(keep_ids)
        removed = len(remove_ids)
        total_kept += kept
        total_removed += removed
        print(f"  {company_name}: {len(prospects)} → {kept} kept, {removed} removed\n")

    conn.close()
    print(f"Done: {total_kept} ICP matches kept, {total_removed} removed")
    return total_kept


def main() -> None:
    argparse.ArgumentParser(description="LLM-based ICP filter (stage 3)").parse_args()
    run_filter()


if __name__ == "__main__":
    main()
