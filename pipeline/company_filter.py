"""
Stage 1.5 — LLM-based company ICP filter.

Reads companies from the database that haven't been filtered yet
(no prospects and not marked as rejected), asks Claude whether each
company matches the ICP, and deletes non-matches.

ICP: European tech company, Series A or B funding, 20-500 employees.

Usage:
  python -m pipeline.company_filter
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
You are a company ICP (Ideal Customer Profile) filter for Avery, an AI recruiting tool.

Your task: given a batch of companies, decide which ones match the target profile.

=== TARGET PROFILE ===
- European tech/software company (HQ or main operations in Europe)
- Funded: Series A, Series B, or equivalent stage (20-500 employees)
- Industry: SaaS, AI/ML, fintech, healthtech, deeptech, or other technology
- Must be an operating company (NOT a VC fund, agency, accelerator, media outlet, sports org, school, or job board)

=== EXCLUDE ===
- Companies outside Europe (US, Asia, etc.)
- Pre-seed / bootstrapped companies with <15 employees
- Late-stage (Series C+) or public companies with 1000+ employees
- Non-tech: consulting firms, marketing agencies, staffing agencies, media, sports, education
- Investment funds, VCs, accelerators, incubators
- Job boards, recruitment marketplaces
- Companies whose product directly competes with AI recruiting tools

If you are unsure about a company, INCLUDE it (let later stages filter).

Respond with ONLY a JSON array of indices (0-based) of companies that match. No explanation.
Example: [0, 2, 5]
If none match: []"""


def _get_unfiltered_companies(conn) -> list[dict[str, Any]]:
    rows = conn.execute("""
        SELECT c.id, c.name, c.domain, c.description, c.hq_city, c.hq_country,
               c.employees, c.latest_round, c.funding_total
        FROM companies c
        WHERE NOT EXISTS (SELECT 1 FROM prospects p WHERE p.company_id = c.id)
        ORDER BY c.name
    """).fetchall()
    return [dict(r) for r in rows]


def filter_companies(
    client: anthropic.AnthropicBedrock,
    companies: list[dict[str, Any]],
    batch_size: int = 30,
) -> list[int]:
    keep_ids: list[int] = []

    for start in range(0, len(companies), batch_size):
        batch = companies[start:start + batch_size]
        lines = []
        for i, c in enumerate(batch):
            parts = [c["name"]]
            if c.get("domain"):
                parts.append(f"({c['domain']})")
            details = []
            if c.get("hq_country"):
                details.append(f"HQ: {c.get('hq_city', '')} {c['hq_country']}")
            if c.get("employees"):
                details.append(f"{c['employees']} employees")
            if c.get("latest_round"):
                details.append(c["latest_round"])
            if c.get("description"):
                details.append(c["description"][:150])
            if details:
                parts.append("— " + ", ".join(details))
            lines.append(f"{i}. {' '.join(parts)}")

        prompt = "\n".join(lines)
        indices = None
        for attempt in range(2):
            response = client.messages.create(
                model=MODEL,
                max_tokens=1024,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )
            text = response.content[0].text.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
            try:
                indices = json.loads(text)
                break
            except json.JSONDecodeError:
                if attempt == 0:
                    print(f"  retry...", end=" ", flush=True)

        if indices is None:
            print(f"  ⚠ failed to parse LLM response: {text[:200]}")
            indices = list(range(len(batch)))

        if not isinstance(indices, list):
            indices = list(range(len(batch)))

        for i in indices:
            if isinstance(i, int) and 0 <= i < len(batch):
                keep_ids.append(batch[i]["id"])

    return keep_ids


def run_company_filter() -> int:
    conn = get_connection()
    init_db(conn)

    companies = _get_unfiltered_companies(conn)
    if not companies:
        print("[Stage 1.5] No unfiltered companies found.")
        conn.close()
        return 0

    print(f"[Stage 1.5 / Company Filter] {len(companies)} companies to evaluate\n")

    client = anthropic.AnthropicBedrock(aws_region=AWS_REGION)
    keep_ids = set(filter_companies(client, companies))

    kept = 0
    removed = 0
    remove_ids = []

    for c in companies:
        emp = c.get("employees") or 0
        if c["id"] in keep_ids and emp <= 100:
            print(f"  ✓ {c['name']} ({c.get('domain', '')}) — {emp} emp")
            kept += 1
        else:
            reason = f">{emp} emp" if c["id"] in keep_ids else "ICP mismatch"
            print(f"  ✗ {c['name']} ({c.get('domain', '')}) — {reason}")
            remove_ids.append(c["id"])
            removed += 1

    if remove_ids:
        placeholders = ",".join("?" * len(remove_ids))
        conn.execute(f"DELETE FROM companies WHERE id IN ({placeholders})", remove_ids)
        conn.commit()

    conn.close()
    print(f"\nDone: {kept} companies kept, {removed} removed")
    return kept


def main() -> None:
    argparse.ArgumentParser(description="LLM-based company ICP filter (stage 1.5)").parse_args()
    run_company_filter()


if __name__ == "__main__":
    main()
