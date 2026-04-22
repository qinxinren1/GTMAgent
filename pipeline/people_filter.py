"""
Stage 3 — LLM-based ICP filtering

Reads stage2 people output and uses Claude to filter for ideal customer
profile matches: founders, Head of People/HR, Head of TA, recruiters.
The LLM considers company stage (Series A vs B) when judging relevance.

Usage:
  python -m pipeline.people_filter
  python -m pipeline.people_filter --input output/stage2_people_output.json
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Any

import anthropic
from dotenv import load_dotenv

load_dotenv()

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_OUTPUT_BASE = os.environ.get("AVERY_OUTPUT_DIR", os.path.join(_REPO_ROOT, "output"))

STAGE2_JSON = os.path.join(_OUTPUT_BASE, "stage2_people_output.json")
OUTPUT_JSON = os.path.join(_OUTPUT_BASE, "stage3_filtered_people.json")

STAGE1_JSON = os.path.join(_OUTPUT_BASE, "stage1_company_results.json")

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


def load_stage2(path: str = STAGE2_JSON) -> list[dict[str, Any]]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_company_meta() -> dict[str, dict[str, Any]]:
    """Load stage1 to get employee count and funding round per company."""
    if not os.path.exists(STAGE1_JSON):
        return {}
    with open(STAGE1_JSON, encoding="utf-8") as f:
        data = json.load(f)
    meta: dict[str, dict[str, Any]] = {}
    for r in data.get("results", []):
        entities = r.get("entities") or []
        if not entities:
            continue
        props = entities[0].get("properties", {})
        name = props.get("name", "")
        meta[name] = {
            "employees": (props.get("workforce") or {}).get("total"),
            "latest_round": (props.get("financials") or {}).get("funding_latest_round", {}).get("name"),
        }
    return meta


def build_user_prompt(
    company_name: str,
    people: list[dict[str, Any]],
    company_info: dict[str, Any],
) -> str:
    employees = company_info.get("employees", "unknown")
    funding = company_info.get("latest_round", "unknown")

    lines = [f"Company: {company_name} ({employees} employees, {funding})"]
    lines.append("")
    for i, p in enumerate(people):
        role = (p.get("currentRole") or {}).get("role", "unknown")
        lines.append(f"{i}. {p.get('name', '?')} — {role}")

    return "\n".join(lines)


def filter_company(
    client: anthropic.AnthropicBedrock,
    company_name: str,
    people: list[dict[str, Any]],
    company_info: dict[str, Any],
) -> list[dict[str, Any]]:
    if not people:
        return []

    user_prompt = build_user_prompt(company_name, people, company_info)

    response = client.messages.create(
        model=MODEL,
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    )

    text = response.content[0].text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
    try:
        indices = json.loads(text)
    except json.JSONDecodeError:
        print(f"    ⚠ failed to parse LLM response for {company_name}: {text[:200]}")
        return []

    if not isinstance(indices, list):
        return []

    selected = []
    for idx in indices:
        if isinstance(idx, int) and 0 <= idx < len(people):
            selected.append(people[idx])
    return selected


def run_filter(
    input_path: str = STAGE2_JSON,
    output_path: str = OUTPUT_JSON,
) -> list[dict[str, Any]]:
    stage2 = load_stage2(input_path)
    company_meta = load_company_meta()
    client = anthropic.AnthropicBedrock(aws_region=AWS_REGION)

    print(f"[Stage 3 / LLM Filter] {len(stage2)} companies, model={MODEL}")

    out: list[dict[str, Any]] = []

    for entry in stage2:
        company_name = entry["company"]
        people = entry["people"]
        info = company_meta.get(company_name, {})

        selected = filter_company(client, company_name, people, info)
        print(f"  {company_name}: {len(people)} → {len(selected)} matches")
        for p in selected:
            role = (p.get("currentRole") or {}).get("role", "?")
            print(f"    ✓ {p.get('name', '?')} — {role}")

        out.append({
            "company": company_name,
            "domain": entry.get("domain", ""),
            "total_before_filter": len(people),
            "people": selected,
        })

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    n_people = sum(len(x["people"]) for x in out)
    print(f"\nDone: {len(out)} companies, {n_people} ICP matches → {output_path}")
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="LLM-based ICP filter (stage 3)")
    p.add_argument("--input", "-i", default=STAGE2_JSON)
    p.add_argument("--output", "-o", default=OUTPUT_JSON)
    args = p.parse_args()
    run_filter(input_path=args.input, output_path=args.output)


if __name__ == "__main__":
    main()
