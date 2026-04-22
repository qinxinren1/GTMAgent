"""
Stage 4 — Email enrichment via Surfe

Reads stage3 filtered people and uses the Surfe API to find their
email addresses from LinkedIn URLs.

Usage:
  python -m pipeline.people_email
  python -m pipeline.people_email --input output/stage3_filtered_people.json
"""

from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any

import httpx
from dotenv import load_dotenv

load_dotenv()

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_OUTPUT_BASE = os.environ.get("AVERY_OUTPUT_DIR", os.path.join(_REPO_ROOT, "output"))

STAGE3_JSON = os.path.join(_OUTPUT_BASE, "stage3_filtered_people.json")
OUTPUT_JSON = os.path.join(_OUTPUT_BASE, "stage4_people_with_emails.json")

SURFE_API_KEY = os.environ.get("SURFE_API_KEY", "")
SURFE_BASE = "https://api.surfe.com/v2"

POLL_INTERVAL = 2
POLL_TIMEOUT = 120


def load_stage3(path: str = STAGE3_JSON) -> list[dict[str, Any]]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def enrich_emails(people: list[dict[str, Any]]) -> dict[str, list[dict[str, str]]]:
    """Submit all people to Surfe, poll for results.
    Returns {externalID: [{"email": ..., "validationStatus": ...}]}
    """
    if not people:
        return {}

    batch = []
    for p in people:
        linkedin = p.get("linkedin") or p.get("linkedInUrl") or ""
        if not linkedin:
            continue
        batch.append({
            "linkedinUrl": linkedin,
            "externalID": p.get("compositeId", p.get("name", "")),
        })

    if not batch:
        return {}

    headers = {
        "Authorization": f"Bearer {SURFE_API_KEY}",
        "Content-Type": "application/json",
    }

    with httpx.Client(timeout=30) as client:
        resp = client.post(
            f"{SURFE_BASE}/people/enrich",
            headers=headers,
            json={"include": {"email": True}, "people": batch},
        )
        resp.raise_for_status()
        data = resp.json()
        enrichment_id = data["enrichmentID"]
        print(f"  Surfe enrichment started: {enrichment_id}")

        elapsed = 0
        while elapsed < POLL_TIMEOUT:
            time.sleep(POLL_INTERVAL)
            elapsed += POLL_INTERVAL

            poll = client.get(
                f"{SURFE_BASE}/people/enrich/{enrichment_id}",
                headers=headers,
            )
            poll.raise_for_status()
            result = poll.json()

            status = result.get("status", "")
            pct = result.get("percentCompleted", 0)
            if status == "COMPLETED":
                print(f"  Enrichment completed ({pct}%)")
                break
            print(f"  Polling... {pct}% ({status})")
        else:
            print(f"  ⚠ Enrichment timed out after {POLL_TIMEOUT}s")
            result = poll.json()

    email_map: dict[str, list[dict[str, str]]] = {}
    for person in result.get("people", []):
        ext_id = person.get("externalID", "")
        emails = person.get("emails", [])
        if ext_id:
            email_map[ext_id] = emails

    return email_map


def run_email_enrichment(
    input_path: str = STAGE3_JSON,
    output_path: str = OUTPUT_JSON,
) -> list[dict[str, Any]]:
    stage3 = load_stage3(input_path)

    all_people = []
    people_company_map: dict[str, str] = {}
    for entry in stage3:
        for p in entry["people"]:
            all_people.append(p)
            ext_id = p.get("compositeId", p.get("name", ""))
            people_company_map[ext_id] = entry["company"]

    print(f"[Stage 4 / Surfe] {len(all_people)} people to enrich")

    email_map = enrich_emails(all_people)

    found = 0
    for p in all_people:
        ext_id = p.get("compositeId", p.get("name", ""))
        emails = email_map.get(ext_id, [])
        p["emails"] = emails
        if emails:
            found += 1

    out: list[dict[str, Any]] = []
    for entry in stage3:
        company_people = []
        for p in entry["people"]:
            ext_id = p.get("compositeId", p.get("name", ""))
            emails = email_map.get(ext_id, [])
            p["emails"] = emails
            company_people.append(p)

        out.append({
            "company": entry["company"],
            "domain": entry.get("domain", ""),
            "people": company_people,
        })

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    print(f"\nResults:")
    for entry in out:
        for p in entry["people"]:
            emails = p.get("emails", [])
            email_str = emails[0]["email"] if emails else "—"
            role = (p.get("currentRole") or {}).get("role", "?")
            print(f"  [{entry['company']}] {p.get('name', '?')} ({role}): {email_str}")

    print(f"\nDone: {found}/{len(all_people)} emails found → {output_path}")
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="Email enrichment via Surfe (stage 4)")
    p.add_argument("--input", "-i", default=STAGE3_JSON)
    p.add_argument("--output", "-o", default=OUTPUT_JSON)
    args = p.parse_args()
    run_email_enrichment(input_path=args.input, output_path=args.output)


if __name__ == "__main__":
    main()
