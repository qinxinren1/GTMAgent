"""
Stage 4 — Email enrichment via Surfe.

Reads prospects from the database that have a LinkedIn URL but no email,
enriches them via Surfe API, and updates the database.

Usage:
  python -m pipeline.people_email
"""

from __future__ import annotations

import argparse
import os
import time
from typing import Any

import httpx
from dotenv import load_dotenv

from pipeline.db import get_connection, get_prospects_without_email, init_db, update_prospect

load_dotenv()

SURFE_API_KEY = os.environ.get("SURFE_API_KEY", "")
SURFE_BASE = "https://api.surfe.com/v2"
POLL_INTERVAL = 2
POLL_TIMEOUT = 120


def enrich_emails(prospects: list[dict[str, Any]]) -> dict[int, dict[str, str]]:
    if not prospects:
        return {}

    batch = []
    id_map: dict[str, int] = {}
    for p in prospects:
        linkedin = p.get("linkedin_url", "")
        if not linkedin:
            continue
        ext_id = p.get("external_id") or str(p["id"])
        batch.append({"linkedinUrl": linkedin, "externalID": ext_id})
        id_map[ext_id] = p["id"]

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
        enrichment_id = resp.json()["enrichmentID"]
        print(f"  Surfe enrichment started: {enrichment_id}")

        elapsed = 0
        result = {}
        while elapsed < POLL_TIMEOUT:
            time.sleep(POLL_INTERVAL)
            elapsed += POLL_INTERVAL
            poll = client.get(f"{SURFE_BASE}/people/enrich/{enrichment_id}", headers=headers)
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

    email_map: dict[int, dict[str, str]] = {}
    for person in result.get("people", []):
        ext_id = person.get("externalID", "")
        emails = person.get("emails", [])
        prospect_id = id_map.get(ext_id)
        if prospect_id and emails:
            best = emails[0]
            email_map[prospect_id] = {
                "email": best.get("email", ""),
                "status": best.get("validationStatus", ""),
            }

    return email_map


def run_email_enrichment() -> int:
    conn = get_connection()
    init_db(conn)
    prospects = get_prospects_without_email(conn)
    conn.close()

    if not prospects:
        print("[Stage 4] No prospects need email enrichment.")
        return 0

    print(f"[Stage 4 / Surfe] {len(prospects)} prospects to enrich\n")

    email_map = enrich_emails(prospects)

    conn = get_connection()
    found = 0
    for pid, info in email_map.items():
        if info["email"]:
            update_prospect(conn, pid, email=info["email"], email_status=info["status"])
            found += 1

    conn.close()

    print(f"\nResults:")
    for p in prospects:
        info = email_map.get(p["id"])
        email = info["email"] if info else "—"
        print(f"  [{p['company_name']}] {p['name']} ({p.get('role', '?')}): {email}")

    print(f"\nDone: {found}/{len(prospects)} emails found and saved to DB")
    return found


def main() -> None:
    argparse.ArgumentParser(description="Email enrichment via Surfe (stage 4)").parse_args()
    run_email_enrichment()


if __name__ == "__main__":
    main()
