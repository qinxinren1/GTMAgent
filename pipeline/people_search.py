"""
Stage 2 — Surfe people by job title (no enrich)

1. Read Exa company JSON (stage1)
2. Optional ICP filter on Exa entities
3. For each company: ``POST /v2/people/search`` with ``companies.domains`` + ``people.jobTitles`` only
4. Write JSON (people from Surfe: name, title, LinkedIn)

Env:
  SURFE_API_KEY
  SURFE_JOB_TITLES=Head of Talent,Recruiter,...  (optional; falls back to DEFAULT_JOB_TITLES)
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import asdict, dataclass, field

import requests
from dotenv import load_dotenv

load_dotenv()

SURFE_API_KEY = os.environ.get("SURFE_API_KEY", "").strip()
SURFE_BASE = "https://api.surfe.com/v2"
HEADERS = {
    "Authorization": f"Bearer {SURFE_API_KEY}",
    "Content-Type": "application/json",
}

# Used when SURFE_JOB_TITLES is unset (comma-separated in .env overrides)
DEFAULT_JOB_TITLES = [
    "Head of Talent",
    "Head of Talent Acquisition",
    "Talent Acquisition",
    "Recruiter",
    "Head of People",
    "VP People",
]

EU_COUNTRIES = {
    "france", "germany", "netherlands", "sweden", "denmark", "finland",
    "norway", "spain", "portugal", "belgium", "austria", "switzerland",
    "ireland", "poland", "czechia", "czech republic", "estonia",
    "latvia", "lithuania", "united kingdom",
}

COMPANY_RESULTS_PATH = "output/stage1_company_results.json"
PEOPLE_RESULTS_PATH = "output/stage2_people_output.json"


def passes_icp(entity: dict) -> tuple[bool, str]:
    props = entity.get("properties", {})
    workforce = props.get("workforce") or {}
    headcount = workforce.get("total")
    if headcount is None:
        return False, "unknown headcount"
    if headcount < 20:
        return False, f"too small ({headcount} employees)"
    if headcount > 300:
        return False, f"too large ({headcount} employees)"
    hq = props.get("headquarters") or {}
    country = (hq.get("country") or "").lower()
    if country not in EU_COUNTRIES:
        return False, f"not EU ({country})"
    return True, "passes ICP"


def normalize_domain(domain: str) -> str:
    d = (domain or "").strip().lower()
    d = d.replace("https://", "").replace("http://", "")
    return d.split("/")[0].rstrip("/")


def _csv_env(name: str) -> list[str]:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


def resolve_job_titles(job_titles: list[str] | None) -> list[str]:
    """``None`` → env ``SURFE_JOB_TITLES`` or ``DEFAULT_JOB_TITLES``; ``[]`` → must set env or defaults apply."""
    if job_titles is not None:
        return list(job_titles)
    env_titles = _csv_env("SURFE_JOB_TITLES")
    return env_titles if env_titles else list(DEFAULT_JOB_TITLES)


def surfe_search_by_job_titles(
    domain: str,
    job_titles: list[str],
    *,
    limit_per_page: int = 100,
    max_pages: int = 25,
) -> list[dict]:
    """Surfe v2: domain + ``people.jobTitles`` only; paginates with ``nextPageToken``."""
    host = normalize_domain(domain)
    if not host:
        return []
    if not job_titles:
        print(f"    ⚠ No job titles configured; skip {host}")
        return []

    all_rows: list[dict] = []
    page_token: str | None = None
    people_block = {"jobTitles": job_titles}

    for _ in range(max_pages):
        payload: dict = {
            "companies": {"domains": [host]},
            "people": people_block,
            "limit": min(max(limit_per_page, 1), 200),
        }
        if page_token:
            payload["pageToken"] = page_token

        try:
            resp = requests.post(
                f"{SURFE_BASE}/people/search",
                headers=HEADERS,
                json=payload,
                timeout=30,
            )
        except Exception as e:
            print(f"    ⚠ Surfe search exception: {e}")
            break

        if resp.status_code not in (200, 201):
            print(f"    ⚠ Surfe search error {resp.status_code}: {(resp.text or '')[:600]}")
            break

        data = resp.json()
        batch = data.get("people") or []
        for p in batch:
            all_rows.append(p)
            print(
                f"    Found: {p.get('firstName')} {p.get('lastName')} — {p.get('jobTitle')}"
            )

        page_token = (data.get("nextPageToken") or "").strip() or None
        if not page_token:
            break
        time.sleep(0.35)

    if not all_rows:
        print(f"    No people returned for {host}")
    return all_rows


@dataclass
class PersonHit:
    first_name: str = ""
    last_name: str = ""
    job_title: str = ""
    linkedin_url: str = ""


@dataclass
class CompanyPeople:
    company_name: str
    domain: str
    hq_city: str
    hq_country: str
    headcount: int
    icp_reason: str
    people: list[PersonHit] = field(default_factory=list)


def run_people(
    input_path: str = COMPANY_RESULTS_PATH,
    output_path: str = PEOPLE_RESULTS_PATH,
    *,
    job_titles: list[str] | None = None,
) -> list[CompanyPeople]:
    if not SURFE_API_KEY:
        raise ValueError("Set SURFE_API_KEY env var")

    titles = resolve_job_titles(job_titles)
    print(f"[Surfe] jobTitles ({len(titles)}): {titles[:8]}{'…' if len(titles) > 8 else ''}")

    parent = os.path.dirname(output_path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    with open(input_path) as f:
        exa_results = json.load(f)

    if isinstance(exa_results, dict):
        results = exa_results.get("results", [])
    else:
        results = exa_results

    print(f"\n[Stage 2] {len(results)} Exa rows\n")

    out: list[CompanyPeople] = []

    for item in results:
        entities = item.get("entities") or []
        if not entities:
            continue

        entity = entities[0]
        props = entity.get("properties", {})
        name = props.get("name", item.get("title", "?"))

        ok, reason = passes_icp(entity)
        if not ok:
            print(f"  ✗ {name:<30} {reason}")
            continue

        hq = props.get("headquarters") or {}
        workforce = props.get("workforce") or {}
        headcount = int(workforce.get("total") or 0)

        url = item.get("url", "")
        domain = url.replace("https://", "").replace("http://", "").rstrip("/")

        print(f"  ✓ {name:<30} {headcount} ppl | {domain}")

        raw = surfe_search_by_job_titles(domain, titles)
        hits = [
            PersonHit(
                first_name=p.get("firstName", ""),
                last_name=p.get("lastName", ""),
                job_title=p.get("jobTitle", ""),
                linkedin_url=p.get("linkedInUrl") or p.get("linkedinUrl") or "",
            )
            for p in raw
        ]

        out.append(
            CompanyPeople(
                company_name=name,
                domain=domain,
                hq_city=hq.get("city") or "",
                hq_country=hq.get("country") or "",
                headcount=headcount,
                icp_reason=reason,
                people=hits,
            )
        )
        time.sleep(1.0)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump([asdict(x) for x in out], f, indent=2, ensure_ascii=False)

    n_people = sum(len(x.people) for x in out)
    print(f"\n{'=' * 50}\nDone: {len(out)} companies, {n_people} people → {output_path}\n")
    return out


if __name__ == "__main__":
    run_people()
