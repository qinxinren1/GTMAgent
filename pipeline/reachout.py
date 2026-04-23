"""
Stage 5 — Outreach message generation

Reads stage4 people with emails and generates personalized 3-message
outreach sequences for LinkedIn (and email when available) using Claude.

Usage:
  python -m pipeline.reachout
  python -m pipeline.reachout --input output/stage4_people_with_emails.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
from typing import Any

import anthropic
from dotenv import load_dotenv

load_dotenv()

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_OUTPUT_BASE = os.environ.get("AVERY_OUTPUT_DIR", os.path.join(_REPO_ROOT, "output"))

STAGE4_JSON = os.path.join(_OUTPUT_BASE, "stage4_people_with_emails.json")
STAGE1_JSON = os.path.join(_OUTPUT_BASE, "stage1_company_results.json")
OUTPUT_JSON = os.path.join(_OUTPUT_BASE, "stage5_reachout.json")
DASHBOARD_JSON = os.path.join(_OUTPUT_BASE, "dashboard_state.json")

AWS_REGION = os.environ.get("AWS_REGION", "eu-central-1")
MODEL = os.environ.get("ANTHROPIC_MODEL", "eu.anthropic.claude-opus-4-6-v1")

SYSTEM_PROMPT = """\
You are a copywriter for Avery, an AI recruiting tool founded by Alisher in Amsterdam.

Your job: write a personalized 3-message outreach sequence for a specific prospect.
You will receive the prospect's name, role, company, city, company stage, and size.

=== CORE RULES ===
Every sequence is 3 messages. The logic is STRICT:

Msg 1 — OPENER: warm, personal, connect/say hi. ZERO pitch. ZERO product mention. Just a human wanting to connect.
Msg 2 — NUDGE WITH VALUE: share one genuine insight or pattern relevant to THEIR specific role. Light coffee ask. No hard sell.
Msg 3 — STEP BACK: remove all pressure. Leave the door open. Short.

=== HOW TO PERSONALIZE ===
The messages must be tailored to WHO this person is:

- A Head of TA / TA Lead → talk about sourcing speed, time-to-shortlist, data trust. Peer-level ("been chatting with 500 Heads of TA").
- A Recruiter / Sourcer → talk about sourcing-to-placement ratio, candidate quality, time spent on unqualified leads.
- A Head of People / CPO / HR Ops → talk about hiring velocity, mis-hires, scaling the team without scaling headcount.
- A HRBP / HR Generalist → talk about wearing too many hats, hiring being one of 10 things on the plate.
- A Founder / CEO / Co-founder → talk founder-to-founder. They're probably hiring directly. Talk about speed, competing for talent without a big TA team.

Think about what keeps THIS person up at night given their role, then write to that.

=== REFERENCE STYLE (TA/HR people) ===

LinkedIn:
Msg 1: Hey [First], Alisher here, founder of Avery 🙌 Not sure we've crossed paths, but I see we've got loads of TA folks in common. Would love to connect — Amsterdam to [City].
Msg 2: Thanks for the add [First] 🙌 Been chatting with ~500 Heads of TA across Europe the past 6 months, and the same three things keep coming up (sourcing speed, data trust, mis-hires). Curious where you sit on it. Up for a virtual coffee next week? ☕️
Msg 3: Hey [First], not going to chase further — know how the inbox looks. If hiring at [Company] ever gets painful, I'm around. Good luck with the year 🍀

Email:
Msg 1:
Subject: quick hello from Amsterdam

Hi [First],
Alisher here, founder of Avery. We're not connected yet, but I spend most of my week talking to Heads of TA across Europe and your name came up a few times.
No pitch — just wanted to say hi and put myself on your radar.
Alisher

Msg 2:
Subject: three patterns from 500 Heads of TA

Hi [First],
Been collecting notes from ~500 TA leaders across European scale-ups. Three things keep coming up for 2026:
Sourcing is still the #1 time sink, even with LinkedIn Recruiter
Nobody trusts their own hiring data enough to act on it
Gap from "we need X" to first qualified shortlist is still 3–4 weeks
Curious if any of that lands for [Company]. Happy to trade notes over coffee if useful ☕️
Alisher

Msg 3:
Subject: stepping back

Hi [First],
Haven't heard back — totally fair, inbox overload is real. I'll stop chasing.
If sourcing speed or mis-hires ever become the thing keeping you up, Avery might be worth a look by then. One-pager here: [link]. No reply needed.
Good luck 🍀
Alisher

=== REFERENCE STYLE (Founders) ===

LinkedIn:
Msg 1: Hey [First], Alisher here, founder of Avery 💯 Fellow founder over in Amsterdam. Helping smaller agencies compete without paying LinkedIn Recruiter prices. Would be great to connect.
Msg 2: Thanks for connecting [First] 🙌 Quick one — how's the sourcing-to-placement ratio looking for your team these days? Hearing from agency founders it's 70/30 (sourcing wins) and they hate it. We're building the thing that flips it. Up for 15 min screenshare next week? I'll run it against one of your live roles ☕️
Msg 3: Hey [First], not going to chase further 🍀 If LinkedIn Recruiter costs get painful or you want to take on more roles without more headcount, I'm around.

Email:
Msg 1:
Subject: fellow founder, quick hello

Hi [First],
Alisher here, founder of Avery — Amsterdam-based. Been watching what [Company] is doing and wanted to put myself on your radar.
No pitch. Just a hi from one founder to another.
Alisher

Msg 2:
Subject: 70% sourcing, 30% placing

Hi [First],
Been comparing how small agencies source vs. the LinkedIn Recruiter-heavy shops. The pattern: the agencies winning right now aren't the ones with the biggest seat count — they're the ones getting qualified, motivated candidates in front of clients 5–10 days faster.
Avery's agents do sourcing and first-round qualification 24/7. Output: pre-qualified people ready to place. No LinkedIn Recruiter contract needed.
Happy to run it against one of your live roles — 15 min screenshare, output speaks for itself.
Alisher

Msg 3:
Subject: last one

Hi [First],
Not going to chase further. If LinkedIn Recruiter costs ever get painful or you want to take on more roles without adding headcount, Avery's here: [link].
Good luck out there 🍀
Alisher

=== IMPORTANT ===
- These examples are REFERENCE for tone and structure only. Do NOT copy them verbatim.
- Write messages that speak to THIS person's specific role and pain points.
- Msg 1 must have ZERO pitch, ZERO product mention. Just connecting.
- Msg 2 must lead with value/insight BEFORE any ask.
- Msg 3 must be short, no pressure, door open.
- Keep LinkedIn messages under 300 chars each.
- Keep emails short (3-5 sentences body max).
- Use [First], [Company], [City] as placeholders.
- Plain text only, no markdown formatting.
- Use emojis sparingly (🙌 🍀 ☕ 💯).
- NEVER use em dashes (—), en dashes (–), or any unicode dashes. Use a comma instead.
- If instructed to generate LinkedIn only, do NOT include email messages in the output."""


def load_stage4(path: str = STAGE4_JSON) -> list[dict[str, Any]]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_company_meta() -> dict[str, dict[str, Any]]:
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
            "latest_round": (props.get("financials") or {}).get(
                "funding_latest_round", {}
            ).get("name"),
            "description": props.get("description", ""),
            "hq_city": (props.get("headquarters") or {}).get("city", ""),
        }
    return meta


def _has_email(person: dict[str, Any]) -> bool:
    emails = person.get("emails") or []
    return bool(emails and emails[0].get("email"))


def build_prospect_prompt(
    person: dict[str, Any],
    company_name: str,
    company_info: dict[str, Any],
) -> str:
    role = (person.get("currentRole") or {}).get("role", "unknown")
    loc = (person.get("location") or {}).get("current", {})
    city = loc.get("city", "unknown")
    employees = company_info.get("employees", "unknown")
    funding = company_info.get("latest_round", "unknown")
    description = company_info.get("description", "")
    has_email = _has_email(person)

    channels = "LinkedIn + email" if has_email else "LinkedIn only (no email available)"
    json_format = """{
  "linkedin_msg1": "...",
  "linkedin_msg2": "...",
  "linkedin_msg3": "..."""
    if has_email:
        json_format += """,
  "email_msg1": "Subject: ...\\n\\nHi [First],\\n...\\nAlisher",
  "email_msg2": "Subject: ...\\n\\nHi [First],\\n...\\nAlisher",
  "email_msg3": "Subject: ...\\n\\nHi [First],\\n...\\nAlisher"
}"""
    else:
        json_format += "\n}"

    return f"""Prospect:
- Name: {person.get("firstName", "")} {person.get("lastName", "")}
- Role: {role}
- Company: {company_name}
- Company size: {employees} employees
- Funding stage: {funding}
- City: {city}
- Company description: {description[:200]}

Write a 3-message outreach sequence ({channels}) personalized to this person's specific role and situation. Think about what someone with the role "{role}" at a {employees}-person {funding} company cares about day-to-day, and write to that.

Respond with ONLY valid JSON (no code fences):
{json_format}"""


def generate_messages(
    client: anthropic.AnthropicBedrock,
    person: dict[str, Any],
    company_name: str,
    company_info: dict[str, Any],
    retries: int = 2,
) -> dict[str, Any]:
    prompt = build_prospect_prompt(person, company_name, company_info)

    for attempt in range(1, retries + 1):
        response = client.messages.create(
            model=MODEL,
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )

        text = response.content[0].text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[-1].rsplit("```", 1)[0].strip()

        try:
            return json.loads(text)
        except json.JSONDecodeError:
            if attempt < retries:
                print(f"retry {attempt}...", end=" ", flush=True)
            else:
                print(f"    ⚠ failed to parse LLM response: {text[:200]}")
    return {}


def fill_placeholders(messages: dict[str, Any], person: dict[str, Any], company: str) -> dict[str, Any]:
    first = person.get("firstName", "")
    loc = (person.get("location") or {}).get("current", {})
    city = loc.get("city", "unknown")

    dash_pattern = re.compile(r"[‐-―−–—]")
    filled = {}
    for key, val in messages.items():
        if isinstance(val, str):
            val = (
                val.replace("[First]", first)
                .replace("[Company]", company)
                .replace("[City]", city)
            )
            val = dash_pattern.sub(", ", val).replace(" ,  ", ", ")
            filled[key] = val
        else:
            filled[key] = val
    return filled


def build_dashboard_entry(
    person: dict[str, Any],
    company: str,
    domain: str,
    messages: dict[str, Any],
) -> dict[str, Any]:
    has_email = _has_email(person)
    emails = person.get("emails") or []
    email_status = "draft" if has_email else "skipped"
    return {
        "id": person.get("compositeId", person.get("name", "")),
        "name": person.get("name", ""),
        "firstName": person.get("firstName", ""),
        "role": (person.get("currentRole") or {}).get("role", ""),
        "company": company,
        "domain": domain,
        "city": ((person.get("location") or {}).get("current", {}).get("city", "")),
        "linkedin": person.get("linkedin", ""),
        "email": emails[0]["email"] if emails else "",
        "messages": dict(messages),
        "status": {
            "linkedin_msg1": "draft",
            "linkedin_msg2": "draft",
            "linkedin_msg3": "draft",
            "email_msg1": email_status,
            "email_msg2": email_status,
            "email_msg3": email_status,
        },
        "response": "none",
        "notes": "",
    }


def run_reachout(
    input_path: str = STAGE4_JSON,
    output_path: str = OUTPUT_JSON,
) -> list[dict[str, Any]]:
    stage4 = load_stage4(input_path)
    company_meta = load_company_meta()
    client = anthropic.AnthropicBedrock(aws_region=AWS_REGION)

    total = sum(len(e["people"]) for e in stage4)
    print(f"[Stage 5 / Reachout] {total} prospects, model={MODEL}")

    all_entries: list[dict[str, Any]] = []
    out: list[dict[str, Any]] = []

    for entry in stage4:
        company_name = entry["company"]
        domain = entry.get("domain", "")
        info = company_meta.get(company_name, {})
        company_results = []

        for p in entry["people"]:
            name = p.get("name", "?")
            role = (p.get("currentRole") or {}).get("role", "?")

            print(f"  {name} ({role} @ {company_name})...", end=" ", flush=True)
            messages = generate_messages(client, p, company_name, info)

            if not messages:
                print("FAILED")
                continue

            messages = fill_placeholders(messages, p, company_name)
            has_email = _has_email(p)
            print(f"→ {'LI + email' if has_email else 'LI only'}")

            dashboard_entry = build_dashboard_entry(p, company_name, domain, messages)
            all_entries.append(dashboard_entry)

            company_results.append({
                "name": name,
                "role": role,
                "messages": dict(messages),
            })

        out.append({
            "company": company_name,
            "domain": domain,
            "prospects": company_results,
        })

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    with open(DASHBOARD_JSON, "w", encoding="utf-8") as f:
        json.dump(all_entries, f, indent=2, ensure_ascii=False)

    n_with_email = sum(1 for e in all_entries if e.get("email"))
    print(f"\nDone: {len(all_entries)} prospects ({n_with_email} with email, {len(all_entries) - n_with_email} LI only)")
    print(f"  Messages → {output_path}")
    print(f"  Dashboard → {DASHBOARD_JSON}")
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="Outreach message generation (stage 5)")
    p.add_argument("--input", "-i", default=STAGE4_JSON)
    p.add_argument("--output", "-o", default=OUTPUT_JSON)
    args = p.parse_args()
    run_reachout(input_path=args.input, output_path=args.output)


if __name__ == "__main__":
    main()
