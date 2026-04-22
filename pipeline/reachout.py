"""
Stage 5 — Outreach message generation

Reads stage4 people with emails, classifies each person into an outreach
chain (TA Leader or Founder), and generates personalized 3-message sequences
for both LinkedIn and email using Claude.

Usage:
  python -m pipeline.reachout
  python -m pipeline.reachout --input output/stage4_people_with_emails.json
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

STAGE4_JSON = os.path.join(_OUTPUT_BASE, "stage4_people_with_emails.json")
STAGE1_JSON = os.path.join(_OUTPUT_BASE, "stage1_company_results.json")
OUTPUT_JSON = os.path.join(_OUTPUT_BASE, "stage5_reachout.json")
DASHBOARD_JSON = os.path.join(_OUTPUT_BASE, "dashboard_state.json")

AWS_REGION = os.environ.get("AWS_REGION", "eu-central-1")
MODEL = os.environ.get("ANTHROPIC_MODEL", "eu.anthropic.claude-opus-4-6-v1")

SYSTEM_PROMPT = """\
You are a copywriter for Avery, an AI recruiting tool founded by Alisher in Amsterdam.

Your job: generate a personalized 3-message outreach sequence for a prospect.
You will be given the prospect's info. First classify them into Chain 1 or Chain 2, then generate messages following the exact tone and structure of the examples below.

The 3-message logic:
Opener — warm, personal, connect/say hi. No pitch.
Nudge with value — soft hook, one useful line, light coffee ask.
Step back — remove pressure, leave door open.

=== CHAIN 1 — TA Leaders at Scale-ups ===
Use for: Heads of TA, recruiters, HR leaders, People Ops, HRBP at funded startups/scale-ups.

LinkedIn examples:
Msg 1 — Connect (Day 0):
Hey [First], Alisher here, founder of Avery 🙌 Not sure we've crossed paths, but I see we've got loads of TA folks in common. Would love to connect — Amsterdam to [City].

Msg 2 — Nudge (Day 5, after accept):
Thanks for the add [First] 🙌 Been chatting with ~500 Heads of TA across Europe the past 6 months, and the same three things keep coming up (sourcing speed, data trust, mis-hires). Curious where you sit on it. Up for a virtual coffee next week? ☕️

Msg 3 — Step back (Day 18):
Hey [First], not going to chase further — know how the inbox looks. If hiring at [Company] ever gets painful, I'm around. Good luck with the year 🍀

Email examples:
Msg 1 — Opener (Day 2):
Subject: quick hello from Amsterdam

Hi [First],
Alisher here, founder of Avery. We're not connected yet, but I spend most of my week talking to Heads of TA across Europe and your name came up a few times.
No pitch — just wanted to say hi and put myself on your radar.
Alisher

Msg 2 — Nudge with value (Day 10):
Subject: three patterns from 40 Heads of TA

Hi [First],
Been collecting notes from ~500 TA leaders across European scale-ups. Three things keep coming up for 2026:
Sourcing is still the #1 time sink, even with LinkedIn Recruiter
Nobody trusts their own hiring data enough to act on it
Gap from "we need X" to first qualified shortlist is still 3–4 weeks
Curious if any of that lands for [Company]. Happy to trade notes over coffee if useful ☕️
Alisher

Msg 3 — Step back (Day 25):
Subject: stepping back

Hi [First],
Haven't heard back — totally fair, inbox overload is real. I'll stop chasing.
If sourcing speed or mis-hires ever become the thing keeping you up, Avery might be worth a look by then. One-pager here: [link]. No reply needed.
Good luck 🍀
Alisher

=== CHAIN 2 — Founders ===
Use for: CEO, Co-founder, Founder, MD at smaller companies (<80 employees or Series A) who likely handle hiring directly.

LinkedIn examples:
Msg 1 — Connect (Day 0):
Hey [First], Alisher here, founder of Avery 💯 Fellow founder over in Amsterdam. Helping smaller agencies compete without paying LinkedIn Recruiter prices. Would be great to connect.

Msg 2 — Nudge (Day 5, after accept):
Thanks for connecting [First] 🙌 Quick one — how's the sourcing-to-placement ratio looking for your team these days? Hearing from agency founders it's 70/30 (sourcing wins) and they hate it. We're building the thing that flips it. Up for 15 min screenshare next week? I'll run it against one of your live roles ☕️

Msg 3 — Step back (Day 18):
Hey [First], not going to chase further 🍀 If LinkedIn Recruiter costs get painful or you want to take on more roles without more headcount, I'm around.

Email examples:
Msg 1 — Opener (Day 2):
Subject: fellow founder, quick hello

Hi [First],
Alisher here, founder of Avery — Amsterdam-based. Been watching what [Company] is doing and wanted to put myself on your radar.
No pitch. Just a hi from one founder to another.
Alisher

Msg 2 — Nudge with value (Day 10):
Subject: 70% sourcing, 30% placing

Hi [First],
Been comparing how small agencies source vs. the LinkedIn Recruiter-heavy shops. The pattern: the agencies winning right now aren't the ones with the biggest seat count — they're the ones getting qualified, motivated candidates in front of clients 5–10 days faster.
Avery's agents do sourcing and first-round qualification 24/7. Output: pre-qualified people ready to place. No LinkedIn Recruiter contract needed.
Happy to run it against one of your live roles — 15 min screenshare, output speaks for itself.
Alisher

Msg 3 — Step back (Day 25):
Subject: last one

Hi [First],
Not going to chase further. If LinkedIn Recruiter costs ever get painful or you want to take on more roles without adding headcount, Avery's here: [link].
Good luck out there 🍀
Alisher

=== INSTRUCTIONS ===
- Follow the EXACT tone, length, and structure of the examples above
- Personalize based on their specific role, company, and city — don't just copy the examples verbatim
- Keep LinkedIn messages under 300 chars
- Keep emails short (3-5 sentences body max)
- Use [First], [Company], [City] as placeholders
- DO NOT use markdown formatting — plain text only

Respond with ONLY valid JSON (no code fences) in this exact format:
{
  "chain": "ta_leader" or "founder",
  "linkedin_msg1": "...",
  "linkedin_msg2": "...",
  "linkedin_msg3": "...",
  "email_msg1": "Subject: ...\\n\\nHi [First],\\n...\\nAlisher",
  "email_msg2": "Subject: ...\\n\\nHi [First],\\n...\\nAlisher",
  "email_msg3": "Subject: ...\\n\\nHi [First],\\n...\\nAlisher"
}"""


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


def build_prospect_prompt(
    person: dict[str, Any],
    company_name: str,
    company_info: dict[str, Any],
) -> str:
    role = (person.get("currentRole") or {}).get("role", "unknown")
    loc = (person.get("location") or {}).get("current", {})
    city = loc.get("city", "unknown")
    emails = person.get("emails") or []
    email = emails[0]["email"] if emails else "none"

    employees = company_info.get("employees", "unknown")
    funding = company_info.get("latest_round", "unknown")
    description = company_info.get("description", "")

    return f"""Prospect:
- Name: {person.get("firstName", "")} {person.get("lastName", "")}
- Role: {role}
- Company: {company_name}
- Company size: {employees} employees
- Funding: {funding}
- City: {city}
- Email: {email}
- Company description: {description[:200]}

Classify this person into Chain 1 (TA Leader) or Chain 2 (Founder) and generate the 6 messages."""


def generate_messages(
    client: anthropic.AnthropicBedrock,
    person: dict[str, Any],
    company_name: str,
    company_info: dict[str, Any],
) -> dict[str, Any]:
    prompt = build_prospect_prompt(person, company_name, company_info)

    response = client.messages.create(
        model=MODEL,
        max_tokens=2048,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )

    text = response.content[0].text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[-1].rsplit("```", 1)[0].strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        print(f"    ⚠ failed to parse LLM response: {text[:200]}")
        return {}


def fill_placeholders(messages: dict[str, Any], person: dict[str, Any], company: str) -> dict[str, Any]:
    first = person.get("firstName", "")
    loc = (person.get("location") or {}).get("current", {})
    city = loc.get("city", "unknown")

    filled = {}
    for key, val in messages.items():
        if isinstance(val, str):
            filled[key] = (
                val.replace("[First]", first)
                .replace("[Company]", company)
                .replace("[City]", city)
            )
        else:
            filled[key] = val
    return filled


def build_dashboard_entry(
    person: dict[str, Any],
    company: str,
    domain: str,
    messages: dict[str, Any],
) -> dict[str, Any]:
    emails = person.get("emails") or []
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
        "chain": messages.get("chain", ""),
        "messages": {
            k: v for k, v in messages.items() if k != "chain"
        },
        "status": {
            "linkedin_msg1": "draft",
            "linkedin_msg2": "draft",
            "linkedin_msg3": "draft",
            "email_msg1": "draft",
            "email_msg2": "draft",
            "email_msg3": "draft",
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
            chain = messages.get("chain", "?")
            print(f"→ {chain}")

            dashboard_entry = build_dashboard_entry(p, company_name, domain, messages)
            all_entries.append(dashboard_entry)

            company_results.append({
                "name": name,
                "role": role,
                "chain": chain,
                "messages": {k: v for k, v in messages.items() if k != "chain"},
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

    n_ta = sum(1 for e in all_entries if e["chain"] == "ta_leader")
    n_founder = sum(1 for e in all_entries if e["chain"] == "founder")
    print(f"\nDone: {len(all_entries)} prospects ({n_ta} TA leaders, {n_founder} founders)")
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
