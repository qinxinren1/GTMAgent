"""
Stage 5 — Outreach message generation.

Reads prospects from the database that don't have messages yet,
generates personalized 3-message outreach sequences using Claude,
and writes messages to the database.

Usage:
  python -m pipeline.reachout
"""

from __future__ import annotations

import argparse
import json
import os
import re
from typing import Any

import anthropic
from dotenv import load_dotenv

from pipeline.db import get_connection, get_prospects_without_messages, init_db, upsert_message

load_dotenv()

AWS_REGION = os.environ.get("AWS_REGION", "eu-central-1")
MODEL = os.environ.get("ANTHROPIC_MODEL", "eu.anthropic.claude-opus-4-6-v1")

SYSTEM_PROMPT = """\
You are a copywriter for Avery, an AI recruiting tool founded by Alisher in Amsterdam.

Your job: write a personalized outreach sequence for a specific prospect.
You will receive the prospect's name, role, company, city, company stage, and size.

=== CORE RULES ===
LinkedIn: 3 messages. Email: 2 messages.

LinkedIn (3 messages):
Msg 1 — OPENER: warm, personal, connect/say hi. ZERO pitch. ZERO product mention. Just a human wanting to connect.
Msg 2 — NUDGE WITH VALUE: share one genuine insight or pattern relevant to THEIR specific role. Light coffee ask. No hard sell.
Msg 3 — STEP BACK: remove all pressure. Leave the door open. Short.

Email (2 messages):
Msg 1 — WARM INTRO + VALUE: introduce yourself warmly, then share one genuine insight or pattern relevant to their role. End with a soft coffee/chat ask. This combines the personal hello with real value in one email.
Msg 2 — STEP BACK: haven't heard back, no pressure, door open. Short.

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
Msg 1: Hey [First], Alisher here, founder of Avery 🙌 Not sure we've crossed paths, but I see we've got loads of TA folks in common. Would love to connect, Amsterdam to [City].
Msg 2: Thanks for the add [First] 🙌 Been chatting with ~500 Heads of TA across Europe the past 6 months, and the same three things keep coming up (sourcing speed, data trust, mis-hires). Curious where you sit on it. Up for a virtual coffee next week? ☕️
Msg 3: Hey [First], not going to chase further, know how the inbox looks. If hiring at [Company] ever gets painful, I'm around. Good luck with the year 🍀

Email:
Msg 1:
Subject: quick hello from Amsterdam

Hi [First],
Alisher here, founder of Avery. I spend most of my week talking to Heads of TA across Europe and your name came up a few times.
Been collecting notes from ~500 TA leaders across European scale-ups. Three things keep coming up for 2026:
Sourcing is still the #1 time sink, even with LinkedIn Recruiter
Nobody trusts their own hiring data enough to act on it
Gap from "we need X" to first qualified shortlist is still 3-4 weeks
Curious if any of that lands for [Company]. Happy to trade notes over coffee if useful ☕️
Alisher

Msg 2:
Subject: stepping back

Hi [First],
Haven't heard back, totally fair, inbox overload is real. I'll stop chasing.
If sourcing speed or mis-hires ever become the thing keeping you up, Avery might be worth a look by then. One-pager here: [link]. No reply needed.
Good luck 🍀
Alisher

=== REFERENCE STYLE (Founders) ===

LinkedIn:
Msg 1: Hey [First], Alisher here, founder of Avery 💯 Fellow founder over in Amsterdam. Helping smaller agencies compete without paying LinkedIn Recruiter prices. Would be great to connect.
Msg 2: Thanks for connecting [First] 🙌 Quick one, how's the sourcing-to-placement ratio looking for your team these days? Hearing from agency founders it's 70/30 (sourcing wins) and they hate it. We're building the thing that flips it. Up for 15 min screenshare next week? I'll run it against one of your live roles ☕️
Msg 3: Hey [First], not going to chase further 🍀 If LinkedIn Recruiter costs get painful or you want to take on more roles without more headcount, I'm around.

Email:
Msg 1:
Subject: fellow founder, quick hello

Hi [First],
Alisher here, founder of Avery, Amsterdam-based. Been watching what [Company] is doing and wanted to put myself on your radar.
Been comparing how small agencies source vs. the LinkedIn Recruiter-heavy shops. The pattern: the agencies winning right now aren't the ones with the biggest seat count, they're the ones getting qualified, motivated candidates in front of clients 5-10 days faster.
Avery's agents do sourcing and first-round qualification 24/7. Output: pre-qualified people ready to place. No LinkedIn Recruiter contract needed.
Happy to run it against one of your live roles, 15 min screenshare, output speaks for itself.
Alisher

Msg 2:
Subject: last one

Hi [First],
Not going to chase further. If LinkedIn Recruiter costs ever get painful or you want to take on more roles without adding headcount, Avery's here: [link].
Good luck out there 🍀
Alisher

=== IMPORTANT ===
- These examples are REFERENCE for tone and structure only. Do NOT copy them verbatim.
- Write messages that speak to THIS person's specific role and pain points.
- LinkedIn Msg 1 must have ZERO pitch, ZERO product mention. Just connecting.
- Email Msg 1 combines warm intro + value/insight + soft ask. NOT just a hello.
- Keep LinkedIn messages under 300 chars each.
- Keep emails 5-8 sentences body max.
- Use [First], [Company], [City] as placeholders.
- Plain text only, no markdown formatting.
- Use emojis sparingly (🙌 🍀 ☕ 💯).
- NEVER use em dashes (—), en dashes (–), or any unicode dashes. Use a comma instead.
- If instructed to generate LinkedIn only, do NOT include email messages in the output."""


_DASH_PATTERN = re.compile(r"[‐-―−–—]")


def _clean_text(text: str) -> str:
    text = _DASH_PATTERN.sub(",", text)
    return text.replace(" , ", ", ")


def build_prospect_prompt(prospect: dict[str, Any]) -> str:
    has_email = bool(prospect.get("email"))
    channels = "LinkedIn + email" if has_email else "LinkedIn only (no email available)"

    json_format = """{
  "linkedin_msg1": "...",
  "linkedin_msg2": "...",
  "linkedin_msg3": "..."""
    if has_email:
        json_format += """,
  "email_msg1": "Subject: ...\\n\\nHi [First],\\n...\\nAlisher",
  "email_msg2": "Subject: ...\\n\\nHi [First],\\n...\\nAlisher"
}"""
    else:
        json_format += "\n}"

    return f"""Prospect:
- Name: {prospect.get('first_name', '')} {prospect.get('last_name', '')}
- Role: {prospect.get('role', 'unknown')}
- Company: {prospect.get('company_name', 'unknown')}
- Company size: {prospect.get('employees') or 'unknown'} employees
- Funding stage: {prospect.get('latest_round') or 'unknown'}
- City: {prospect.get('city') or 'unknown'}
- Company description: {(prospect.get('company_description') or '')[:200]}

Write an outreach sequence (LinkedIn: 3 messages, {channels.replace('LinkedIn + email', 'email: 2 messages').replace('LinkedIn only (no email available)', 'no email')}) personalized to this person's specific role and situation.

Respond with ONLY valid JSON (no code fences):
{json_format}"""


def generate_messages(
    client: anthropic.AnthropicBedrock,
    prospect: dict[str, Any],
    retries: int = 2,
) -> dict[str, Any]:
    prompt = build_prospect_prompt(prospect)

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


def fill_placeholders(messages: dict[str, Any], prospect: dict[str, Any]) -> dict[str, Any]:
    first = prospect.get("first_name") or ""
    company = prospect.get("company_name") or ""
    city = prospect.get("city") or "unknown"

    filled = {}
    for key, val in messages.items():
        if isinstance(val, str):
            val = val.replace("[First]", first).replace("[Company]", company).replace("[City]", city)
            val = _clean_text(val)
            filled[key] = val
        else:
            filled[key] = val
    return filled


def _parse_email_msg(text: str) -> tuple[str, str]:
    if text.startswith("Subject:"):
        parts = text.split("\n\n", 1)
        subject = parts[0].replace("Subject:", "").strip()
        body = parts[1].strip() if len(parts) > 1 else ""
        return subject, body
    return "", text


def save_messages_to_db(prospect_id: int, messages: dict[str, Any], has_email: bool) -> None:
    conn = get_connection()
    init_db(conn)

    for key, content in messages.items():
        if not isinstance(content, str):
            continue

        if key.startswith("linkedin_msg"):
            channel = "linkedin"
            seq = int(key[-1])
            upsert_message(conn, prospect_id=prospect_id, channel=channel,
                           sequence_num=seq, subject=None, content=content, status="draft")

        elif key.startswith("email_msg"):
            channel = "email"
            seq = int(key[-1])
            subject, body = _parse_email_msg(content)
            upsert_message(conn, prospect_id=prospect_id, channel=channel,
                           sequence_num=seq, subject=subject, content=body, status="draft")

    conn.close()


def run_reachout() -> int:
    conn = get_connection()
    init_db(conn)
    prospects = get_prospects_without_messages(conn)
    conn.close()

    if not prospects:
        print("[Stage 5] No prospects need message generation.")
        return 0

    print(f"[Stage 5 / Reachout] {len(prospects)} prospects, model={MODEL}\n")

    client = anthropic.AnthropicBedrock(aws_region=AWS_REGION)
    generated = 0

    for p in prospects:
        name = p.get("name", "?")
        role = p.get("role", "?")
        company = p.get("company_name", "?")
        has_email = bool(p.get("email"))

        print(f"  {name} ({role} @ {company})...", end=" ", flush=True)
        messages = generate_messages(client, p)

        if not messages:
            print("FAILED")
            continue

        messages = fill_placeholders(messages, p)
        print(f"→ {'LI + email' if has_email else 'LI only'}")

        save_messages_to_db(p["id"], messages, has_email)
        generated += 1

    print(f"\nDone: {generated}/{len(prospects)} prospects with messages generated")
    return generated


def main() -> None:
    argparse.ArgumentParser(description="Outreach message generation (stage 5)").parse_args()
    run_reachout()


if __name__ == "__main__":
    main()
