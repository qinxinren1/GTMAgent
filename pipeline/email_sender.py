"""
Stage 6 — Automated email sending via Loops.so

Reads scheduled email messages from the database and sends them on the
correct day according to the outreach sequence schedule using Loops
transactional emails.

Setup:
  1. Create a Loops.so account and get an API key
  2. Create a transactional email template with data variables:
     {subject}, {body}  (the template is just a shell for our content)
  3. Set env vars: LOOPS_API_KEY, LOOPS_TRANSACTIONAL_ID

Sequence timing (from the day the prospect is scheduled):
  Email 1 — Day 0  (opener)
  Email 2 — Day 8  (nudge)
  Email 3 — Day 23 (step back)

Usage:
  python -m pipeline.email_sender schedule
  python -m pipeline.email_sender schedule --prospect-id 5
  python -m pipeline.email_sender send
  python -m pipeline.email_sender send --dry-run
  python -m pipeline.email_sender status
"""

from __future__ import annotations

import argparse
import os
import time
from datetime import date, datetime, timedelta
from typing import Any

import requests
from dotenv import load_dotenv

from pipeline.db import get_connection, init_db, update_message

load_dotenv()

LOOPS_API_KEY = os.environ.get("LOOPS_API_KEY", "")
LOOPS_TRANSACTIONAL_ID = os.environ.get("LOOPS_TRANSACTIONAL_ID", "")
LOOPS_BASE = "https://app.loops.so/api/v1"

SEQUENCE_DAYS = {1: 0, 2: 14}

MAX_SEND_PER_RUN = int(os.environ.get("LOOPS_MAX_PER_RUN", "50"))
SEND_INTERVAL = float(os.environ.get("LOOPS_SEND_INTERVAL", "0.15"))


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {LOOPS_API_KEY}",
        "Content-Type": "application/json",
    }


# ── Schedule ──────────────────────────────────────────────────────────────

def schedule_emails(
    prospect_id: int | None = None,
    start_date: date | None = None,
) -> int:
    conn = get_connection()
    init_db(conn)
    base = start_date or date.today()

    where = "m.channel = 'email' AND m.status = 'draft' AND m.scheduled_date IS NULL"
    params: list[Any] = []
    if prospect_id:
        where += " AND m.prospect_id = ?"
        params.append(prospect_id)

    rows = conn.execute(f"""
        SELECT m.id, m.prospect_id, m.sequence_num, p.email, p.email_status, p.name
        FROM messages m
        JOIN prospects p ON m.prospect_id = p.id
        WHERE {where}
    """, params).fetchall()

    scheduled = 0
    for row in rows:
        if not row["email"]:
            continue

        day_offset = SEQUENCE_DAYS.get(row["sequence_num"], 0)
        send_date = base + timedelta(days=day_offset)

        conn.execute(
            "UPDATE messages SET status = 'scheduled', scheduled_date = ? WHERE id = ?",
            (send_date.isoformat(), row["id"]),
        )
        scheduled += 1
        print(f"  Scheduled: {row['name']} email #{row['sequence_num']} → {send_date}")

    conn.commit()
    conn.close()
    print(f"\n{scheduled} emails scheduled (starting {base})")
    return scheduled


# ── Send ──────────────────────────────────────────────────────────────────

def _send_one(to: str, subject: str, body: str, first_name: str = "") -> dict[str, Any]:
    payload: dict[str, Any] = {
        "transactionalId": LOOPS_TRANSACTIONAL_ID,
        "email": to,
        "addToAudience": False,
        "dataVariables": {
            "subject": subject,
            "body": body.replace("\n", "<br>"),
            "firstName": first_name,
        },
    }
    resp = requests.post(
        f"{LOOPS_BASE}/transactional",
        headers=_headers(),
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def send_due_emails(dry_run: bool = False) -> int:
    conn = get_connection()
    init_db(conn)
    today = date.today().isoformat()

    rows = conn.execute("""
        SELECT m.id, m.subject, m.content, m.sequence_num,
               p.email, p.name, p.first_name, p.response,
               c.name AS company_name
        FROM messages m
        JOIN prospects p ON m.prospect_id = p.id
        JOIN companies c ON p.company_id = c.id
        WHERE m.channel = 'email'
          AND m.status = 'scheduled'
          AND m.scheduled_date <= ?
        ORDER BY m.scheduled_date, p.name, m.sequence_num
    """, (today,)).fetchall()

    if not rows:
        print("No emails due for sending.")
        return 0

    if not dry_run and not LOOPS_API_KEY:
        print("ERROR: LOOPS_API_KEY not set in .env")
        return 0
    if not dry_run and not LOOPS_TRANSACTIONAL_ID:
        print("ERROR: LOOPS_TRANSACTIONAL_ID not set in .env")
        return 0

    print(f"{'[DRY RUN] ' if dry_run else ''}{len(rows)} emails due for sending\n")

    sent = 0

    for row in rows:
        if sent >= MAX_SEND_PER_RUN:
            print(f"\nReached max send limit ({MAX_SEND_PER_RUN}). Remaining will send next run.")
            break

        if row["response"] in ("replied", "meeting_booked", "rejected", "bounced"):
            print(f"  SKIP {row['name']} — response is {row['response']}")
            conn.execute(
                "UPDATE messages SET status = 'skipped' WHERE id = ?", (row["id"],)
            )
            conn.commit()
            continue

        subject = row["subject"] or "(no subject)"
        body = row["content"] or ""
        to_email = row["email"]

        print(f"  → {row['name']} ({row['company_name']}) — email #{row['sequence_num']}: {subject}")

        if dry_run:
            sent += 1
            continue

        try:
            result = _send_one(to_email, subject, body, row["first_name"] or "")
            now = datetime.utcnow().isoformat()

            if result.get("success"):
                conn.execute(
                    "UPDATE messages SET status = 'sent', sent_at = ? WHERE id = ?",
                    (now, row["id"]),
                )
                conn.execute(
                    "INSERT INTO send_log (message_id, ses_message_id, status) VALUES (?, ?, 'sent')",
                    (row["id"], "loops"),
                )
                conn.commit()
                sent += 1
                print(f"    ✓ sent")
            else:
                error_msg = result.get("message", "unknown error")
                conn.execute(
                    "INSERT INTO send_log (message_id, status, error) VALUES (?, 'failed', ?)",
                    (row["id"], error_msg),
                )
                conn.commit()
                print(f"    ✗ FAILED: {error_msg}")

        except requests.RequestException as e:
            error_msg = str(e)
            conn.execute(
                "INSERT INTO send_log (message_id, status, error) VALUES (?, 'failed', ?)",
                (row["id"], error_msg),
            )
            conn.commit()
            print(f"    ✗ FAILED: {error_msg}")

        if sent < len(rows):
            time.sleep(SEND_INTERVAL)

    conn.close()
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Done: {sent} emails {'would be ' if dry_run else ''}sent")
    return sent


# ── Status ────────────────────────────────────────────────────────────────

def check_status() -> None:
    conn = get_connection()
    init_db(conn)

    print("=== Database Stats ===")
    for row in conn.execute("""
        SELECT m.status, COUNT(*) as n
        FROM messages m
        WHERE m.channel = 'email'
        GROUP BY m.status
        ORDER BY m.status
    """).fetchall():
        print(f"  {row['status']}: {row['n']}")

    due = conn.execute("""
        SELECT COUNT(*) FROM messages
        WHERE channel = 'email' AND status = 'scheduled'
          AND scheduled_date <= ?
    """, (date.today().isoformat(),)).fetchone()[0]
    print(f"  Due today or earlier: {due}")
    conn.close()

    print("\n=== Loops.so Account ===")
    if not LOOPS_API_KEY:
        print("  ⚠ LOOPS_API_KEY not set")
        return
    try:
        resp = requests.get(f"{LOOPS_BASE}/api-key", headers=_headers(), timeout=10)
        if resp.status_code == 200:
            print("  ✓ API key valid")
        else:
            print(f"  ✗ API key invalid ({resp.status_code})")

        resp = requests.get(f"{LOOPS_BASE}/transactional", headers=_headers(), timeout=10)
        if resp.status_code == 200:
            templates = resp.json()
            print(f"  Transactional templates: {len(templates)}")
            for t in templates:
                marker = " ← active" if t.get("id") == LOOPS_TRANSACTIONAL_ID else ""
                print(f"    {t.get('id')}: {t.get('name', '?')}{marker}")
    except Exception as e:
        print(f"  ⚠ Cannot reach Loops: {e}")


# ── Pause sequence ────────────────────────────────────────────────────────

def pause_prospect(prospect_id: int) -> None:
    conn = get_connection()
    cur = conn.execute(
        "UPDATE messages SET status = 'skipped' WHERE prospect_id = ? AND channel = 'email' AND status = 'scheduled'",
        (prospect_id,),
    )
    conn.commit()
    print(f"Paused {cur.rowcount} scheduled emails for prospect {prospect_id}")
    conn.close()


# ── CLI ───────────────────────────────────────────────────────────────────

def main() -> None:
    p = argparse.ArgumentParser(description="Email sender via Loops.so (stage 6)")
    sub = p.add_subparsers(dest="command")

    sched = sub.add_parser("schedule", help="Schedule draft emails")
    sched.add_argument("--prospect-id", type=int, default=None)
    sched.add_argument("--start-date", type=str, default=None, help="YYYY-MM-DD")

    send = sub.add_parser("send", help="Send scheduled emails due today")
    send.add_argument("--dry-run", action="store_true")

    sub.add_parser("status", help="Show Loops account and email stats")

    pause = sub.add_parser("pause", help="Pause sequence for a prospect")
    pause.add_argument("prospect_id", type=int)

    args = p.parse_args()

    if args.command == "schedule":
        start = date.fromisoformat(args.start_date) if args.start_date else None
        schedule_emails(prospect_id=args.prospect_id, start_date=start)
    elif args.command == "send":
        send_due_emails(dry_run=args.dry_run)
    elif args.command == "status":
        check_status()
    elif args.command == "pause":
        pause_prospect(args.prospect_id)
    else:
        p.print_help()


if __name__ == "__main__":
    main()
