"""
Avery GTM Outreach Dashboard (SQLite-backed)

Tracks outreach status for each prospect: message drafts, send status,
responses, and notes.

Usage:
  streamlit run dashboard.py
"""

import os
from datetime import date, timedelta

import pandas as pd
import streamlit as st

from pipeline.db import get_connection, get_full_dashboard, init_db, update_message, update_prospect
from pipeline.email_sender import SEQUENCE_DAYS, schedule_emails, send_due_emails

def check_auth() -> bool:
    if st.session_state.get("authenticated"):
        return True

    st.title("Avery GTM Dashboard")
    st.markdown("Please log in to continue.")

    with st.form("login_form"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Log in", type="primary", use_container_width=True)

    if submitted:
        correct_user = st.secrets["auth"]["username"]
        correct_pass = st.secrets["auth"]["password"]
        if username == correct_user and password == correct_pass:
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("Invalid username or password")

    return False


STATUS_OPTIONS = ["draft", "scheduled", "sent", "skipped"]
RESPONSE_OPTIONS = ["none", "accepted", "replied", "meeting_booked", "rejected", "bounced"]
TYPE_LABELS = {"inhouse": "In-house", "rpo": "RPO / Freelance", "agency": "Agency"}

MSG_LABELS = {
    ("linkedin", 1): "LI Connect (Day 0)",
    ("linkedin", 2): "LI Nudge (Day 5)",
    ("linkedin", 3): "LI Step back (Day 18)",
    ("email", 1): "Email Opener (Day 2)",
    ("email", 2): "Email Nudge (Day 10)",
    ("email", 3): "Email Step back (Day 25)",
}


def _status_icon(status: str) -> str:
    return {"draft": "📝", "scheduled": "📅", "sent": "✅", "skipped": "⏭️"}.get(status, "")


def _build_overview_df(data: list[dict]) -> pd.DataFrame:
    rows = []
    for d in data:
        li_msgs = [m for m in d["messages"] if m["channel"] == "linkedin"]
        em_msgs = [m for m in d["messages"] if m["channel"] == "email"]
        li_sent = sum(1 for m in li_msgs if m["status"] == "sent")
        em_sent = sum(1 for m in em_msgs if m["status"] == "sent")
        rows.append({
            "Name": d.get("name", ""),
            "Role": d.get("role", ""),
            "Company": d.get("company_name", ""),
            "Type": TYPE_LABELS.get(d.get("prospect_type", ""), d.get("prospect_type", "")),
            "Email": d.get("email") or "—",
            "LI": f"{li_sent}/{len(li_msgs)}",
            "Email Sent": f"{em_sent}/{len(em_msgs)}" if em_msgs else "—",
            "Response": d.get("response", "none"),
        })
    return pd.DataFrame(rows)


def main() -> None:
    st.set_page_config(page_title="Avery GTM Dashboard", layout="wide")

    if not check_auth():
        return

    conn = get_connection()
    init_db(conn)
    data = get_full_dashboard(conn)

    if not data:
        st.warning("No data found. Run the pipeline first.")
        return

    # ── Sidebar ──────────────────────────────────────────────────────────
    with st.sidebar:
        st.header("🎯 Filters")
        companies = sorted(set(d["company_name"] for d in data))
        sel_companies = st.multiselect("Company", companies, default=companies)

        types = sorted(set(d["prospect_type"] for d in data))
        sel_types = st.multiselect(
            "Type", types, default=types,
            format_func=lambda t: TYPE_LABELS.get(t, t),
        )
        sel_response = st.multiselect("Response", RESPONSE_OPTIONS, default=RESPONSE_OPTIONS)

        st.divider()
        st.header("📧 Email Controls")

        n_draft = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE channel='email' AND status='draft'",
        ).fetchone()[0]
        n_scheduled = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE channel='email' AND status='scheduled'",
        ).fetchone()[0]
        n_due = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE channel='email' AND status='scheduled' AND scheduled_date <= ?",
            (date.today().isoformat(),),
        ).fetchone()[0]
        n_sent = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE channel='email' AND status='sent'",
        ).fetchone()[0]

        c1, c2 = st.columns(2)
        c1.metric("Draft", n_draft)
        c2.metric("Scheduled", n_scheduled)
        c1.metric("Due Today", n_due)
        c2.metric("Sent", n_sent)

        if n_draft > 0 and st.button("📅 Schedule All Drafts", use_container_width=True):
            count = schedule_emails()
            st.success(f"Scheduled {count} emails")
            st.rerun()

        if n_due > 0:
            if st.button("🚀 Send Due Emails", type="primary", use_container_width=True):
                sent = send_due_emails(dry_run=False)
                if sent > 0:
                    st.success(f"Sent {sent} emails!")
                else:
                    st.warning("No emails sent — check Loops config")
                st.rerun()

    # ── Filter data ──────────────────────────────────────────────────────
    filtered = [
        d for d in data
        if d["company_name"] in sel_companies
        and d["prospect_type"] in sel_types
        and d.get("response", "none") in sel_response
    ]

    # ── Header metrics ───────────────────────────────────────────────────
    st.title("Avery GTM Dashboard")

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Prospects", len(filtered))
    m2.metric("With Email", sum(1 for d in filtered if d.get("email")))
    total_sent = sum(1 for d in filtered for m in d["messages"] if m["status"] == "sent")
    total_msgs = sum(len(d["messages"]) for d in filtered)
    m3.metric("Sent / Total", f"{total_sent} / {total_msgs}")
    m4.metric("Replied", sum(1 for d in filtered if d.get("response") in ("replied", "meeting_booked")))
    m5.metric("Accepted", sum(1 for d in filtered if d.get("response") == "accepted"))

    # ── Overview table ───────────────────────────────────────────────────
    st.subheader("Prospect Overview")

    if not filtered:
        st.info("No prospects match current filters.")
        return

    search = st.text_input("🔍 Search by name, role, or company", key="search")
    display = filtered
    if search:
        q = search.lower()
        display = [
            d for d in filtered
            if q in d.get("name", "").lower()
            or q in d.get("role", "").lower()
            or q in d.get("company_name", "").lower()
        ]

    # Batch schedule
    schedulable = [
        d for d in display
        if d.get("email")
        and d.get("response", "none") not in ("replied", "meeting_booked", "rejected", "bounced")
        and any(m["channel"] == "email" and m["status"] == "draft" for m in d["messages"])
    ]

    if schedulable:
        with st.expander(f"📅 Batch Schedule ({len(schedulable)} prospects with draft emails)", expanded=False):
            options = [f"{d['name']} — {d['company_name']}" for d in schedulable]
            selected = st.multiselect(
                "Select prospects to schedule",
                options,
                default=options,
                key="batch_select",
            )
            _, b2 = st.columns([3, 1])
            with b2:
                if st.button("📅 Schedule Selected", type="primary", use_container_width=True):
                    scheduled_count = 0
                    for label, d in zip(options, schedulable):
                        if label not in selected:
                            continue
                        base = date.today()
                        for m in d["messages"]:
                            if m["channel"] == "email" and m["status"] == "draft":
                                offset = SEQUENCE_DAYS.get(m["sequence_num"], 0)
                                send_date = base + timedelta(days=offset)
                                update_message(conn, m["id"],
                                               status="scheduled",
                                               scheduled_date=send_date.isoformat())
                                scheduled_count += 1
                    st.success(f"Scheduled {scheduled_count} emails for {len(selected)} prospects")
                    st.rerun()

    df = _build_overview_df(display)

    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Email": st.column_config.TextColumn("Email", width="medium"),
            "Response": st.column_config.TextColumn("Response", width="small"),
        },
    )

    # ── Prospect detail ──────────────────────────────────────────────────
    st.divider()
    h1, h2 = st.columns([4, 1])
    h1.subheader("Prospect Details")
    save_clicked = h2.button("💾 Save", type="primary", use_container_width=True)

    names = [f"{d['name']} — {d['company_name']}" for d in filtered]
    if not names:
        return

    selected_name = st.selectbox("Select prospect", names)
    idx = names.index(selected_name)
    prospect = filtered[idx]
    prospect_id = prospect["id"]

    # Prospect info
    li_url = prospect.get("linkedin_url") or ""
    name_display = f"[{prospect['name']}]({li_url})" if li_url else prospect['name']
    st.markdown(f"### {name_display}")
    company_url = prospect.get("domain") or ""
    if company_url:
        st.markdown(f"**{prospect.get('role', '')}** at **[{prospect['company_name']}](https://{company_url})**")
    else:
        st.markdown(f"**{prospect.get('role', '')}** at **{prospect['company_name']}**")

    ptype = TYPE_LABELS.get(prospect.get("prospect_type", ""), prospect.get("prospect_type", ""))
    email_display = prospect.get("email") or "—"

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(f"**Type:** {ptype}")
    c2.markdown(f"**City:** {prospect.get('city') or '—'}")
    c3.markdown(f"**Funding:** {prospect.get('latest_round') or '—'} · {prospect.get('employees') or '?'} emp")
    c4.markdown(f"**Email:** `{email_display}`")

    # Response & notes
    r1, r2 = st.columns([1, 2])
    with r1:
        new_response = st.selectbox(
            "Response status",
            RESPONSE_OPTIONS,
            index=RESPONSE_OPTIONS.index(prospect.get("response", "none")),
            key=f"resp_{prospect_id}",
        )
    with r2:
        new_notes = st.text_input(
            "Notes",
            value=prospect.get("notes", "") or "",
            key=f"notes_{prospect_id}",
        )

    # ── Messages ─────────────────────────────────────────────────────────
    li_msgs = [m for m in prospect["messages"] if m["channel"] == "linkedin"]
    em_msgs = [m for m in prospect["messages"] if m["channel"] == "email"]

    li_tab, em_tab = st.tabs([
        f"LinkedIn ({sum(1 for m in li_msgs if m['status'] == 'sent')}/{len(li_msgs)} sent)",
        f"Email ({sum(1 for m in em_msgs if m['status'] == 'sent')}/{len(em_msgs)} sent)",
    ])

    with li_tab:
        for m in li_msgs:
            seq = m["sequence_num"]
            label = MSG_LABELS.get(("linkedin", seq), f"LI Msg {seq}")
            icon = _status_icon(m.get("status", "draft"))
            with st.expander(f"{icon} #{seq} {label} — {m.get('status', 'draft')}", expanded=(m.get("status") == "draft")):
                st.selectbox(
                    "Status",
                    STATUS_OPTIONS,
                    index=STATUS_OPTIONS.index(m.get("status", "draft")),
                    key=f"mstatus_{m['id']}",
                )
                st.text_area(
                    "Content",
                    value=m.get("content", ""),
                    height=100,
                    key=f"mcontent_{m['id']}",
                    label_visibility="collapsed",
                )
                st.caption(f"{len(m.get('content', ''))} chars")

    with em_tab:
        if not em_msgs:
            st.info("No email messages — LinkedIn only for this prospect.")
        for m in em_msgs:
            seq = m["sequence_num"]
            label = MSG_LABELS.get(("email", seq), f"Email {seq}")
            icon = _status_icon(m.get("status", "draft"))
            sched_date = m.get("scheduled_date") or ""
            date_display = f" · 📅 {sched_date}" if sched_date else ""
            header = f"{icon} #{seq} {label} — {m.get('status', 'draft')}{date_display}"
            with st.expander(header, expanded=(m.get("status") == "draft")):
                st.selectbox(
                    "Status",
                    STATUS_OPTIONS,
                    index=STATUS_OPTIONS.index(m.get("status", "draft")),
                    key=f"mstatus_{m['id']}",
                )
                if sched_date:
                    st.caption(f"Scheduled for: {sched_date}")
                st.text_input(
                    "Subject",
                    value=m.get("subject", ""),
                    key=f"msubj_{m['id']}",
                )
                st.text_area(
                    "Body",
                    value=m.get("content", ""),
                    height=140,
                    key=f"mcontent_{m['id']}",
                    label_visibility="collapsed",
                )

    # ── Save ─────────────────────────────────────────────────────────────
    if save_clicked:
        update_prospect(conn, prospect_id, response=new_response, notes=new_notes)

        emails = sorted(
            [m for m in prospect["messages"] if m["channel"] == "email"],
            key=lambda m: m["sequence_num"],
        )
        em_by_seq = {m["sequence_num"]: m for m in emails}

        # 1) Reply cancels all future scheduled emails
        old_response = prospect.get("response", "none")
        if new_response in ("replied", "meeting_booked") and old_response not in ("replied", "meeting_booked"):
            cancelled = 0
            for m in emails:
                if m["status"] == "scheduled":
                    update_message(conn, m["id"], status="skipped")
                    cancelled += 1
            if cancelled:
                st.toast(f"Auto-cancelled {cancelled} follow-up email(s)")
            # Save non-email messages and skip rest of email logic
            for m in prospect["messages"]:
                if m["channel"] != "email":
                    mid = m["id"]
                    update_message(conn, mid,
                                   status=st.session_state.get(f"mstatus_{mid}", m["status"]),
                                   content=st.session_state.get(f"mcontent_{mid}", m["content"]))
            st.success("Changes saved!")
            st.rerun()

        # 2) Detect schedule trigger: any email draft → scheduled
        trigger_seq = None
        for m in emails:
            new_st = st.session_state.get(f"mstatus_{m['id']}", m["status"])
            if m["status"] == "draft" and new_st == "scheduled":
                trigger_seq = m["sequence_num"]
                break

        # 3) Detect revert trigger: any email scheduled → draft
        revert_seq = None
        if trigger_seq is None:
            for m in emails:
                new_st = st.session_state.get(f"mstatus_{m['id']}", m["status"])
                if m["status"] == "scheduled" and new_st == "draft":
                    revert_seq = m["sequence_num"]
                    break

        auto_handled: set[int] = set()

        if trigger_seq is not None:
            # Calculate base date: today minus the trigger email's offset
            trigger_day = SEQUENCE_DAYS.get(trigger_seq, 0)
            base = date.today() - timedelta(days=trigger_day)
            for seq, fm in em_by_seq.items():
                if fm["status"] in ("sent", "skipped"):
                    continue
                offset = SEQUENCE_DAYS.get(seq, 0)
                send_date = base + timedelta(days=offset)
                if send_date < date.today():
                    send_date = date.today()
                update_message(conn, fm["id"],
                               status="scheduled",
                               scheduled_date=send_date.isoformat(),
                               content=st.session_state.get(f"mcontent_{fm['id']}", fm["content"]),
                               subject=st.session_state.get(f"msubj_{fm['id']}", fm.get("subject", "")))
                auto_handled.add(fm["id"])

        elif revert_seq is not None:
            for seq, fm in em_by_seq.items():
                if fm["status"] in ("sent", "skipped"):
                    continue
                update_message(conn, fm["id"],
                               status="draft",
                               scheduled_date="",
                               content=st.session_state.get(f"mcontent_{fm['id']}", fm["content"]),
                               subject=st.session_state.get(f"msubj_{fm['id']}", fm.get("subject", "")))
                auto_handled.add(fm["id"])

        # Save remaining messages not already handled
        for m in prospect["messages"]:
            mid = m["id"]
            if mid in auto_handled:
                continue
            new_status = st.session_state.get(f"mstatus_{mid}", m["status"])
            new_content = st.session_state.get(f"mcontent_{mid}", m["content"])
            updates: dict = {"status": new_status, "content": new_content}
            if m["channel"] == "email":
                updates["subject"] = st.session_state.get(f"msubj_{mid}", m.get("subject", ""))
            update_message(conn, mid, **updates)

        st.success("Changes saved!")
        st.rerun()


if __name__ == "__main__":
    main()
