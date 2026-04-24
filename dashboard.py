"""
Avery GTM Outreach Dashboard (SQLite-backed)

Tracks outreach status for each prospect: message drafts, send status,
responses, and notes.

Usage:
  streamlit run dashboard.py
"""

import hashlib
from datetime import date, timedelta

import pandas as pd
import streamlit as st

from pipeline.db import get_connection, get_full_dashboard, init_db, update_message, update_prospect
from pipeline.email_sender import SEQUENCE_DAYS, schedule_emails, send_due_emails

def _auth_token() -> str:
    user = st.secrets["auth"]["username"]
    pw = st.secrets["auth"]["password"]
    return hashlib.sha256(f"{user}:{pw}".encode()).hexdigest()[:16]


def check_auth() -> bool:
    token = st.query_params.get("token", "")
    if token == _auth_token():
        st.session_state["authenticated"] = True
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
            st.query_params["token"] = _auth_token()
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


def _capture_stdout(func, *args, **kwargs):
    """Run a function, capture its stdout, return (result, output)."""
    import io
    import contextlib
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        result = func(*args, **kwargs)
    return result, buf.getvalue()


def _render_pipeline_section(conn) -> None:
    st.title("Avery GTM Dashboard")

    n_companies = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]

    # Step 1: Find Companies
    st.subheader("Step 1 — Find Companies")
    q1, q2, q3, q4 = st.columns([5, 2, 1, 2])
    with q1:
        query = st.text_input("query", value="startup in Europe with series A / series B funding and 50 to 200 employees 2025 2026", key="pipeline_query", label_visibility="collapsed")
    with q2:
        location = st.text_input("country", value="", placeholder="Country: NL, DE, FR", key="pipeline_location", label_visibility="collapsed")
    with q3:
        num = st.number_input("n", min_value=5, max_value=100, value=30, key="pipeline_num", label_visibility="collapsed")
    with q4:
        btn_find = st.button("Search & Filter", type="primary", use_container_width=True, key="btn_find")

    if btn_find:
        from pipeline.company_search import run_company_search
        from pipeline.company_filter import run_company_filter
        loc = location.strip() or None
        with st.status("Searching companies...", expanded=True) as status:
            st.write("Searching via Exa...")
            _, search_log = _capture_stdout(run_company_search, query=query, location=loc, num_results=num)
            st.code(search_log, language="text")
            st.write("Filtering by ICP...")
            _, filter_log = _capture_stdout(run_company_filter)
            st.code(filter_log, language="text")
            status.update(label="Search & filter complete!", state="complete")

    st.divider()

    # Step 2: Process Companies
    st.subheader("Step 2 — Process Companies")

    # Companies without prospects (need people search)
    unprocessed_rows = conn.execute("""
        SELECT id, name, domain, description, hq_country
        FROM companies c
        WHERE NOT EXISTS (SELECT 1 FROM prospects p WHERE p.company_id = c.id)
        ORDER BY c.id DESC
    """).fetchall()
    unprocessed = [dict(r) for r in unprocessed_rows]

    # Companies with prospects but missing messages (interrupted pipeline)
    incomplete_rows = conn.execute("""
        SELECT DISTINCT c.id, c.name, c.domain, c.description, c.hq_country
        FROM companies c
        JOIN prospects p ON p.company_id = c.id
        LEFT JOIN messages m ON p.id = m.prospect_id
        WHERE m.id IS NULL
        ORDER BY c.name
    """).fetchall()
    incomplete = [dict(r) for r in incomplete_rows]

    n_pending = len(unprocessed) + len(incomplete)

    if unprocessed:
        st.caption(f"{len(unprocessed)} new companies (need people search)")
        rows = []
        for c in unprocessed:
            domain = c["domain"] or ""
            link = f"https://{domain}" if domain else None
            rows.append({
                "Company": c["name"],
                "Website": link,
                "Description": (c["description"] or "")[:120],
                "Country": c["hq_country"] or "—",
            })
        df_queue = pd.DataFrame(rows)

        selected = st.dataframe(
            df_queue,
            use_container_width=True,
            hide_index=True,
            on_select="rerun",
            selection_mode="multi-row",
            column_config={
                "Website": st.column_config.LinkColumn("Website", display_text="Visit"),
            },
            key="company_queue_table",
        )

        sel_rows = selected.selection.rows if selected.selection else []
        if sel_rows:
            del_ids = [unprocessed[i]["id"] for i in sel_rows]
            if st.button(f"Delete {len(sel_rows)} selected", type="secondary", key="btn_del_companies"):
                placeholders = ",".join("?" * len(del_ids))
                conn.execute(f"DELETE FROM companies WHERE id IN ({placeholders})", del_ids)
                conn.commit()
                st.rerun()

    if incomplete:
        st.caption(f"{len(incomplete)} companies need to finish processing (email/messages)")
        inc_rows = []
        for c in incomplete:
            domain = c["domain"] or ""
            link = f"https://{domain}" if domain else None
            n_missing = conn.execute("""
                SELECT COUNT(*) FROM prospects p
                LEFT JOIN messages m ON p.id = m.prospect_id
                WHERE p.company_id = ? AND m.id IS NULL
            """, (c["id"],)).fetchone()[0]
            inc_rows.append({
                "Company": c["name"],
                "Website": link,
                "Pending": f"{n_missing} prospects",
                "Country": c["hq_country"] or "—",
            })

        inc_selected = st.dataframe(
            pd.DataFrame(inc_rows),
            use_container_width=True,
            hide_index=True,
            on_select="rerun",
            selection_mode="multi-row",
            column_config={
                "Website": st.column_config.LinkColumn("Website", display_text="Visit"),
            },
            key="incomplete_queue_table",
        )

        inc_sel_rows = inc_selected.selection.rows if inc_selected.selection else []
        if inc_sel_rows:
            del_ids = [incomplete[i]["id"] for i in inc_sel_rows]
            if st.button(f"Delete {len(inc_sel_rows)} selected", type="secondary", key="btn_del_incomplete"):
                placeholders = ",".join("?" * len(del_ids))
                conn.execute(f"DELETE FROM messages WHERE prospect_id IN (SELECT id FROM prospects WHERE company_id IN ({placeholders}))", del_ids)
                conn.execute(f"DELETE FROM prospects WHERE company_id IN ({placeholders})", del_ids)
                conn.execute(f"DELETE FROM companies WHERE id IN ({placeholders})", del_ids)
                conn.commit()
                st.rerun()

    if n_pending == 0:
        st.caption("All companies processed.")

    btn_process = st.button(
        f"Run Pipeline ({n_pending})" if n_pending > 0 else "All done",
        type="primary",
        disabled=(n_pending == 0),
        use_container_width=True,
        key="btn_process",
    )

    if btn_process:
        from pipeline.people_search import run_people
        from pipeline.people_filter import run_filter
        from pipeline.people_email import run_email_enrichment
        from pipeline.reachout import run_reachout
        with st.status("Processing companies...", expanded=True) as status:
            st.write("Stage 2 — People search...")
            _, log2 = _capture_stdout(run_people)
            st.code(log2, language="text")
            st.write("Stage 3 — ICP filter...")
            _, log3 = _capture_stdout(run_filter)
            st.code(log3, language="text")
            st.write("Stage 4 — Email enrichment...")
            _, log4 = _capture_stdout(run_email_enrichment)
            st.code(log4, language="text")
            st.write("Stage 5 — Message generation...")
            _, log5 = _capture_stdout(run_reachout)
            st.code(log5, language="text")
            status.update(label="Pipeline complete!", state="complete")

    st.divider()

    # Step 3: Send Emails
    st.subheader("Step 3 — Send Emails")

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
    n_email_sent = conn.execute(
        "SELECT COUNT(*) FROM messages WHERE channel='email' AND status='sent'",
    ).fetchone()[0]

    e1, e2, e3, e4, e5, e6 = st.columns([1, 1, 1, 1, 2, 2])
    e1.metric("Draft", n_draft)
    e2.metric("Scheduled", n_scheduled)
    e3.metric("Due Today", n_due)
    e4.metric("Sent", n_email_sent)
    with e5:
        if st.button("Schedule All Drafts", disabled=(n_draft == 0), use_container_width=True, key="btn_schedule_all"):
            count = schedule_emails()
            st.success(f"Scheduled {count} emails")
            st.rerun()
    with e6:
        if st.button("Send Due Emails", type="primary", disabled=(n_due == 0), use_container_width=True, key="btn_send_due"):
            sent_count = send_due_emails(dry_run=False)
            if sent_count > 0:
                st.success(f"Sent {sent_count} emails!")
            else:
                st.warning("No emails sent — check Loops config")
            st.rerun()

    # Summary metrics
    n_prospects = conn.execute("SELECT COUNT(*) FROM prospects").fetchone()[0]
    n_with_email = conn.execute(
        "SELECT COUNT(*) FROM prospects WHERE email IS NOT NULL AND email != ''"
    ).fetchone()[0]
    n_total_sent = conn.execute("SELECT COUNT(*) FROM messages WHERE status = 'sent'").fetchone()[0]
    n_total_msgs = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    n_replied = conn.execute(
        "SELECT COUNT(*) FROM prospects WHERE response IN ('replied', 'meeting_booked')"
    ).fetchone()[0]

    st.divider()

    st.subheader("Overview")
    p1, p2, p3, p4 = st.columns(4)
    p1.metric("Companies", n_companies)
    p2.metric("Prospects", f"{n_prospects} ({n_with_email} email)")
    p3.metric("Sent / Total", f"{n_total_sent} / {n_total_msgs}")
    p4.metric("Replied", n_replied)


def main() -> None:
    st.set_page_config(page_title="Avery GTM Dashboard", layout="wide")

    if not check_auth():
        return

    conn = get_connection()
    init_db(conn)

    # ── Pipeline controls (top) ─────────────────────────────────────────
    _render_pipeline_section(conn)

    data = get_full_dashboard(conn)

    if not data:
        st.info("No prospects yet. Use the Pipeline section above to find companies and people.")
        return

    companies = sorted(set(d["company_name"] for d in data))
    types = sorted(set(d["prospect_type"] for d in data))

    def _email_status(d):
        em = [m for m in d["messages"] if m["channel"] == "email"]
        if not em:
            return "no email"
        statuses = set(m["status"] for m in em)
        if "sent" in statuses:
            return "sent"
        if "scheduled" in statuses:
            return "scheduled"
        return "draft"

    email_statuses = sorted(set(_email_status(d) for d in data))

    search = st.text_input("Search by name, role, or company", key="search", placeholder="Type to search...")
    sel_companies = st.pills("Company", companies, selection_mode="multi", default=companies, key="sel_companies")
    sel_types = st.pills("Type", types, selection_mode="multi", default=types, format_func=lambda t: TYPE_LABELS.get(t, t), key="sel_types")
    sel_response = st.pills("Response", RESPONSE_OPTIONS, selection_mode="multi", default=RESPONSE_OPTIONS, key="sel_response")
    sel_email_status = st.pills("Email Status", email_statuses, selection_mode="multi", default=email_statuses, key="sel_email_status")

    filtered = [
        d for d in data
        if d["company_name"] in sel_companies
        and d["prospect_type"] in sel_types
        and d.get("response", "none") in sel_response
        and _email_status(d) in sel_email_status
    ]

    if search:
        q = search.lower()
        filtered = [
            d for d in filtered
            if q in d.get("name", "").lower()
            or q in d.get("role", "").lower()
            or q in d.get("company_name", "").lower()
        ]

    if not filtered:
        st.info("No prospects match current filters.")
        return

    df = _build_overview_df(filtered)

    prospect_selected = st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="multi-row",
        column_config={
            "Email": st.column_config.TextColumn("Email", width="medium"),
            "Response": st.column_config.TextColumn("Response", width="small"),
        },
        key="prospect_table",
    )

    # Schedule button for selected/all draft prospects
    schedulable_indices = [
        i for i, d in enumerate(filtered)
        if d.get("email")
        and d.get("response", "none") not in ("replied", "meeting_booked", "rejected", "bounced")
        and any(m["channel"] == "email" and m["status"] == "draft" for m in d["messages"])
    ]
    sel_rows = prospect_selected.selection.rows if prospect_selected.selection else []
    sched_targets = [i for i in sel_rows if i in schedulable_indices] if sel_rows else schedulable_indices

    if sched_targets:
        label = f"Schedule {len(sched_targets)} Selected" if sel_rows else f"Schedule All Drafts ({len(sched_targets)})"
        if st.button(label, type="primary", use_container_width=True, key="btn_batch_schedule"):
            scheduled_count = 0
            for i in sched_targets:
                d = filtered[i]
                base = date.today()
                for m in d["messages"]:
                    if m["channel"] == "email" and m["status"] == "draft":
                        offset = SEQUENCE_DAYS.get(m["sequence_num"], 0)
                        send_date = base + timedelta(days=offset)
                        update_message(conn, m["id"],
                                       status="scheduled",
                                       scheduled_date=send_date.isoformat())
                        scheduled_count += 1
            st.success(f"Scheduled {scheduled_count} emails for {len(sched_targets)} prospects")
            st.rerun()

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
