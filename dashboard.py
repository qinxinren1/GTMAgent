"""
Avery GTM Outreach Dashboard

Tracks outreach status for each prospect: message drafts, send status,
responses, and notes.

Usage:
  streamlit run dashboard.py
"""

import json
import os

import streamlit as st

_REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
_OUTPUT_BASE = os.environ.get("AVERY_OUTPUT_DIR", os.path.join(_REPO_ROOT, "output"))
DASHBOARD_JSON = os.path.join(_OUTPUT_BASE, "dashboard_state.json")

STATUS_OPTIONS = ["draft", "scheduled", "sent", "skipped"]
RESPONSE_OPTIONS = ["none", "accepted", "replied", "meeting_booked", "rejected", "bounced"]
MSG_KEYS = ["linkedin_msg1", "linkedin_msg2", "linkedin_msg3", "email_msg1", "email_msg2", "email_msg3"]
MSG_LABELS = {
    "linkedin_msg1": "LI Connect (Day 0)",
    "linkedin_msg2": "LI Nudge (Day 5)",
    "linkedin_msg3": "LI Step back (Day 18)",
    "email_msg1": "Email Opener (Day 2)",
    "email_msg2": "Email Nudge (Day 10)",
    "email_msg3": "Email Step back (Day 25)",
}


def load_data() -> list[dict]:
    if not os.path.exists(DASHBOARD_JSON):
        return []
    with open(DASHBOARD_JSON, encoding="utf-8") as f:
        return json.load(f)


def save_data(data: list[dict]) -> None:
    with open(DASHBOARD_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def main() -> None:
    st.set_page_config(page_title="Avery GTM Dashboard", layout="wide")
    st.title("Avery GTM Outreach Dashboard")

    data = load_data()
    if not data:
        st.warning("No data found. Run `python -m pipeline.reachout` first.")
        return

    # --- Sidebar filters ---
    st.sidebar.header("Filters")
    companies = sorted(set(d["company"] for d in data))
    sel_companies = st.sidebar.multiselect("Company", companies, default=companies)

    chains = sorted(set(d.get("chain", "") for d in data))
    sel_chains = st.sidebar.multiselect("Chain", chains, default=chains)

    sel_response = st.sidebar.multiselect("Response", RESPONSE_OPTIONS, default=RESPONSE_OPTIONS)

    filtered = [
        d for d in data
        if d["company"] in sel_companies
        and d.get("chain", "") in sel_chains
        and d.get("response", "none") in sel_response
    ]

    # --- Summary metrics ---
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Prospects", len(filtered))

    n_with_email = sum(1 for d in filtered if d.get("email"))
    col2.metric("With Email", n_with_email)

    all_statuses = []
    for d in filtered:
        for k in MSG_KEYS:
            all_statuses.append(d.get("status", {}).get(k, "draft"))
    n_sent = sum(1 for s in all_statuses if s == "sent")
    col3.metric("Messages Sent", n_sent)

    n_replied = sum(1 for d in filtered if d.get("response") in ("replied", "meeting_booked"))
    col4.metric("Replied / Meeting", n_replied)

    n_accepted = sum(1 for d in filtered if d.get("response") == "accepted")
    col5.metric("Accepted", n_accepted)

    st.divider()

    # --- Overview table ---
    st.subheader("Prospect Overview")
    header_cols = st.columns([2, 2, 1.5, 1, 1.5, 1.5, 1, 1, 1])
    for col, label in zip(header_cols, ["Name", "Role", "Company", "Chain", "Email", "LinkedIn", "LI", "Email", "Response"]):
        col.markdown(f"**{label}**")
    for d in filtered:
        li_status = [d.get("status", {}).get(k, "draft") for k in MSG_KEYS[:3]]
        em_status = [d.get("status", {}).get(k, "draft") for k in MSG_KEYS[3:]]
        row = st.columns([2, 2, 1.5, 1, 1.5, 1.5, 1, 1, 1])
        row[0].write(d.get("name", ""))
        row[1].write(d.get("role", ""))
        row[2].write(d.get("company", ""))
        row[3].write(d.get("chain", ""))
        row[4].write(d.get("email", "") or "—")
        li_url = d.get("linkedin", "")
        row[5].markdown(f"[Profile]({li_url})" if li_url else "—")
        row[6].write(f"{sum(1 for s in li_status if s == 'sent')}/3")
        row[7].write(f"{sum(1 for s in em_status if s == 'sent')}/3")
        row[8].write(d.get("response", "none"))

    st.divider()

    # --- Per-prospect detail ---
    st.subheader("Prospect Details")
    names = [f"{d['name']} ({d['company']})" for d in filtered]
    if not names:
        st.info("No prospects match current filters.")
        return

    selected_name = st.selectbox("Select prospect", names)
    idx = names.index(selected_name)
    prospect = filtered[idx]
    original_idx = data.index(prospect)

    col_info, col_status = st.columns([1, 1])

    with col_info:
        st.markdown(f"**{prospect['name']}**")
        st.markdown(f"Role: {prospect['role']}")
        st.markdown(f"Company: {prospect['company']} ({prospect['domain']})")
        st.markdown(f"City: {prospect.get('city', '—')}")
        st.markdown(f"Chain: `{prospect.get('chain', '—')}`")
        if prospect.get("linkedin"):
            st.markdown(f"[LinkedIn Profile]({prospect['linkedin']})")
        st.markdown(f"Email: `{prospect.get('email') or '—'}`")

    with col_status:
        new_response = st.selectbox(
            "Response status",
            RESPONSE_OPTIONS,
            index=RESPONSE_OPTIONS.index(prospect.get("response", "none")),
            key=f"resp_{original_idx}",
        )
        new_notes = st.text_area(
            "Notes",
            value=prospect.get("notes", ""),
            key=f"notes_{original_idx}",
        )

    st.divider()

    # --- Messages ---
    messages = prospect.get("messages", {})
    status = prospect.get("status", {})

    li_tab, em_tab = st.tabs(["LinkedIn Messages", "Email Messages"])

    with li_tab:
        for key in MSG_KEYS[:3]:
            label = MSG_LABELS[key]
            msg = messages.get(key, "")
            current_status = status.get(key, "draft")

            st.markdown(f"#### {label}")
            status_col, copy_col = st.columns([3, 1])
            with status_col:
                st.selectbox(
                    "Status",
                    STATUS_OPTIONS,
                    index=STATUS_OPTIONS.index(current_status),
                    key=f"status_{original_idx}_{key}",
                    label_visibility="collapsed",
                )
            with copy_col:
                st.code(f"{len(msg)} chars", language=None)
            st.text_area(
                label,
                value=msg,
                height=100,
                key=f"msg_{original_idx}_{key}",
                label_visibility="collapsed",
            )
            st.divider()

    with em_tab:
        for key in MSG_KEYS[3:]:
            label = MSG_LABELS[key]
            msg = messages.get(key, "")
            current_status = status.get(key, "draft")

            subject = ""
            body = msg
            if msg.startswith("Subject:"):
                parts = msg.split("\n\n", 1)
                subject = parts[0].replace("Subject: ", "")
                body = parts[1] if len(parts) > 1 else ""

            st.markdown(f"#### {label}")
            status_col, subj_col = st.columns([1, 3])
            with status_col:
                st.selectbox(
                    "Status",
                    STATUS_OPTIONS,
                    index=STATUS_OPTIONS.index(current_status),
                    key=f"status_{original_idx}_{key}",
                    label_visibility="collapsed",
                )
            with subj_col:
                st.text_input(
                    "Subject",
                    value=subject,
                    key=f"subj_{original_idx}_{key}",
                )
            st.text_area(
                label,
                value=body,
                height=140,
                key=f"msg_{original_idx}_{key}",
                label_visibility="collapsed",
            )
            st.divider()

    st.divider()

    if st.button("Save Changes", type="primary"):
        for key in MSG_KEYS:
            new_status_val = st.session_state.get(f"status_{original_idx}_{key}")
            if new_status_val:
                data[original_idx].setdefault("status", {})[key] = new_status_val
            new_msg_val = st.session_state.get(f"msg_{original_idx}_{key}")
            if new_msg_val is not None:
                if key in MSG_KEYS[3:]:
                    new_subj = st.session_state.get(f"subj_{original_idx}_{key}", "")
                    data[original_idx].setdefault("messages", {})[key] = f"Subject: {new_subj}\n\n{new_msg_val}"
                else:
                    data[original_idx].setdefault("messages", {})[key] = new_msg_val
        data[original_idx]["response"] = new_response
        data[original_idx]["notes"] = new_notes
        save_data(data)
        st.success("Saved!")
        st.rerun()


if __name__ == "__main__":
    main()
