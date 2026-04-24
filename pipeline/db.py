"""
SQLite database for the Avery GTM pipeline.

Provides schema creation and CRUD helpers for companies, prospects, and messages.

Usage:
    from pipeline.db import get_connection, init_db
    conn = get_connection()
    init_db(conn)
"""

from __future__ import annotations

import os
import sqlite3
from typing import Any

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_OUTPUT_BASE = os.environ.get("AVERY_OUTPUT_DIR", os.path.join(_REPO_ROOT, "output"))
DB_PATH = os.path.join(_OUTPUT_BASE, "avery_gtm.db")

SCHEMA = """\
CREATE TABLE IF NOT EXISTS companies (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT UNIQUE NOT NULL,
    domain          TEXT,
    url             TEXT,
    description     TEXT,
    founded_year    INTEGER,
    hq_city         TEXT,
    hq_country      TEXT,
    employees       INTEGER,
    funding_total   INTEGER,
    latest_round    TEXT,
    latest_amount   INTEGER,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS prospects (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id      INTEGER NOT NULL REFERENCES companies(id),
    external_id     TEXT,
    name            TEXT NOT NULL,
    first_name      TEXT,
    last_name       TEXT,
    role            TEXT,
    prospect_type   TEXT DEFAULT 'inhouse',
    city            TEXT,
    country         TEXT,
    linkedin_url    TEXT,
    email           TEXT,
    email_status    TEXT,
    response        TEXT DEFAULT 'none',
    notes           TEXT DEFAULT '',
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS messages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    prospect_id     INTEGER NOT NULL REFERENCES prospects(id),
    channel         TEXT NOT NULL,
    sequence_num    INTEGER NOT NULL,
    subject         TEXT,
    content         TEXT NOT NULL,
    status          TEXT DEFAULT 'draft',
    scheduled_date  DATE,
    sent_at         TIMESTAMP,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS send_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id      INTEGER NOT NULL REFERENCES messages(id),
    ses_message_id  TEXT,
    status          TEXT NOT NULL,
    error           TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_prospect_ext ON prospects(external_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_message ON messages(prospect_id, channel, sequence_num);
"""


def get_connection(path: str = DB_PATH) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


# ── Company CRUD ──────────────────────────────────────────────────────────

def upsert_company(conn: sqlite3.Connection, **kw: Any) -> int:
    conn.execute(
        """INSERT INTO companies (name, domain, url, description, founded_year,
               hq_city, hq_country, employees, funding_total, latest_round, latest_amount)
           VALUES (:name, :domain, :url, :description, :founded_year,
               :hq_city, :hq_country, :employees, :funding_total, :latest_round, :latest_amount)
           ON CONFLICT(name) DO UPDATE SET
               domain=excluded.domain, url=excluded.url, description=excluded.description,
               founded_year=excluded.founded_year, hq_city=excluded.hq_city,
               hq_country=excluded.hq_country, employees=excluded.employees,
               funding_total=excluded.funding_total, latest_round=excluded.latest_round,
               latest_amount=excluded.latest_amount""",
        kw,
    )
    conn.commit()
    row = conn.execute("SELECT id FROM companies WHERE name=?", (kw["name"],)).fetchone()
    return row["id"]


def get_companies(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute("SELECT * FROM companies ORDER BY name").fetchall()
    return [dict(r) for r in rows]


# ── Prospect CRUD ─────────────────────────────────────────────────────────

def upsert_prospect(conn: sqlite3.Connection, **kw: Any) -> int:
    conn.execute(
        """INSERT INTO prospects (company_id, external_id, name, first_name, last_name,
               role, prospect_type, city, country, linkedin_url, email, email_status,
               response, notes)
           VALUES (:company_id, :external_id, :name, :first_name, :last_name,
               :role, :prospect_type, :city, :country, :linkedin_url, :email, :email_status,
               :response, :notes)
           ON CONFLICT(external_id) DO UPDATE SET
               name=excluded.name, first_name=excluded.first_name, last_name=excluded.last_name,
               role=excluded.role, prospect_type=excluded.prospect_type,
               city=excluded.city, country=excluded.country,
               linkedin_url=excluded.linkedin_url, email=excluded.email,
               email_status=excluded.email_status""",
        kw,
    )
    conn.commit()
    row = conn.execute("SELECT id FROM prospects WHERE external_id=?", (kw["external_id"],)).fetchone()
    return row["id"]


def get_prospects(
    conn: sqlite3.Connection,
    company_id: int | None = None,
) -> list[dict[str, Any]]:
    if company_id is not None:
        rows = conn.execute(
            "SELECT * FROM prospects WHERE company_id=? ORDER BY name", (company_id,)
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM prospects ORDER BY name").fetchall()
    return [dict(r) for r in rows]


def update_prospect(conn: sqlite3.Connection, prospect_id: int, **kw: Any) -> None:
    sets = ", ".join(f"{k}=:{k}" for k in kw)
    kw["id"] = prospect_id
    conn.execute(f"UPDATE prospects SET {sets} WHERE id=:id", kw)
    conn.commit()


# ── Message CRUD ──────────────────────────────────────────────────────────

def upsert_message(conn: sqlite3.Connection, **kw: Any) -> int:
    conn.execute(
        """INSERT INTO messages (prospect_id, channel, sequence_num, subject, content, status)
           VALUES (:prospect_id, :channel, :sequence_num, :subject, :content, :status)
           ON CONFLICT(prospect_id, channel, sequence_num) DO UPDATE SET
               subject=excluded.subject, content=excluded.content, status=excluded.status""",
        kw,
    )
    conn.commit()
    row = conn.execute(
        "SELECT id FROM messages WHERE prospect_id=:prospect_id AND channel=:channel AND sequence_num=:sequence_num",
        kw,
    ).fetchone()
    return row["id"]


def get_messages(
    conn: sqlite3.Connection,
    prospect_id: int | None = None,
) -> list[dict[str, Any]]:
    if prospect_id is not None:
        rows = conn.execute(
            "SELECT * FROM messages WHERE prospect_id=? ORDER BY channel, sequence_num",
            (prospect_id,),
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM messages ORDER BY prospect_id, channel, sequence_num").fetchall()
    return [dict(r) for r in rows]


def update_message(conn: sqlite3.Connection, message_id: int, **kw: Any) -> None:
    sets = ", ".join(f"{k}=:{k}" for k in kw)
    kw["id"] = message_id
    conn.execute(f"UPDATE messages SET {sets} WHERE id=:id", kw)
    conn.commit()


# ── Pipeline queries ─────────────────────────────────────────────────────

def get_companies_without_prospects(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute("""
        SELECT c.* FROM companies c
        LEFT JOIN prospects p ON c.id = p.company_id
        WHERE p.id IS NULL
        ORDER BY c.name
    """).fetchall()
    return [dict(r) for r in rows]


def get_prospects_without_email(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute("""
        SELECT p.*, c.name AS company_name, c.domain
        FROM prospects p
        JOIN companies c ON p.company_id = c.id
        WHERE (p.email IS NULL OR p.email = '')
          AND p.linkedin_url IS NOT NULL AND p.linkedin_url != ''
        ORDER BY c.name, p.name
    """).fetchall()
    return [dict(r) for r in rows]


def get_prospects_without_messages(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute("""
        SELECT p.*, c.name AS company_name, c.domain, c.description AS company_description,
               c.employees, c.latest_round, c.hq_city
        FROM prospects p
        JOIN companies c ON p.company_id = c.id
        LEFT JOIN messages m ON p.id = m.prospect_id
        WHERE m.id IS NULL
        ORDER BY c.name, p.name
    """).fetchall()
    return [dict(r) for r in rows]


def prospect_exists(conn: sqlite3.Connection, external_id: str) -> bool:
    row = conn.execute("SELECT 1 FROM prospects WHERE external_id = ?", (external_id,)).fetchone()
    return row is not None


# ── Dashboard queries ────────────────────────────────────────────────────

def get_full_dashboard(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute("""
        SELECT p.*, c.name AS company_name, c.domain, c.latest_round, c.employees,
               c.hq_city AS company_city, c.description AS company_description
        FROM prospects p
        JOIN companies c ON p.company_id = c.id
        ORDER BY c.name, p.name
    """).fetchall()

    results = []
    for r in rows:
        d = dict(r)
        msgs = conn.execute(
            "SELECT * FROM messages WHERE prospect_id=? ORDER BY channel, sequence_num",
            (d["id"],),
        ).fetchall()
        d["messages"] = [dict(m) for m in msgs]
        results.append(d)
    return results
