"""
SQLite database layer.

Two purposes:
  1. Persist search results so you can revisit them without re-running API calls.
  2. Cache raw Proxycurl responses so repeated runs don't burn API credits.

Schema
------
searches          — one row per (company_url, keyword) search session
company_universe  — company names extracted from target-company alumni profiles
candidates        — final list of people to approach (name + LinkedIn URL)
profile_cache     — raw JSON profiles keyed by LinkedIn URL (24-hour TTL)
"""

import sqlite3
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List

DB_PATH = Path(__file__).parent / "recruitment.db"


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")   # safe concurrent reads
    return conn


def init_db():
    conn = get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS searches (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            company_url TEXT    NOT NULL,
            company_name TEXT,
            keyword     TEXT    NOT NULL,
            created_at  TEXT    DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS company_universe (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            search_id           INTEGER NOT NULL REFERENCES searches(id),
            company_name        TEXT    NOT NULL,
            company_linkedin_url TEXT,
            count_before        INTEGER DEFAULT 0,
            count_after         INTEGER DEFAULT 0,
            count_total         INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS candidates (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            search_id    INTEGER NOT NULL REFERENCES searches(id),
            name         TEXT,
            linkedin_url TEXT,
            title        TEXT,
            company      TEXT,
            added_at     TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS profile_cache (
            linkedin_url TEXT PRIMARY KEY,
            data         TEXT NOT NULL,
            cached_at    TEXT DEFAULT (datetime('now'))
        );
    """)
    conn.commit()
    conn.close()


# ------------------------------------------------------------------ #
#  Searches                                                            #
# ------------------------------------------------------------------ #

def create_search(company_url: str, keyword: str, company_name: str = None) -> int:
    conn = get_conn()
    cur = conn.execute(
        "INSERT INTO searches (company_url, keyword, company_name) VALUES (?, ?, ?)",
        (company_url, keyword, company_name)
    )
    conn.commit()
    search_id = cur.lastrowid
    conn.close()
    return search_id


def list_searches() -> List[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM searches ORDER BY created_at DESC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_search(search_id: int) -> Optional[dict]:
    conn = get_conn()
    row = conn.execute("SELECT * FROM searches WHERE id=?", (search_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


# ------------------------------------------------------------------ #
#  Company Universe                                                     #
# ------------------------------------------------------------------ #

def upsert_company(
    search_id: int,
    company_name: str,
    company_linkedin_url: str,
    relationship: str   # 'before' | 'after'
):
    """Increment count for a company, inserting if needed."""
    conn = get_conn()
    existing = conn.execute(
        "SELECT id, count_before, count_after FROM company_universe "
        "WHERE search_id=? AND company_name=?",
        (search_id, company_name)
    ).fetchone()

    if existing:
        new_before = existing["count_before"] + (1 if relationship == "before" else 0)
        new_after  = existing["count_after"]  + (1 if relationship == "after"  else 0)
        conn.execute(
            "UPDATE company_universe SET count_before=?, count_after=?, "
            "count_total=?, company_linkedin_url=COALESCE(NULLIF(company_linkedin_url,''), ?) "
            "WHERE id=?",
            (new_before, new_after, new_before + new_after, company_linkedin_url, existing["id"])
        )
    else:
        cb = 1 if relationship == "before" else 0
        ca = 1 if relationship == "after"  else 0
        conn.execute(
            "INSERT INTO company_universe "
            "(search_id, company_name, company_linkedin_url, count_before, count_after, count_total) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (search_id, company_name, company_linkedin_url, cb, ca, cb + ca)
        )
    conn.commit()
    conn.close()


def get_company_universe(search_id: int) -> List[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM company_universe WHERE search_id=? ORDER BY count_total DESC",
        (search_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ------------------------------------------------------------------ #
#  Candidates                                                          #
# ------------------------------------------------------------------ #

def save_candidate(
    search_id: int,
    name: str,
    linkedin_url: str,
    title: str,
    company: str
):
    conn = get_conn()
    # Avoid duplicates within same search
    exists = conn.execute(
        "SELECT 1 FROM candidates WHERE search_id=? AND linkedin_url=?",
        (search_id, linkedin_url)
    ).fetchone()
    if not exists:
        conn.execute(
            "INSERT INTO candidates (search_id, name, linkedin_url, title, company) "
            "VALUES (?, ?, ?, ?, ?)",
            (search_id, name, linkedin_url, title, company)
        )
        conn.commit()
    conn.close()


def get_candidates(search_id: int) -> List[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM candidates WHERE search_id=? ORDER BY company, name",
        (search_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ------------------------------------------------------------------ #
#  Profile cache                                                        #
# ------------------------------------------------------------------ #

CACHE_TTL_HOURS = 24


def get_cached_profile(linkedin_url: str) -> Optional[dict]:
    conn = get_conn()
    row = conn.execute(
        "SELECT data, cached_at FROM profile_cache WHERE linkedin_url=?",
        (linkedin_url,)
    ).fetchone()
    conn.close()
    if row:
        cached_at = datetime.fromisoformat(row["cached_at"])
        if datetime.utcnow() - cached_at < timedelta(hours=CACHE_TTL_HOURS):
            return json.loads(row["data"])
    return None


def cache_profile(linkedin_url: str, data: dict):
    conn = get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO profile_cache (linkedin_url, data, cached_at) "
        "VALUES (?, ?, datetime('now'))",
        (linkedin_url, json.dumps(data))
    )
    conn.commit()
    conn.close()
