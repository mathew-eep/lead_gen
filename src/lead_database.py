"""Database layer for compliant lead collection."""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class LeadDatabase:
    """SQLite persistence for discovered companies and business contacts."""

    def __init__(self, db_path: str = "./data/leads.db") -> None:
        self.db_path = db_path
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS companies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic TEXT NOT NULL,
                    name TEXT,
                    website TEXT NOT NULL,
                    source_url TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(topic, website)
                )
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS contacts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company_website TEXT NOT NULL,
                    email TEXT NOT NULL,
                    source_url TEXT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(company_website, email)
                )
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic TEXT NOT NULL,
                    companies_found INTEGER DEFAULT 0,
                    contacts_found INTEGER DEFAULT 0,
                    status TEXT NOT NULL,
                    started_at DATETIME NOT NULL,
                    finished_at DATETIME,
                    notes TEXT
                )
                """
            )

            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_contacts_email ON contacts(email)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_companies_topic ON companies(topic)"
            )

            conn.commit()

    def add_company(
        self,
        topic: str,
        website: str,
        name: Optional[str] = None,
        source_url: Optional[str] = None,
    ) -> bool:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO companies (topic, name, website, source_url)
                    VALUES (?, ?, ?, ?)
                    """,
                    (topic, name, website, source_url),
                )
                conn.commit()
                return conn.total_changes > 0
        except Exception as exc:
            logger.error("Failed to add company: %s", exc)
            return False

    def add_contact(
        self,
        company_website: str,
        email: str,
        source_url: str,
    ) -> bool:
        try:
            with sqlite3.connect(self.db_path) as conn:
                # If table already existed with old schema, columns might still be there but not required.
                # However, since we removed the columns in the CREATE statement, we just insert into the core 3.
                try:
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO contacts (
                            company_website, email, source_url
                        ) VALUES (?, ?, ?)
                        """,
                        (company_website, email.lower(), source_url),
                    )
                except sqlite3.OperationalError:
                    # Fallback for if the old schema still exists in the local database file and requires 'contact_type'
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO contacts (
                            company_website, email, contact_type, source_url, confidence
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        (company_website, email.lower(), "general", source_url, 1.0),
                    )
                
                conn.commit()
                return conn.total_changes > 0
        except Exception as exc:
            logger.error("Failed to add contact: %s", exc)
            return False

    def start_run(self, topic: str) -> int:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO runs (topic, status, started_at)
                VALUES (?, 'running', ?)
                """,
                (topic, datetime.utcnow().isoformat()),
            )
            conn.commit()
            return int(cur.lastrowid)

    def finish_run(
        self,
        run_id: int,
        companies_found: int,
        contacts_found: int,
        status: str = "success",
        notes: Optional[str] = None,
    ) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                UPDATE runs
                SET companies_found = ?,
                    contacts_found = ?,
                    status = ?,
                    finished_at = ?,
                    notes = ?
                WHERE id = ?
                """,
                (
                    companies_found,
                    contacts_found,
                    status,
                    datetime.utcnow().isoformat(),
                    notes,
                    run_id,
                ),
            )
            conn.commit()

    def get_stats(self) -> Dict[str, int]:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM companies")
            company_count = int(cur.fetchone()[0])
            cur.execute("SELECT COUNT(*) FROM contacts")
            contact_count = int(cur.fetchone()[0])
            cur.execute("SELECT COUNT(*) FROM runs")
            run_count = int(cur.fetchone()[0])

        return {
            "companies": company_count,
            "contacts": contact_count,
            "runs": run_count,
        }

    def top_contacts(self, limit: int = 50) -> List[Dict[str, str]]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT company_website, email, contact_type, source_url, confidence, created_at
                FROM contacts
                ORDER BY confidence DESC, created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]
