from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional


# Ścieżka do bazy: <repo_root>/data/portfolio.db
DB_PATH = Path(__file__).resolve().parent.parent / "data" / "portfolio.db"


def get_connection(db_path: Optional[Path] = None) -> sqlite3.Connection:
    """
    Zwraca połączenie do bazy SQLite.
    Tworzy katalog /data jeśli nie istnieje.
    """
    if db_path is None:
        db_path = DB_PATH

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db(conn: sqlite3.Connection | None = None) -> None:
    """
    Tworzy tabele, jeśli jeszcze nie istnieją.

    Tabele:
      - positions   – aktualne pozycje w portfelu
      - trades      – historia transakcji (BUY/SELL)
      - cash_flows  – dopłaty / wypłaty w PLN
    """
    close_after = False
    if conn is None:
        conn = get_connection()
        close_after = True

    cur = conn.cursor()

    cur.executescript(
        """
        CREATE TABLE IF NOT EXISTS positions (
            ticker          TEXT PRIMARY KEY,
            currency        TEXT NOT NULL,
            units           REAL NOT NULL,
            avg_price_ccy   REAL NOT NULL,
            last_update     TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS trades (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            date            TEXT NOT NULL,   -- ISO YYYY-MM-DD HH:MM:SS
            ticker          TEXT NOT NULL,
            side            TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
            units           REAL NOT NULL,
            price_ccy       REAL NOT NULL,
            currency        TEXT NOT NULL,
            fx              REAL NOT NULL,
            value_pln       REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS cash_flows (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            date            TEXT NOT NULL,   -- ISO YYYY-MM-DD
            amount_pln      REAL NOT NULL,
            type            TEXT NOT NULL CHECK (type IN ('DEPOSIT','WITHDRAW')),
            note            TEXT
        );
        """
    )

    conn.commit()

    if close_after:
        conn.close()


if __name__ == "__main__":
    # test lokalny
    init_db()
    print(f"DB initialized at: {DB_PATH}")
