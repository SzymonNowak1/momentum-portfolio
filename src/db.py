import sqlite3
from pathlib import Path

# Ścieżka do bazy: repo_root/data/portfolio.db
DB_PATH = Path(__file__).resolve().parent.parent / "data" / "portfolio.db"


def get_connection(db_path: Path | None = None) -> sqlite3.Connection:
    """
    Zwraca połączenie do bazy SQLite.
    Jeśli plik bazy nie istnieje – zostanie utworzony.
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
    Na razie:
      - contributions (dopłaty)
      - positions (aktualne pozycje)
      - trades (historia transakcji)
      - equity_history (wartość portfela w czasie)
    """
    close_after = False
    if conn is None:
        conn = get_connection()
        close_after = True

    cur = conn.cursor()

    cur.executescript(
        """
        CREATE TABLE IF NOT EXISTS contributions (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            date          TEXT NOT NULL,          -- YYYY-MM-DD
            amount_pln    REAL NOT NULL,
            note          TEXT
        );

        CREATE TABLE IF NOT EXISTS positions (
            ticker            TEXT PRIMARY KEY,
            currency          TEXT NOT NULL,
            units             REAL NOT NULL,      -- ilość (może być ułamkowa)
            avg_price_ccy     REAL NOT NULL,      -- średnia cena w walucie instrumentu
            created_at        TEXT NOT NULL,
            updated_at        TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS trades (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            date          TEXT NOT NULL,          -- YYYY-MM-DD
            ticker        TEXT NOT NULL,
            side          TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
            units         REAL NOT NULL,
            price_ccy     REAL NOT NULL,
            value_pln     REAL NOT NULL,
            reason        TEXT,
            FOREIGN KEY (ticker) REFERENCES positions(ticker)
        );

        CREATE TABLE IF NOT EXISTS equity_history (
            date          TEXT PRIMARY KEY,       -- YYYY-MM-DD
            equity_pln    REAL NOT NULL
        );
        """
    )

    conn.commit()
    if close_after:
        conn.close()
