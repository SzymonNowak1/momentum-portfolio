import sqlite3
from pathlib import Path

DB_PATH = Path("data/portfolio.db")


def init_db():
    """Initialize SQLite database with required tables."""
    DB_PATH.parent.mkdir(exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # === OPEN POSITIONS ===
    cur.execute("""
    CREATE TABLE IF NOT EXISTS portfolio_positions (
        ticker TEXT PRIMARY KEY,
        currency TEXT,
        quantity REAL,
        avg_price REAL,
        value_pln REAL,
        opened_at TEXT
    );
    """)

    # === TRANSACTION HISTORY ===
    cur.execute("""
    CREATE TABLE IF NOT EXISTS portfolio_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        ticker TEXT,
        side TEXT,
        quantity REAL,
        price_ccy REAL,
        currency TEXT,
        price_pln REAL,
        regime TEXT,
        note TEXT
    );
    """)

    # === EQUITY HISTORY ===
    cur.execute("""
    CREATE TABLE IF NOT EXISTS portfolio_equity (
        date TEXT PRIMARY KEY,
        equity_pln REAL,
        cash_pln REAL,
        invested_pln REAL,
        fx_usd REAL,
        fx_eur REAL
    );
    """)

    conn.commit()
    conn.close()
    print("[DB] portfolio.db initialized (positions + history + equity).")


# ==============================================================
# Utility methods
# ==============================================================

def record_transaction(timestamp, ticker, side, quantity, price_ccy, currency,
                       price_pln, regime, note=""):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO portfolio_history
        (timestamp, ticker, side, quantity, price_ccy, currency, price_pln, regime, note)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (timestamp, ticker, side, quantity, price_ccy, currency,
          price_pln, regime, note))

    conn.commit()
    conn.close()


def update_position(ticker, currency, quantity, avg_price, value_pln, opened_at):
    """Insert or update an open position."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO portfolio_positions 
        (ticker, currency, quantity, avg_price, value_pln, opened_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(ticker) DO UPDATE SET
            quantity = excluded.quantity,
            avg_price = excluded.avg_price,
            value_pln = excluded.value_pln,
            opened_at = excluded.opened_at
    """, (ticker, currency, quantity, avg_price, value_pln, opened_at))

    conn.commit()
    conn.close()


def remove_position(ticker):
    """Delete a position after SELL."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("DELETE FROM portfolio_positions WHERE ticker=?", (ticker,))

    conn.commit()
    conn.close()


def record_equity(date, equity_pln, cash_pln, invested_pln, fx_usd, fx_eur):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO portfolio_equity
        (date, equity_pln, cash_pln, invested_pln, fx_usd, fx_eur)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (date, equity_pln, cash_pln, invested_pln, fx_usd, fx_eur))

    conn.commit()
    conn.close()
