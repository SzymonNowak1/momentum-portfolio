import sqlite3
from pathlib import Path
from datetime import datetime

DB_PATH = Path("data/portfolio.db")


# ==============================================================
# CONNECTION
# ==============================================================

def get_connection():
    """Return SQLite connection, create DB if missing."""
    conn = sqlite3.connect(DB_PATH)
    return conn


# ==============================================================
# INIT DB STRUCTURE
# ==============================================================

def init_db():
    conn = get_connection()
    cur = conn.cursor()

    # Table with open positions
    cur.execute("""
        CREATE TABLE IF NOT EXISTS portfolio_positions (
            ticker TEXT PRIMARY KEY,
            quantity REAL,
            currency TEXT,
            avg_price_ccy REAL,
            avg_price_pln REAL
        )
    """)

    # Table with transactions history
    cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
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
        )
    """)

    conn.commit()
    conn.close()


# ==============================================================
# CRUD FOR POSITIONS
# ==============================================================

def load_positions():
    conn = get_connection()
    df = None
    try:
        import pandas as pd
        df = pd.read_sql("SELECT * FROM portfolio_positions", conn)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


def update_position(ticker, qty, currency, avg_price_ccy, avg_price_pln):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO portfolio_positions (ticker, quantity, currency, avg_price_ccy, avg_price_pln)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(ticker) DO UPDATE SET
            quantity = excluded.quantity,
            avg_price_ccy = excluded.avg_price_ccy,
            avg_price_pln = excluded.avg_price_pln
    """, (ticker, qty, currency, avg_price_ccy, avg_price_pln))

    conn.commit()
    conn.close()


def remove_position(ticker):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM portfolio_positions WHERE ticker=?", (ticker,))
    conn.commit()
    conn.close()


# ==============================================================
# TRANSACTIONS
# ==============================================================

def record_transaction(timestamp, ticker, side, quantity, price_ccy, currency, price_pln, regime, note=""):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO transactions (
            timestamp, ticker, side, quantity, price_ccy, currency,
            price_pln, regime, note
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (timestamp, ticker, side, quantity, price_ccy, currency, price_pln, regime, note))

    conn.commit()
    conn.close()
