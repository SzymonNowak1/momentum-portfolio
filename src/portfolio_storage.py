import pandas as pd
import sqlite3
from pathlib import Path

DB_PATH = Path("data/portfolio.db")


def get_connection():
    return sqlite3.connect(DB_PATH)


# ==============================================================
# LOAD POSITIONS
# ==============================================================

def load_positions():
    conn = get_connection()
    try:
        df = pd.read_sql("SELECT * FROM portfolio_positions", conn)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


# ==============================================================
# SAVE / UPDATE POSITION
# ==============================================================

def save_position(ticker, quantity, currency, avg_price_ccy, avg_price_pln):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO portfolio_positions (ticker, quantity, currency, avg_price_ccy, avg_price_pln)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(ticker) DO UPDATE SET
            quantity = excluded.quantity,
            avg_price_ccy = excluded.avg_price_ccy,
            avg_price_pln = excluded.avg_price_pln
    """, (ticker, quantity, currency, avg_price_ccy, avg_price_pln))

    conn.commit()
    conn.close()


# ==============================================================
# REMOVE POSITION
# ==============================================================

def delete_position(ticker):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM portfolio_positions WHERE ticker=?", (ticker,))
    conn.commit()
    conn.close()
