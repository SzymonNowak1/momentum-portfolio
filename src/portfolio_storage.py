import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path("data/portfolio.db")


# ---------------------------------------------------------
# Load current positions
# ---------------------------------------------------------
def load_positions():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM portfolio_positions", conn)
    conn.close()
    return df


# ---------------------------------------------------------
# Save (overwrite) a position
# ---------------------------------------------------------
def update_position(ticker, quantity, currency, avg_price):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO portfolio_positions (ticker, quantity, currency, avg_price)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(ticker)
        DO UPDATE SET
            quantity = excluded.quantity,
            currency = excluded.currency,
            avg_price = excluded.avg_price
    """, (ticker, quantity, currency, avg_price))

    conn.commit()
    conn.close()


# ---------------------------------------------------------
# Remove a position
# ---------------------------------------------------------
def remove_position(ticker):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("DELETE FROM portfolio_positions WHERE ticker=?", (ticker,))
    conn.commit()
    conn.close()


# ---------------------------------------------------------
# Save transaction (BUY or SELL)
# ---------------------------------------------------------
def record_transaction(**kwargs):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO transactions (timestamp, ticker, side, quantity,
                                  price_ccy, currency, price_pln, regime, note)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        kwargs["timestamp"],
        kwargs["ticker"],
        kwargs["side"],
        kwargs["quantity"],
        kwargs["price_ccy"],
        kwargs["currency"],
        kwargs["price_pln"],
        kwargs["regime"],
        kwargs.get("note", "")
    ))

    conn.commit()
    conn.close()

def record_contribution(timestamp, amount_pln):
    """
    Saves a monthly contribution (deposit) into the contributions table.
    Example: record_contribution("2025-12-10", 2000)
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO contributions (timestamp, amount_pln)
        VALUES (?, ?)
    """, (timestamp, amount_pln))

    conn.commit()
    conn.close()

    print(f"[CONTRIBUTION] Saved contribution: {amount_pln} PLN on {timestamp}")
# ---------------------------------------------------------
# NEW: Compute total equity in PLN
# ---------------------------------------------------------
def estimate_total_equity(price_data, fx_row):
    """
    Returns total value of the portfolio in PLN.

    price_data  : dict[ticker] = DataFrame with Close column
    fx_row      : Series with fx rates: fx_row["USD"], fx_row["EUR"]
    """

    positions = load_positions()
    if positions.empty:
        return 0.0

    total_pln = 0.0

    for _, pos in positions.iterrows():
        ticker = pos["ticker"]
        qty = pos["quantity"]
        currency = pos["currency"]

        df = price_data.get(ticker)
        if df is None or df.empty:
            continue

        price_ccy = df["Close"].iloc[-1]

        # FX conversion
        if currency == "USD":
            fx = fx_row["USD"]
        elif currency == "EUR":
            fx = fx_row["EUR"]
        else:
            fx = 1.0

        total_pln += qty * price_ccy * fx

    return round(total_pln, 2)
