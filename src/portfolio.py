import pandas as pd
from datetime import datetime
from db import (
    update_position,
    remove_position,
    record_transaction,
)
import sqlite3
from pathlib import Path

DB_PATH = Path("data/portfolio.db")


# ==============================================================
# Load open positions from SQLite
# ==============================================================

def load_positions():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM portfolio_positions", conn)
    conn.close()
    return df


# ==============================================================
# SELL LOGIC – MAIN ENTRY
# ==============================================================

def process_sell_signals(
    today,
    regime_a,
    top5_tickers,
    price_data,
    fx_row,
    exit_threshold=-0.05,  # ROC12 < -5%
):
    """
    Generates SELL signals based on:
      1. BEAR → sell all immediately
      2. stock not in TOP5 → sell
      3. momentum < exit threshold → sell
      4. price < SMA200 → sell

    Parameters:
      today: "YYYY-MM-DD"
      regime_a: "BULL" or "BEAR"
      top5_tickers: list of strings
      price_data: dict[ticker] = DataFrame with columns:
           Close, SMA200, ROC12
      fx_row: row with fx rates
    """

    positions = load_positions()
    if positions.empty:
        print("[SELL] No open positions → nothing to evaluate.")
        return []

    sell_list = []  # list of tickers to sell today

    # =====================================================
    # 1. MARKET BEAR → SELL EVERYTHING
    # =====================================================
    if regime_a == "BEAR":
        print("[SELL] Market Regime = BEAR → SELL ALL positions immediately!")
        sell_list = positions["ticker"].tolist()
        return sell_list  # no further checks needed

    # =====================================================
    # 2–4. Evaluate each stock individually
    # =====================================================
    for _, pos in positions.iterrows():
        ticker = pos["ticker"]
        currency = pos["currency"]
        qty = pos["quantity"]

        df = price_data.get(ticker)
        if df is None or df.empty:
            print(f"[SELL] No price data for {ticker}, skipping...")
            continue

        close = df["Close"].iloc[-1]
        sma200 = df["SMA200"].iloc[-1]
        roc12 = df["ROC12"].iloc[-1]

        # ---- Rule 2: not in TOP5 ----
        if ticker not in top5_tickers:
            print(f"[SELL] {ticker} dropped out of TOP5 → SELL today.")
            sell_list.append(ticker)
            continue

        # ---- Rule 3: momentum exit threshold ----
        if roc12 < exit_threshold:
            print(f"[SELL] {ticker} ROC12={roc12:.2%} < {exit_threshold:.2%} → SELL.")
            sell_list.append(ticker)
            continue

        # ---- Rule 4: trend broken ----
        if close < sma200:
            print(f"[SELL] {ticker} Close < SMA200 → SELL.")
            sell_list.append(ticker)
            continue

    return sell_list


# ==============================================================
# Execute SELL orders and update DB
# ==============================================================

def execute_sell_orders(today, sell_list, price_data, fx_row, regime):
    """Perform SELL transactions and update DB."""
    if not sell_list:
        print("[SELL] No sell signals today.")
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    for ticker in sell_list:

        # Load position
        row = cur.execute("""
            SELECT quantity, currency
            FROM portfolio_positions
            WHERE ticker=?
        """, (ticker,)).fetchone()

        if row is None:
            print(f"[SELL] Attempted to sell {ticker}, but position not found.")
            continue

        qty, currency = row

        # Get price
        df = price_data[ticker]
        price_ccy = df["Close"].iloc[-1]

        # FX conversion
        if currency == "USD":
            fx_rate = fx_row["USD"]
        elif currency == "EUR":
            fx_rate = fx_row["EUR"]
        else:
            fx_rate = 1.0

        price_pln = price_ccy * fx_rate

        # ---------------------------------------------------
        # Record transaction
        # ---------------------------------------------------
        record_transaction(
            timestamp=today,
            ticker=ticker,
            side="SELL",
            quantity=qty,
            price_ccy=price_ccy,
            currency=currency,
            price_pln=price_pln,
            regime=regime,
            note="SELL triggered by rules"
        )

        # Remove position from table
        remove_position(ticker)

        print(f"[SELL EXECUTED] {ticker}: sold {qty} @ {price_ccy:.2f} {currency} "
              f"({price_pln:.2f} PLN)")

    conn.close()
