from __future__ import annotations
from datetime import date

import sqlite3
from pathlib import Path
import pandas as pd

from db import record_transaction, update_position, remove_position
from data_loader import load_price_history

DB_PATH = Path("data/portfolio.db")


def _load_positions() -> pd.DataFrame:
    """Wczytuje aktualne pozycje z SQLite."""
    if not DB_PATH.exists():
        return pd.DataFrame(columns=["ticker", "currency", "quantity", "avg_price", "value_pln", "opened_at"])

    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM portfolio_positions", conn)
    conn.close()
    return df


def _get_fx_for_currency(fx_row: pd.Series, currency: str) -> float:
    if currency.upper().startswith("USD"):
        return float(fx_row["USD"])
    if currency.upper().startswith("EUR"):
        return float(fx_row["EUR"])
    # domyślnie PLN
    return 1.0


# =========================================================
# 1. SELL ALL – używane przy pełnym rebalansie lub BEAR
# =========================================================

def sell_all_positions_for_rebalance(
    today: date,
    fx_row: pd.Series,
    regime: str,
    note: str = "REBALANCE SELL ALL",
) -> None:
    """
    Sprzedaje wszystkie aktualne pozycje po cenie rynkowej.
    Używane:
      - przy miesięcznym rebalansie
      - przy przejściu w BEAR (wtedy note np. 'BEAR SELL ALL')
    """
    positions = _load_positions()
    if positions.empty:
        print("[REBALANCE] No open positions – nothing to sell.")
        return

    print(f"[REBALANCE] Selling ALL current positions ({len(positions)}) ...")

    for _, pos in positions.iterrows():
        ticker = pos["ticker"]
        qty = float(pos["quantity"])
        currency = pos["currency"]

        # ostatnia cena
        price_df = load_price_history(ticker, period="6mo")
        last_price = float(price_df.iloc[-1, 0])

        fx = _get_fx_for_currency(fx_row, currency)
        price_pln = last_price * fx
        value_pln = price_pln * qty

        record_transaction(
            timestamp=today.isoformat(),
            ticker=ticker,
            side="SELL",
            quantity=qty,
            price_ccy=last_price,
            currency=currency,
            price_pln=value_pln,
            regime=regime,
            note=note,
        )

        remove_position(ticker)

        print(f"  [SELL] {ticker}: {qty:.4f} @ {last_price:.2f} {currency} "
              f"≈ {value_pln:,.2f} PLN")

    print("[REBALANCE] All positions sold.")


# =========================================================
# 2. BUY wg docelowej alokacji
# =========================================================

def buy_according_to_allocation(
    today: date,
    alloc_df: pd.DataFrame,
    fx_row: pd.Series,
    regime: str,
    note: str = "REBALANCE BUY",
) -> None:
    """
    Kupuje pozycje zgodnie z tabelą alokacji (alloc_df), którą
    wcześniej policzył build_target_allocation w main.py.

    Oczekiwane kolumny w alloc_df:
      - 'ticker'
      - 'currency'
      - 'target_value_pln'
    """

    if alloc_df.empty:
        print("[REBALANCE] Allocation table is empty – nothing to buy.")
        return

    print("[REBALANCE] Buying positions according to target allocation...")

    for _, row in alloc_df.iterrows():
        ticker = row["ticker"]
        currency = row["currency"]
        target_pln = float(row["target_value_pln"])

        if target_pln <= 0:
            continue

        price_df = load_price_history(ticker, period="6mo")
        last_price = float(price_df.iloc[-1, 0])

        fx = _get_fx_for_currency(fx_row, currency)
        price_pln = last_price * fx

        qty = target_pln / price_pln
        value_pln = qty * price_pln

        record_transaction(
            timestamp=today.isoformat(),
            ticker=ticker,
            side="BUY",
            quantity=qty,
            price_ccy=last_price,
            currency=currency,
            price_pln=value_pln,
            regime=regime,
            note=note,
        )

        update_position(
            ticker=ticker,
            currency=currency,
            quantity=qty,
            avg_price=last_price,
            value_pln=value_pln,
            opened_at=today.isoformat(),
        )

        print(f"  [BUY] {ticker}: {qty:.4f} @ {last_price:.2f} {currency} "
              f"≈ {value_pln:,.2f} PLN")

    print("[REBALANCE] Target allocation bought.")
