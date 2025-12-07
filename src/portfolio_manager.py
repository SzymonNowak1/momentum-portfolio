from __future__ import annotations

import pandas as pd
from datetime import datetime
from typing import Dict, List

from db import get_connection
from portfolio_storage import load_positions, save_positions
from fx import get_fx_rate
from data_loader import load_price_history


ZPR_TICKER = "ZPR1.DE"


def record_trade(ticker, side, units, price_ccy, currency, fx, value_pln):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO trades (date, ticker, side, units, price_ccy, currency, fx, value_pln)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            datetime.utcnow().isoformat(timespec="seconds"),
            ticker,
            side,
            units,
            price_ccy,
            currency,
            fx,
            value_pln,
        ),
    )
    conn.commit()
    conn.close()


# --------------------------------------------------------------------

def sell_all_positions(date_str: str):
    """Sprzedaje wszystkie akcje w portfelu — używane w BEAR mode."""
    df = load_positions()
    if df.empty:
        return

    conn = get_connection()
    cur = conn.cursor()

    for _, row in df.iterrows():
        ticker = row["ticker"]
        units = float(row["units"])
        currency = row["currency"]

        # cena rynkowa
        price = load_price_history(ticker, period="5d").iloc[-1]["Close"]
        fx = get_fx_rate(currency)

        value_pln = units * price * fx

        record_trade(ticker, "SELL", units, price, currency, fx, value_pln)

    # usuń pozycje
    cur.execute("DELETE FROM positions")
    conn.commit()
    conn.close()


# --------------------------------------------------------------------

def buy_zpr_from_all_cash(total_cash_pln: float):
    """Kupuje ZPR1.DE za całą dostępną gotówkę."""
    if total_cash_pln <= 0:
        return

    hist = load_price_history(ZPR_TICKER, period="5d")
    price = hist.iloc[-1]["Close"]
    currency = "EUR"  # ZPR1.DE notowane w EUR
    fx = get_fx_rate(currency)

    units = total_cash_pln / (price * fx)

    record_trade(ZPR_TICKER, "BUY", units, price, currency, fx, total_cash_pln)

    # zapis do positions
    df = pd.DataFrame(
        [{
            "ticker": ZPR_TICKER,
            "currency": currency,
            "units": units,
            "avg_price_ccy": price,
            "last_update": datetime.utcnow().isoformat(timespec="seconds"),
        }]
    )
    save_positions(df)


# --------------------------------------------------------------------

def buy_top5(targets: Dict[str, float], total_value_pln: float):
    """
    targets = {"AAPL": 0.20, "MSFT": 0.20, ...}
    """
    df = pd.DataFrame(columns=[
        "ticker", "currency", "units", "avg_price_ccy", "last_update"
    ])

    for ticker, weight in targets.items():
        allocation = total_value_pln * weight

        price = load_price_history(ticker, period="5d").iloc[-1]["Close"]
        currency = "USD"
        fx = get_fx_rate(currency)

        units = allocation / (price * fx)

        record_trade(
            ticker, "BUY", units, price, currency, fx, allocation
        )

        df.loc[len(df)] = [
            ticker,
            currency,
            units,
            price,
            datetime.utcnow().isoformat(timespec="seconds"),
        ]

    save_positions(df)
