import pandas as pd
from db import record_transaction, update_position
from datetime import datetime


def buy_according_to_allocation(today, alloc_df, fx_row, price_data):
    """
    Executes BUY operations for each row in alloc_df:
    ticker | target_value_ccy | currency
    """

    print("\n[BUY ENGINE] Rozpoczynam wykonywanie zakupów...")

    for _, row in alloc_df.iterrows():
        ticker = row["ticker"]
        target_value_ccy = row["target_value_ccy"]
        currency = row["currency"]

        df = price_data.get(ticker)
        if df is None or df.empty:
            print(f"[BUY] Brak danych price_data dla {ticker} — pomijam.")
            continue

        # last available close price
        price_ccy = df["Close"].iloc[-1]

        if price_ccy <= 0:
            print(f"[BUY] Cena nieprawidłowa dla {ticker} — pomijam.")
            continue

        # quantity to buy
        qty = target_value_ccy / price_ccy
        qty = round(qty, 4)  # precision

        # FX conversion
        if currency == "USD":
            fx = fx_row["USD"]
        elif currency == "EUR":
            fx = fx_row["EUR"]
        else:
            fx = 1.0

        price_pln = price_ccy * fx

        print(f"[BUY] {ticker}: kupuję {qty} @ {price_ccy} {currency}, ({price_pln:.2f} PLN)")

        # -- Record transaction
        record_transaction(
            timestamp=today,
            ticker=ticker,
            side="BUY",
            quantity=qty,
            price_ccy=price_ccy,
            currency=currency,
            price_pln=price_pln,
            regime="BULL",   # możesz tu dodać realny regime z main()
            note="BUY according to target allocation"
        )

        # -- Update portfolio state
        update_position(
            ticker=ticker,
            qty=qty,
            currency=currency,
            avg_price_ccy=price_ccy,
            avg_price_pln=price_pln
        )

    print("[BUY ENGINE] Zakończono wykonywanie zakupów.\n")
