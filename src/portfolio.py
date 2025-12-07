import pandas as pd
import numpy as np
from datetime import datetime

from .db import load_portfolio_db, save_portfolio_db, save_transaction
from .data_loader import load_price_history, load_fx_rates

# =========================================
# PORTFOLIO ENGINE
# =========================================

def load_current_portfolio():
    """Wczytuje zapisany portfel z SQLite."""
    df = load_portfolio_db()
    if df is None or df.empty:
        return pd.DataFrame(columns=["ticker", "currency", "shares", "avg_price", "last_update"])
    return df


def get_live_prices(tickers):
    """Pobiera aktualne ceny wszystkich tickerów (close)."""
    price_map = {}
    for t in tickers:
        df = load_price_history(t, period="1mo")
        price_map[t] = df.iloc[-1, 0]
    return price_map


def convert_to_pln(value, currency, fx_row):
    """Konwertuje wartość z innej waluty do PLN."""
    if currency == "PLN":
        return value
    rate = fx_row[currency]
    return value * rate


def compute_target_values(total_equity_pln, target_alloc_dict):
    """
    Wylicza ile PLN powinno być przeznaczone na każdy ticker.

    target_alloc_dict → {"AAPL": 0.2, "GOOGL": 0.2, ...}
    """
    df = []
    for t, w in target_alloc_dict.items():
        df.append({"ticker": t,
                   "weight": w,
                   "target_pln": total_equity_pln * w})
    return pd.DataFrame(df)


def compute_orders(portfolio_df, target_df, price_map, fx_row, universe_currency_map):
    """
    Wylicza, ile akcji kupić / sprzedać, aby portfel dopasować do targetów.

    portfolio_df – obecne pozycje
    target_df – pożądane wartości PLN
    """
    orders = []

    for _, row in target_df.iterrows():
        ticker = row["ticker"]
        target_pln = row["target_pln"]

        currency = universe_currency_map[ticker]
        price = price_map[ticker]

        # przeliczamy cenę w PLN
        fx = fx_row[currency]
        price_pln = price * fx

        # obecne pozycje
        pos = portfolio_df[portfolio_df["ticker"] == ticker]
        if pos.empty:
            current_pln = 0
            current_shares = 0
        else:
            current_shares = pos.iloc[0]["shares"]
            current_pln = current_shares * price_pln

        diff_pln = target_pln - current_pln

        # jeśli różnica jest mała → ignorujemy (unikamy mikrotransakcji)
        if abs(diff_pln) < 50:
            continue

        shares_to_trade = diff_pln / price_pln

        order_type = "BUY" if shares_to_trade > 0 else "SELL"

        orders.append({
            "ticker": ticker,
            "currency": currency,
            "order": order_type,
            "shares": round(abs(shares_to_trade), 4),
            "price": price,
            "fx": fx
        })

    return pd.DataFrame(orders)


def apply_orders_to_portfolio(portfolio_df, orders_df):
    """
    Zapisuje transakcje oraz aktualizuje portfel.
    """
    for _, o in orders_df.iterrows():
        ticker = o["ticker"]
        shares = o["shares"]
        price = o["price"]
        currency = o["currency"]
        fx = o["fx"]
        action = o["order"]

        save_transaction(
            ticker=ticker,
            action=action,
            shares=shares,
            price=price,
            currency=currency,
            fx=fx,
            timestamp=str(datetime.utcnow())
        )

        # aktualizacja portfela
        row = portfolio_df[portfolio_df["ticker"] == ticker]

        if action == "BUY":
            if row.empty:
                portfolio_df = pd.concat([
                    portfolio_df,
                    pd.DataFrame([{
                        "ticker": ticker,
                        "currency": currency,
                        "shares": shares,
                        "avg_price": price,
                        "last_update": str(datetime.utcnow())
                    }])
                ])
            else:
                old_shares = row.iloc[0]["shares"]
                old_price = row.iloc[0]["avg_price"]
                new_shares = old_shares + shares
                new_avg_price = (old_shares * old_price + shares * price) / new_shares

                portfolio_df.loc[portfolio_df["ticker"] == ticker, ["shares", "avg_price", "last_update"]] = \
                    [new_shares, new_avg_price, str(datetime.utcnow())]

        else:  # SELL
            if not row.empty:
                old_shares = row.iloc[0]["shares"]
                new_shares = old_shares - shares

                if new_shares <= 0:
                    portfolio_df = portfolio_df[portfolio_df["ticker"] != ticker]
                else:
                    portfolio_df.loc[portfolio_df["ticker"] == ticker, ["shares", "last_update"]] = \
                        [new_shares, str(datetime.utcnow())]

    portfolio_df = portfolio_df.reset_index(drop=True)
    save_portfolio_db(portfolio_df)
    return portfolio_df


# =========================================
# MAIN PROCESSOR
# =========================================

def run_portfolio_engine(target_alloc_dict, universe_currency_map):
    """
    Główna funkcja realizująca logikę portfela.
    """
    print("\n=== [PORTFOLIO ENGINE] Loading portfolio... ===")

    portfolio_df = load_current_portfolio()
    print(portfolio_df)

    tickers = list(target_alloc_dict.keys())
    price_map = get_live_prices(tickers)

    fx_row = load_fx_rates()
    total_equity_pln = 0

    # liczymy wartość portfela
    for _, row in portfolio_df.iterrows():
        t = row["ticker"]
        c = row["currency"]
        shares = row["shares"]
        p = price_map.get(t, 0)
        fx = fx_row[c]
        total_equity_pln += shares * p * fx

    print(f"\n[PORTFOLIO] Current equity = {total_equity_pln:.2f} PLN")

    target_df = compute_target_values(total_equity_pln, target_alloc_dict)

    orders_df = compute_orders(portfolio_df, target_df, price_map, fx_row, universe_currency_map)

    if orders_df.empty:
        print("\n[PORTFOLIO] No trades needed.")
        return portfolio_df

    print("\n=== TRADES TO EXECUTE ===")
    print(orders_df)

    portfolio_df = apply_orders_to_portfolio(portfolio_df, orders_df)

    print("\n=== UPDATED PORTFOLIO ===")
    print(portfolio_df)

    return portfolio_df
