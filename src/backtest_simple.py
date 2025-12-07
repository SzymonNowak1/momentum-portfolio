import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime
from universe import load_universe
import os

# =====================================================================
# BACKTEST PARAMETERS
# =====================================================================
START = "2015-01-01"
END = "2025-12-05"
TOP_N = 5
REBALANCE_DAY = 10
MONTHLY_CONTRIBUTION = 2000  # PLN

os.makedirs("reports", exist_ok=True)

# =====================================================================
# LOAD PRICE DATA
# =====================================================================
def download_price_history(tickers):
    data = yf.download(
        tickers,
        start=START,
        end=END,
        interval="1d",
        auto_adjust=True,
        progress=False,
        threads=True
    )
    return data["Close"]


# =====================================================================
# MOMENTUM CALCULATION
# =====================================================================
def compute_momentum_scores(price_df):
    roc3 = price_df.pct_change(63).iloc[-1]
    roc6 = price_df.pct_change(126).iloc[-1]
    roc12 = price_df.pct_change(252).iloc[-1]

    score = (roc3 + roc6 + roc12) / 3
    return score.sort_values(ascending=False)


# =====================================================================
# BACKTEST ENGINE
# =====================================================================
def run_backtest():

    print("\n=== BACKTEST: 2015 → 2025 ===\n")

    tickers = load_universe()
    prices = download_price_history(tickers)

    equity = 0.0
    positions = {}
    trades = []
    equity_curve = []

    current_month = None

    for date, row in prices.iterrows():
        if row.isna().all():
            continue

        # --- monthly contribution ---
        if date.day == REBALANCE_DAY:
            equity += MONTHLY_CONTRIBUTION

        # --- rebalancing only on REBALANCE_DAY ---
        if date.day == REBALANCE_DAY:
            scores = compute_momentum_scores(prices.loc[:date])
            topN = list(scores.head(TOP_N).index)

            # SELL everything outside topN
            for ticker in list(positions.keys()):
                if ticker not in topN:
                    buy_price = positions[ticker]["buy_price"]
                    sell_price = row[ticker]
                    pnl = (sell_price - buy_price) / buy_price * 100

                    trades.append([ticker, positions[ticker]["buy_date"], date, pnl])
                    equity += positions[ticker]["amount"] * (sell_price / buy_price)
                    del positions[ticker]

            # BUY topN allocations
            target_per_position = equity / TOP_N

            for ticker in topN:
                price = row[ticker]
                amount = target_per_position / price

                positions[ticker] = {
                    "buy_date": date,
                    "buy_price": price,
                    "amount": amount
                }

        # record equity curve
        total = 0
        for t, pos in positions.items():
            total += pos["amount"] * row[t]

        equity_curve.append([date, equity + total])

    # --- SAVE REPORTS ---
    eq_df = pd.DataFrame(equity_curve, columns=["date", "equity"])
    eq_df.to_csv("reports/backtest_equity.csv", index=False)

    trades_df = pd.DataFrame(trades, columns=["ticker", "buy_date", "sell_date", "pnl_pct"])
    trades_df.to_csv("reports/backtest_trades.csv", index=False)

    # --- METRICS ---
    print("\n=== BACKTEST RESULTS ===\n")

    num_trades = len(trades_df)
    win_rate = (trades_df["pnl_pct"] > 0).mean() * 100 if num_trades > 0 else 0
    avg_pnl = trades_df["pnl_pct"].mean() if num_trades > 0 else 0
    best_trade = trades_df["pnl_pct"].max() if num_trades > 0 else 0
    worst_trade = trades_df["pnl_pct"].min() if num_trades > 0 else 0

    eq_vals = eq_df["equity"].values
    max_eq = np.maximum.accumulate(eq_vals)
    dd = (eq_vals - max_eq) / max_eq * 100
    max_dd = dd.min()

    total_return = (eq_vals[-1] / eq_vals[0] - 1) * 100

    print(f"Liczba transakcji:      {num_trades}")
    print(f"Win rate:               {win_rate:.2f}%")
    print(f"Średnie P&L:            {avg_pnl:.2f}%")
    print(f"Najlepsza transakcja:   {best_trade:.2f}%")
    print(f"Najgorsza transakcja:   {worst_trade:.2f}%")
    print(f"Maksymalne DD:          {max_dd:.2f}%")
    print(f"Całkowity zwrot:        {total_return:.2f}%")

    print("\nPliki zapisane w folderze /reports/:")
    print(" - backtest_equity.csv")
    print(" - backtest_trades.csv\n")


if __name__ == "__main__":
    run_backtest()
