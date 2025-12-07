# src/backtest_simple.py
"""
Prosty backtest strategii TOP5 momentum + market regime (SPY)
Okres: 2015-01-01 do 2025-12-05

Założenia:
- Rebalancing: raz w miesiącu (10 dzień lub najbliższa sesja)
- Regime:
    BULL  -> inwestujemy w TOP5 momentum
    BEAR  -> sprzedajemy wszystko (cash)
- Momentum:
    score = 0.4 * ROC12 + 0.3 * ROC6 + 0.3 * ROC3
    ROCx liczone jako zmiana ceny w okresie x miesięcy
- Brak doraźnych SELL (tylko na rebalansie i zmianie regime)
- Wpłata: 2000 jednostek waluty (np. PLN) co miesiąc w dniu rebalansu
- Brak FX (wszystko w jednej walucie) – FX dołożymy później
"""

import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta

from universe import load_universe  # zakładam, że zwraca listę tickerów


START_DATE = "2015-01-01"
END_DATE = "2025-12-05"
MONTHLY_CONTRIBUTION = 2000.0
TOP_N = 5


# ============================
# POMOCNICZE FUNKCJE
# ============================

def download_prices(tickers, start, end):
    """
    Pobiera dane z Yahoo Finance (Close) dla listy tickerów.
    Zwraca DataFrame o indeksie datowym i kolumnach = tickery.
    """
    data = yf.download(
        tickers=tickers,
        start=start,
        end=end,
        progress=False,
        auto_adjust=False,
        group_by="ticker"
    )

    # Jeśli jeden ticker -> struktura jest inna
    if isinstance(tickers, str):
        close = data["Close"].to_frame(name=tickers)
    else:
        # multi-index (ticker, field) -> wybieramy Close
        close = data.xs("Close", axis=1, level=1)

    close = close.dropna(how="all")
    return close


def compute_momentum_scores(price_df, asof_date, months_3=63, months_6=126, months_12=252):
    """
    Liczy ROC3/6/12 oraz score dla wszystkich tickerów na dzień asof_date.
    Zwraca Series: score per ticker (tylko tickery z kompletem danych).
    months_* -> liczba sesji (szacunkowo; 21 dni/msc)
    """
    if asof_date not in price_df.index:
        # jeśli brak notowania dokładnie tego dnia, używamy ostatniego wcześniejszego
        prev_dates = price_df.index[price_df.index <= asof_date]
        if len(prev_dates) == 0:
            return pd.Series(dtype=float)
        asof_date = prev_dates[-1]

    idx = price_df.index.get_loc(asof_date)

    def roc_n(n):
        if idx - n < 0:
            return None
        past = price_df.iloc[idx - n]
        today = price_df.iloc[idx]
        return (today / past - 1.0) * 100.0  # w %

    roc3 = roc_n(months_3)
    roc6 = roc_n(months_6)
    roc12 = roc_n(months_12)

    if roc12 is None:
        return pd.Series(dtype=float)

    # score = 0.4 * 12m + 0.3 * 6m + 0.3 * 3m
    score = 0.4 * roc12 + 0.3 * roc6 + 0.3 * roc3
    score = score.dropna()
    return score


def compute_regime_spy(spy_close: pd.Series, asof_date, lookback_sma=200, lookback_roc=252):
    """
    Prosta wersja Strategy A:
    - BULL jeśli:
        cena > SMA200 oraz ROC12 > 0
      w przeciwnym razie BEAR
    """
    if asof_date not in spy_close.index:
        prev_dates = spy_close.index[spy_close.index <= asof_date]
        if len(prev_dates) == 0:
            return "BEAR"
        asof_date = prev_dates[-1]

    idx = spy_close.index.get_loc(asof_date)
    if idx < lookback_sma or idx < lookback_roc:
        return "BEAR"  # za mało danych

    window_sma = spy_close.iloc[idx - lookback_sma + 1: idx + 1]
    sma200 = window_sma.mean()
    price_today = spy_close.iloc[idx]

    price_past = spy_close.iloc[idx - lookback_roc]
    roc12 = (price_today / price_past - 1.0) * 100.0  # w %

    if price_today > sma200 and roc12 > 0:
        return "BULL"
    else:
        return "BEAR"


def get_rebalance_dates(price_index):
    """
    Zwraca listę dat rebalansów:
    - 10 dzień każdego miesiąca
    - jeśli 10 to nie sesja, bierzemy najbliższą sesję PO 10 (>=10)
    """
    dates = []
    start = price_index[0]
    end = price_index[-1]

    current = datetime(start.year, start.month, 10)
    while current <= end:
        # znajdź najbliższą datę sesyjną >= current
        candidates = price_index[(price_index >= current)]
        if len(candidates) > 0:
            d = candidates[0]
            if d <= end:
                dates.append(d)
        # przechodzimy do kolejnego miesiąca
        year = current.year + (current.month // 12)
        month = current.month % 12 + 1
        current = datetime(year, month, 10)

    return dates


# ============================
# GŁÓWNY BACKTEST
# ============================

def run_backtest():
    print("=== BACKTEST: 2015-01-01 → 2025-12-05 ===")

    # ---------- 1. Universe + dane ----------
    universe = load_universe()
    # zakładam, że to jest lista tickerów; jeśli DataFrame, wybierz columnę z tickerami
    if not isinstance(universe, (list, tuple, pd.Index)):
        try:
            universe = list(universe["ticker"])
        except Exception:
            universe = list(universe)

    tickers = sorted(set(universe))
    print(f"[DATA] Universe: {len(tickers)} spółek")

    # SPY do regime
    spy_close = download_prices("SPY", START_DATE, END_DATE)["SPY"]

    # dane dla całego universum
    prices = download_prices(tickers, START_DATE, END_DATE)
    common_idx = prices.index.intersection(spy_close.index)
    prices = prices.loc[common_idx]
    spy_close = spy_close.loc[common_idx]

    # ---------- 2. Daty rebalansu ----------
    rebalance_dates = get_rebalance_dates(prices.index)
    print(f"[INFO] Liczba rebalansów: {len(rebalance_dates)}")

    # ---------- 3. Zmienne portfela ----------
    cash = 0.0
    positions = {}  # ticker -> ilość
    trade_log = []  # lista dictów z transakcjami
    equity_curve = []

    total_contributions = 0.0

    # pomocniczo: kiedy otwarto pozycję (dla statystyk)
    position_entry_price = {}
    position_entry_date = {}

    for d in prices.index:
        # aktualna wartość portfela
        equity = cash
        for t, qty in positions.items():
            if t in prices.columns and not np.isnan(prices.at[d, t]):
                equity += qty * prices.at[d, t]
        equity_curve.append({"date": d, "equity": equity})

        # REBALANS?
        if d in rebalance_dates:
            # 1) comiesięczna wpłata
            cash += MONTHLY_CONTRIBUTION
            total_contributions += MONTHLY_CONTRIBUTION

            # 2) SPY regime
            regime = compute_regime_spy(spy_close, d)

            if regime == "BEAR":
                # sprzedajemy wszystko
                for t, qty in list(positions.items()):
                    price_exit = prices.at[d, t]
                    pnl_abs = qty * price_exit
                    pnl_pct = (price_exit / position_entry_price[t] - 1.0) * 100.0

                    trade_log.append({
                        "ticker": t,
                        "entry_date": position_entry_date[t],
                        "exit_date": d,
                        "entry_price": position_entry_price[t],
                        "exit_price": price_exit,
                        "pnl_abs": pnl_abs,
                        "pnl_pct": pnl_pct,
                    })
                    cash += qty * price_exit
                    del positions[t]
                    del position_entry_price[t]
                    del position_entry_date[t]

                # w BEAR nie otwieramy nowych pozycji
                continue

            # 3) BULL -> wybór TOP5 momentum
            scores = compute_momentum_scores(prices, d)
            if scores.empty:
                continue

            top = scores.sort_values(ascending=False).head(TOP_N)
            target_tickers = list(top.index)

            # 4) Sprzedajemy wszystko (prosty model: pełny rebalance)
            for t, qty in list(positions.items()):
                price_exit = prices.at[d, t]
                pnl_abs = qty * price_exit
                pnl_pct = (price_exit / position_entry_price[t] - 1.0) * 100.0

                trade_log.append({
                    "ticker": t,
                    "entry_date": position_entry_date[t],
                    "exit_date": d,
                    "entry_price": position_entry_price[t],
                    "exit_price": price_exit,
                    "pnl_abs": pnl_abs,
                    "pnl_pct": pnl_pct,
                })
                cash += qty * price_exit
                del positions[t]
                del position_entry_price[t]
                del position_entry_date[t]

            # 5) Kupujemy TOP5 za całość equity (cash)
            if cash <= 0:
                continue

            equity_after_sell = cash
            alloc_per_ticker = equity_after_sell / len(target_tickers)

            for t in target_tickers:
                price = prices.at[d, t]
                if np.isnan(price) or price <= 0:
                    continue

                qty = alloc_per_ticker // price
                if qty <= 0:
                    continue

                cost = qty * price
                if cost > cash:
                    continue

                cash -= cost
                positions[t] = positions.get(t, 0.0) + qty
                position_entry_price[t] = price
                position_entry_date[t] = d

    # ---------- 4. Zamknięcie pozostałych pozycji na końcu backtestu ----------
    last_date = prices.index[-1]
    for t, qty in list(positions.items()):
        price_exit = prices.at[last_date, t]
        pnl_abs = qty * price_exit
        pnl_pct = (price_exit / position_entry_price[t] - 1.0) * 100.0

        trade_log.append({
            "ticker": t,
            "entry_date": position_entry_date[t],
            "exit_date": last_date,
            "entry_price": position_entry_price[t],
            "exit_price": price_exit,
            "pnl_abs": pnl_abs,
            "pnl_pct": pnl_pct,
        })
        cash += qty * price_exit

    # końcowe equity
    final_equity = cash
    equity_curve.append({"date": last_date, "equity": final_equity})

    equity_df = pd.DataFrame(equity_curve).drop_duplicates(subset=["date"]).set_index("date").sort_index()
    trades_df = pd.DataFrame(trade_log)

    # jeśli nie było żadnych trade'ów:
    if trades_df.empty:
        print("Brak transakcji w backteście.")
        return

    # ============================
    # STATYSTYKI
    # ============================

    num_trades = len(trades_df)
    wins = trades_df[trades_df["pnl_pct"] > 0]
    losses = trades_df[trades_df["pnl_pct"] <= 0]

    win_rate = len(wins) / num_trades * 100.0
    loss_rate = 100.0 - win_rate

    total_return = (final_equity / total_contributions - 1.0) * 100.0

    # equity curve: max gain, max DD
    eq = equity_df["equity"]
    running_max = eq.cummax()
    drawdown = (eq / running_max - 1.0) * 100.0
    max_drawdown = drawdown.min()
    max_runup = ((running_max / running_max.cummin()) - 1.0) * 100.0
    max_runup_value = max_runup.max()

    avg_pnl = trades_df["pnl_pct"].mean()
    avg_gain = wins["pnl_pct"].mean() if len(wins) > 0 else 0.0
    avg_loss = losses["pnl_pct"].mean() if len(losses) > 0 else 0.0

    biggest_win = trades_df["pnl_pct"].max()
    biggest_loss = trades_df["pnl_pct"].min()

    gross_profit = wins["pnl_abs"].sum()
    gross_loss = losses["pnl_abs"].sum()
    profit_factor = gross_profit / abs(gross_loss) if gross_loss != 0 else np.inf

    # średni czas trwania transakcji (dni)
    trades_df["duration_days"] = (trades_df["exit_date"] - trades_df["entry_date"]).dt.days
    avg_duration = trades_df["duration_days"].mean()

    print("\n===== WYNIKI BACKTESTU =====")
    print(f"Liczba transakcji:               {num_trades}")
    print(f"% transakcji zyskownych:         {win_rate:6.2f} %")
    print(f"% transakcji stratnych:          {loss_rate:6.2f} %")
    print(f"Średni P&L na transakcji:        {avg_pnl:6.2f} %")
    print(f"Średni zysk na transakcji:       {avg_gain:6.2f} %")
    print(f"Średnia strata na transakcji:    {avg_loss:6.2f} %")
    print(f"Stosunek śr. zysku do śr. straty:{(avg_gain / abs(avg_loss)) if avg_loss != 0 else np.inf:6.2f}")
    print(f"Największy zysk na transakcji:   {biggest_win:6.2f} %")
    print(f"Największa strata na transakcji: {biggest_loss:6.2f} %")
    print(f"Profit factor (wsp. zysku):      {profit_factor:6.2f}")
    print()
    print(f"Łączne wpłaty:                   {total_contributions:,.2f}")
    print(f"Końcowe equity:                  {final_equity:,.2f}")
    print(f"Zysk netto:                      {final_equity - total_contributions:,.2f}")
    print(f"Zysk netto %:                    {total_return:6.2f} %")
    print()
    print(f"Maksymalny wzrost equity (run-up): {max_runup_value:6.2f} %")
    print(f"Maksymalny drawdown:               {max_drawdown:6.2f} %")
    print(f"Średni czas trwania transakcji:    {avg_duration:6.1f} dni")

    # zapis raportów do plików (opcjonalnie)
    equity_df.to_csv("reports/backtest_equity.csv")
    trades_df.to_csv("reports/backtest_trades.csv", index=False)
    print("\nRaporty zapisane do:")
    print("  reports/backtest_equity.csv")
    print("  reports/backtest_trades.csv")


if __name__ == "__main__":
    run_backtest()
