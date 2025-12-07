# src/backtest_buffett_like.py

"""
UWAGA:
To jest PROTOTYP backtestu w stylu Buffett/Lynch 2.0,
ale oparty WYŁĄCZNIE o dane cenowe (brak bazy historycznych fundamentów).

Celem tego pliku jest:
  - przetestowanie MECHANIKI portfela (wagi, rebalans, T-Bill),
  - przygotowanie gotowego silnika, do którego później
    będzie można podpiąć prawdziwe historyczne QualityScore / ValueScore / itd.

Dlatego traktuj wyniki tego backtestu jako ORIENTACYJNE,
a nie „prawdę objawioną Buffetta” :)
"""

from datetime import datetime
import pandas as pd
import numpy as np

from data_loader import load_price_history
from universe import load_universe
from buffett_lynch_portfolio import build_portfolio


def compute_price_based_quality(prices: pd.Series,
                                lookback_days: int = 252 * 3) -> float:
    """
    Pseudo-QualityScore na bazie ceny:
      - CAGR za ostatnie ~3 lata
      - zmienność dzienna
      - prosty Sharpe

    Im lepsza relacja zysku do ryzyka, tym wyższy score.
    To NIE jest prawdziwy Buffett, ale daje nam „testową jakość”.
    """

    if prices.shape[0] < 252:  # mniej niż rok danych – słaba wiarygodność
        return np.nan

    # Ogranicz do lookback
    px = prices.dropna().iloc[-lookback_days:]

    if px.shape[0] < 60:
        return np.nan

    ret = px.pct_change().dropna()
    if ret.empty:
        return np.nan

    # CAGR
    total_return = px.iloc[-1] / px.iloc[0] - 1.0
    years = ret.shape[0] / 252.0
    if years <= 0:
        return np.nan
    cagr = (1.0 + total_return) ** (1 / years) - 1.0

    vol = ret.std() * np.sqrt(252)
    if vol == 0 or np.isnan(vol):
        return np.nan

    sharpe = cagr / vol

    # Sklejmy w prosty score [im więcej, tym lepiej]
    score = 0.6 * cagr + 0.4 * sharpe

    return float(score)


def build_scores_for_date(universe, price_panel, date) -> pd.DataFrame:
    """
    Dla danej daty:
      - bierze historyczne ceny do tej daty,
      - liczy pseudo-QualityScore,
      - zwraca DataFrame z kolumnami: ticker, QualityScore, TotalScore.
    """
    rows = []

    for ticker in universe:
        df = price_panel.get(ticker)
        if df is None or df.empty:
            continue

        # ceny do 'date'
        df_sub = df[df.index <= date]
        if df_sub.empty:
            continue

        q_score = compute_price_based_quality(df_sub["Close"])
        if np.isnan(q_score):
            continue

        rows.append({"ticker": ticker,
                     "QualityScore": q_score,
                     "TotalScore": q_score})  # na razie Total = Quality

    if not rows:
        return pd.DataFrame()

    scores_df = pd.DataFrame(rows)
    return scores_df


def run_backtest(start_date: str = "2015-01-01",
                 end_date: str = "2025-12-05",
                 top_n: int = 15,
                 rebalance_freq: str = "Q"):
    """
    Prosty backtest cenowy:
      - uniwersum: load_universe()
      - rebalans: co kwartał (domyślnie)
      - wagi: Buffett/Lynch 2.0 (wg QualityScore) + cap/floor
      - hedge: jeśli SPY < SMA200 -> 100% cash (tu: 0% ekspozycji na rynek)

    Wynik:
      - equity curve
      - statystyki P&L
      - pliki CSV w /reports
    """

    print("\n=== BACKTEST Buffett-like (cenowy prototyp) ===\n")
    print(f"Okres: {start_date} -> {end_date}")
    print(f"Top N: {top_n}, rebalans: {rebalance_freq}\n")

    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)

    universe = load_universe()
    print(f"[INFO] Uniwersum: {universe}\n")

    # ------------------------------------------------------
    # 1. Ładujemy ceny dla całego okresu (dla wszystkich tickerów)
    # ------------------------------------------------------
    price_panel = {}
    for ticker in universe + ["SPY"]:
        print(f"[data_loader] Ładuję dane dla {ticker}...")
        df = load_price_history(ticker, end=end_date)
        # filtrujemy do start_dt
        df = df[df.index >= start_dt]
        price_panel[ticker] = df

    # Daty tradingowe (bierzemy z SPY jako proxy rynku)
    spy = price_panel["SPY"]
    trading_days = spy.index[(spy.index >= start_dt) & (spy.index <= end_dt)]

    if trading_days.empty:
        print("[ERROR] Brak dat tradingowych w podanym okresie.")
        return

    # Daty rebalansu
    trading_calendar = pd.Series(index=trading_days, data=1.0)
    rebal_dates = trading_calendar.resample(rebalance_freq).last().dropna().index
    rebal_dates = [d for d in rebal_dates if d >= trading_days[0] and d <= trading_days[-1]]

    print(f"[INFO] Liczba dat rebalansu: {len(rebal_dates)}\n")

    # ------------------------------------------------------
    # 2. Symulacja portfela
    # ------------------------------------------------------
    equity = 1.0  # zaczynamy od 1.0
    equity_curve = []
    current_weights = {}  # ticker -> weight

    prev_date = trading_days[0]

    for date in trading_days:
        # Czy rebalans?
        if date in rebal_dates:
            # Najpierw zamykamy poprzedni „slot” equity
            equity_curve.append({"date": prev_date, "equity": equity})
            prev_date = date

            # Market regime (SPY vs SMA200)
            spy_sub = spy[spy.index <= date]
            if spy_sub.shape[0] < 200:
                regime_bull = True
            else:
                sma200 = spy_sub["Close"].rolling(200).mean().iloc[-1]
                close_spy = spy_sub["Close"].iloc[-1]
                regime_bull = close_spy >= sma200

            if not regime_bull:
                print(f"[{date.date()}] Regime = BEAR -> 100% cash (T-Bill).")
                current_weights = {}
            else:
                print(f"[{date.date()}] Regime = BULL -> buduję portfel...")

                scores_df = build_scores_for_date(universe, price_panel, date)

                if scores_df.empty:
                    print("  [WARN] Brak sensownych score'ów, zostajemy w cash.")
                    current_weights = {}
                else:
                    # Do build_portfolio potrzebujemy jeszcze np. kolumny 'sector'
                    scores_df["sector"] = "N/A"
                    portfolio_df = build_portfolio(scores_df, top_n=top_n,
                                                   quality_col="QualityScore")

                    current_weights = dict(
                        zip(portfolio_df["ticker"], portfolio_df["weight"])
                    )

                    print("  Nowy portfel:")
                    for t, w in current_weights.items():
                        print(f"    {t}: {w:.2%}")

        # Aktualizacja equity wg bieżących wag
        if current_weights:
            # dzienny zwrot portfela
            port_ret = 0.0
            for ticker, w in current_weights.items():
                df_t = price_panel.get(ticker)
                if df_t is None or date not in df_t.index:
                    continue
                # procentowa zmiana Close względem poprzedniego dnia
                idx = df_t.index.get_loc(date)
                if idx == 0:
                    continue
                px_today = df_t["Close"].iloc[idx]
                px_prev = df_t["Close"].iloc[idx - 1]
                r = (px_today / px_prev) - 1.0
                port_ret += w * r

            equity *= (1.0 + port_ret)

        equity_curve.append({"date": date, "equity": equity})

    # ------------------------------------------------------
    # 3. Statystyki + zapis
    # ------------------------------------------------------
    eq_df = pd.DataFrame(equity_curve).drop_duplicates(subset="date").set_index("date")
    eq_df = eq_df.sort_index()

    total_return = eq_df["equity"].iloc[-1] - 1.0
    years = (eq_df.index[-1] - eq_df.index[0]).days / 365.25
    cagr = (eq_df["equity"].iloc[-1]) ** (1 / years) - 1.0 if years > 0 else np.nan

    dd = eq_df["equity"] / eq_df["equity"].cummax() - 1.0
    max_dd = dd.min()

    print("\n=== WYNIKI BACKTESTU (prototyp cenowy) ===\n")
    print(f"Okres:         {start_date} -> {end_date}")
    print(f"Końcowe equity: {eq_df['equity'].iloc[-1]:.4f}")
    print(f"Całkowity zwrot: {total_return:.2%}")
    print(f"CAGR:            {cagr:.2%}")
    print(f"Maksymalny DD:   {max_dd:.2%}")

    # Zapis equity curve
    import os
    os.makedirs("reports", exist_ok=True)
    eq_df.to_csv("reports/buffett_like_equity.csv")
    print("\n[INFO] Zapisano krzywą kapitału do reports/buffett_like_equity.csv")
