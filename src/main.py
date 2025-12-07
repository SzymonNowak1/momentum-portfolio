from datetime import date

from db import init_db
from data_loader import load_price_history, load_price_history_multi
from strategy_a import compute_regime
from universe import get_us_universe
from momentum import compute_momentum_scores
from fx import load_fx_history
from portfolio import build_target_allocation
from portfolio_storage import load_positions
from sell_rules import generate_sell_signals
from contribution import is_contribution_day
from trade_engine import sell_all_positions_for_rebalance, buy_according_to_allocation
from contribution import is_contribution_day

def estimate_equity_pln(positions_df, fx_row):
    """
    Szacuje wartość portfela w PLN na podstawie:
      units * last_price * FX
    Jeśli nie ma pozycji -> zwraca 100_000 jako "kapitał startowy" (do testów).
    """
    if positions_df.empty:
        return 100_000.0

    total_pln = 0.0
    for _, row in positions_df.iterrows():
        ticker = row["ticker"]
        units = float(row["units"])
        currency = row["currency"]

        # pobieramy ostatnią cenę (kolumna = ticker)
        price_df = load_price_history(ticker, period="6mo")
        last_price = price_df.iloc[-1, 0]

        fx = fx_row.get(currency, 1.0)
        value_pln = units * last_price * fx
        total_pln += value_pln

    return total_pln


def main():
    print("=== Momentum Portfolio Engine v2 (sygnałowy) ===")

    # --- 0. Baza danych ---
    init_db()
    print("[DB] SQLite portfolio database initialized.")

    today = date.today()
    print(f"[INFO] Today: {today.isoformat()}")

    # --- 1. Strategia A: filtr SP500 (SPY) ---
    spy_df = load_price_history("SPY", period="15y")
    price_col = spy_df.columns[0]

    regime_df = compute_regime(spy_df[price_col])
    regime_today = regime_df["regime"].iloc[-1]
    regime_date = regime_df.index[-1].date()

    print(f"[Strategy A] Last regime date: {regime_date}")
    print(f"[Strategy A] SP500 Regime today: {regime_today}")

    # --- 2. Strategia B: TOP5 momentum dla US universe (tylko w BULL) ---
    top5_df = None
    top5_tickers = []

    tickers = get_us_universe()
    prices = load_price_history_multi(tickers, period="5y")
    mom_df = compute_momentum_scores(prices)

    if regime_today == "BULL":
        print("\n[Strategy B] Regime = BULL -> liczę TOP5 momentum dla US universe...")

        top5_df = mom_df.head(5)
        top5_tickers = top5_df["ticker"].tolist()

        print("\n[Strategy B] TOP 5 momentum today:")
        for i, row in top5_df.iterrows():
            print(
                f"  {i+1}. {row['ticker']}: "
                f"score={row['score']:.2f}, "
                f"roc3={row['roc3']:.1f}%, roc6={row['roc6']:.1f}%, roc12={row['roc12']:.1f}%"
            )
    else:
        print("\n[Strategy B] Regime = BEAR -> teoretycznie trzymamy się safe asset (ZPR1.DE).")

    # --- 3. FX do PLN ---
    print("\n[FX] Ładuję kursy USD/PLN i EUR/PLN...")
    fx_hist = load_fx_history(period="10y")
    fx_today_row = fx_hist.iloc[-1]

    print("[FX] Dzisiejsze FX (ostatni dostępny):")
    print(fx_today_row)

    # --- 4. Wczytanie aktualnych pozycji z bazy ---
    positions_df = load_positions()
    print("\n[PORTFOLIO] Current positions from DB:")
    if positions_df.empty:
        print("  (no positions yet)")
    else:
        print(positions_df.to_string(index=False))

    # --- 5. Szacowana wartość portfela ---
    equity_pln = estimate_equity_pln(positions_df, fx_today_row)
    print(f"\n[PORTFOLIO] Estimated total equity: {equity_pln:,.2f} PLN")

    # --- 6. Czy dziś jest dzień dopłaty / rebalansu? ---
    is_contrib = is_contribution_day(today)
    if is_contrib:
        print("\n[CONTRIBUTION] Today IS a contribution & rebalance day (10th or shifted).")
    else:
        print("\n[CONTRIBUTION] Today is NOT a contribution day.")

    # --- 7. Sygnały SELL (jeszcze bez wykonywania transakcji) ---
    held_tickers = positions_df["ticker"].tolist() if not positions_df.empty else []

    if held_tickers:
        sell_signals = generate_sell_signals(
            regime=regime_today,
            top5=top5_tickers,
            held_tickers=held_tickers,
            momentum_df=mom_df,
            roc12_threshold=-5.0,
        )

        print("\n[SELL SIGNALS] Generated signals for held positions:")
        if not sell_signals:
            print("  (no sell signals today)")
        else:
            for t, reason in sell_signals.items():
                print(f"  SELL {t}: reason = {reason}")
    else:
        print("\n[SELL SIGNALS] No held positions -> nothing to check.")

    # --- 8. Docelowa alokacja (teoretyczna) ---
    if regime_today == "BULL":
        alloc_df = build_target_allocation(
            equity_pln=equity_pln,
            regime=regime_today,
            top5=top5_df,
            fx_row=fx_today_row,
            safe_asset="ZPR1.DE",
        )
    else:
        alloc_df = build_target_allocation(
            equity_pln=equity_pln,
            regime=regime_today,
            top5=None,
            fx_row=fx_today_row,
            safe_asset="ZPR1.DE",
        )

    print(f"\n[Portfolio] Target allocation for equity = {equity_pln:,.0f} PLN:")
    print(alloc_df.to_string(index=False))


if __name__ == "__main__":
    main()
