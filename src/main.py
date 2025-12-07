from data_loader import load_price_history, load_price_history_multi
from strategy_a import compute_regime
from universe import get_us_universe
from momentum import compute_momentum_scores
from fx import load_fx_history
from portfolio import build_target_allocation
from db import init_db

def main():
    print("=== Momentum Portfolio Engine V1 ===")

    # --- 1. Strategia A: filtr SP500 (SPY) ---
    spy_df = load_price_history("SPY", period="15y")
    price_col = spy_df.columns[0]

    regime_df = compute_regime(spy_df[price_col])

    today = regime_df.index[-1]
    today_regime = regime_df["regime"].iloc[-1]

    print(f"[Strategy A] Date: {today.date()}")
    print(f"[Strategy A] SP500 Regime today: {today_regime}")

    # --- 2. Strategia B: TOP5 momentum dla US universe (tylko w BULL) ---
    top5 = None
    if today_regime == "BULL":
        print("\n[Strategy B] Regime = BULL -> liczę TOP5 momentum dla US universe...")

        tickers = get_us_universe()
        prices = load_price_history_multi(tickers, period="5y")

        mom_df = compute_momentum_scores(prices)

        print("\n[Strategy B] TOP 5 momentum today:")
        top5 = mom_df.head(5)
        for i, row in top5.iterrows():
            print(
                f"  {i+1}. {row['ticker']}: "
                f"score={row['score']:.2f}, "
                f"roc3={row['roc3']:.1f}%, roc6={row['roc6']:.1f}%, roc12={row['roc12']:.1f}%"
            )
    else:
        print("\n[Strategy B] Regime = BEAR -> teoretycznie trzymamy się safe asset (np. ZPR1.DE).")

    # --- 3. FX do PLN ---
    print("\n[FX] Ładuję kursy USD/PLN i EUR/PLN...")
    fx_hist = load_fx_history(period="10y")

    # bierzemy ostatni kurs nie późniejszy niż 'today'
    fx_today = fx_hist.loc[fx_hist.index <= today].iloc[-1]

    print("[FX] Dzisiejsze FX:")
    print(fx_today)

    # --- 4. Docelowa alokacja portfela ---
    test_equity_pln = 100_000.0  # na razie "na sztywno", tylko do podglądu

    alloc_df = build_target_allocation(
        equity_pln=test_equity_pln,
        regime=today_regime,
        top5=top5,
        fx_row=fx_today,
        safe_asset="ZPR1.DE",
    )

    print(f"\n[Portfolio] Target allocation for equity = {test_equity_pln:,.0f} PLN:")
    print(alloc_df.to_string(index=False))


if __name__ == "__main__":
    main()
