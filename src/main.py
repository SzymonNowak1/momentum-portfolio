from datetime import date
from data_loader import load_price_history, load_price_history_multi
from strategy_a import compute_regime
from momentum import compute_momentum_scores
from universe import get_us_universe

def main():
    print("=== Momentum Portfolio Engine ===")

    # --- 1. Strategia A: SP500 Regime (SPY) ---
    spy_df = load_price_history("SPY", period="15y")
    price_col = spy_df.columns[0]
    regime_df = compute_regime(spy_df[price_col])

    today = regime_df.index[-1]
    today_regime = regime_df["regime"].iloc[-1]

    print(f"[Strategy A] Date: {today.date()}")
    print(f"[Strategy A] SP500 Regime today: {today_regime}")

    # --- 2. Jeśli BULL, liczmy momentum i TOP 5 ---
    if today_regime == "BULL":
        print("\n[Strategy B] Regime is BULL -> computing TOP 5 momentum for US universe...")

        tickers = get_us_universe()
        prices = load_price_history_multi(tickers, period="5y")

        # just in case: dopasowujemy index do SPY (opcjonalnie)
        # prices = prices.loc[prices.index.intersection(regime_df.index)]

        mom_df = compute_momentum_scores(prices)

        print("\n[Strategy B] TOP 5 momentum today:")
        top5 = mom_df.head(5)
        for i, row in top5.iterrows():
            print(f"  {i+1}. {row['ticker']}: score={row['score']:.2f}, "
                  f"roc3={row['roc3']:.1f}%, roc6={row['roc6']:.1f}%, roc12={row['roc12']:.1f}%")
    else:
        print("\n[Strategy B] Regime is BEAR -> docelowo trzymamy się ZPR1.DE (safe asset)")


if __name__ == "__main__":
    main()
