from datetime import date
from data_loader import load_price_history
from strategy_a import compute_regime

def main():
    print("=== Momentum Portfolio Engine ===")

    # --- 1. Pobieramy dane SPY (S&P500 ETF) ---
    spy_df = load_price_history("SPY", period="15y")

    # --- 2. Liczymy filtr makro ---
    regime_df = compute_regime(spy_df["SPY"])

    # --- 3. Dzisiejszy reżim ---
    today = regime_df.index[-1]
    today_regime = regime_df["regime"].iloc[-1]

    print(f"[Strategy A] Date: {today.date()}")
    print(f"[Strategy A] SP500 Regime today: {today_regime}")

    # Debug info:
    print(regime_df.tail(3))


if __name__ == "__main__":
    main()
