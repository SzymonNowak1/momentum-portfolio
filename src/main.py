from datetime import date
from data_loader import load_price_history
from strategy_a import compute_regime

def main():
    print("=== Momentum Portfolio Engine ===")

    # --- 1. Pobieramy dane SPY ---
    spy_df = load_price_history("SPY", period="15y")

    print("\n[DEBUG] spy_df columns:", spy_df.columns)
    print("[DEBUG] spy_df head:")
    print(spy_df.head())

    # jeśli kolumna nie nazywa się SPY, automatycznie ją znajdziemy
    price_col = spy_df.columns[0]

    print(f"\n[DEBUG] Using price column: {price_col}")

    # --- 2. Liczymy filtr makro ---
    regime_df = compute_regime(spy_df[price_col])

    # --- 3. Wyniki ---
    today = regime_df.index[-1]
    today_regime = regime_df["regime"].iloc[-1]

    print(f"\n[Strategy A] Date: {today.date()}")
    print(f"[Strategy A] SP500 Regime today: {today_regime}")

    print("\n[DEBUG] Last rows:")
    print(regime_df.tail())


if __name__ == "__main__":
    main()
