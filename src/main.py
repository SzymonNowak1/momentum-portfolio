from datetime import datetime
import traceback

from data_loader import load_price_history
from fx import load_fx_row
from strategy_a import compute_regime
from momentum import compute_top5_momentum
from universe import load_universe
from db import init_db

from portfolio_storage import (
    load_positions,
    estimate_total_equity,
    record_contribution,
)

from portfolio import (
    process_sell_signals,
    execute_sell_orders,
    build_target_allocation,
)

from trade_engine import (
    sell_all_positions_for_rebalance,
    buy_according_to_allocation,
)

from contribution import check_contribution_day


# ============================================================
# Helper – czy dziś dzień rebalancingu?
# 10-ty dzień miesiąca, jeśli to dzień roboczy
# ============================================================
def is_rebalance_day(today: datetime) -> bool:
    if today.day != 10:
        return False
    # weekend (5 = sobota, 6 = niedziela)
    if today.weekday() >= 5:
        return False
    return True


# ============================================================
# MAIN ENGINE
# ============================================================
def main():
    print("\n=== Momentum Portfolio Engine v2 (synchronizacja DB) ===\n")

    today_dt = datetime.now()
    today = today_dt.strftime("%Y-%m-%d")
    print(f"[INFO] Today: {today}\n")

    # inicjalizacja bazy (tabele, jeśli brak)
    init_db()
    print("[DB] SQLite portfolio database initialized.\n")

    # ========================================================
    # 1. STRATEGIA A — MARKET REGIME (SP500)
    # ========================================================
    print("[A] Ładuję historię SPY...")
    spy_df = load_price_history("SPY", period="15y")

    print("[A] Obliczam tryb rynku SP500...")
    # kluczowa zmiana: używamy kolumny 'Close', a nie nieistniejącej 'SPY'
    regime = compute_regime(spy_df["Close"])
    print(f"[A] Dzisiejszy tryb rynku = {regime}\n")

    # ========================================================
    # 2. STRATEGIA B — MOMENTUM TOP 5
    # ========================================================
    print("\n[B] Obliczam ranking momentum dla US...")

    # 1. Ładuję dane dla wszystkich tickerów w universe
    universe = load_universe()
    price_data = load_price_history(universe, today)

    # 2. Momentum ranking
    top5 = compute_top5_momentum(price_data)

    print("[B] TOP5 momentum:", top5)
    
    # ========================================================
    # 3. FX RATES
    # ========================================================
    fx_row = load_fx_row()
    print("[FX] Dzisiejsze kursy:")
    print(fx_row, "\n")
    
    # ========================================================
    # 4. LOAD PORTFOLIO STATE
    # ========================================================
    positions = load_positions()
    print("[PORTFOLIO] Obecne pozycje:")
    print(positions if not positions.empty else "(brak pozycji)", "\n")

    # Uwaga: teraz price_data istnieje i equity się policzy!
    equity = estimate_total_equity(price_data, fx_row)
    print(f"[PORTFOLIO] Łączne equity portfela: {equity:,.2f} PLN\n")
    
    
    # ========================================================
    # 5. SELL SIGNALS (natychmiastowe)
    # ========================================================
    print("[SELL] Sprawdzam sygnały sprzedaży...\n")

    price_data = {}
    tickers_to_check = set(top5) | set(positions["ticker"].tolist()) if not positions.empty else set(top5)

    for t in tickers_to_check:
        try:
            df = load_price_history(t, period="2y")
            price_data[t] = df
        except Exception as e:
            print(f"[WARN] [SELL] Brak danych dla {t}: {e}")

    sell_list = process_sell_signals(
        today=today,
        regime_a=regime,
        top5_tickers=top5,
        price_data=price_data,
        fx_row=fx_row,
    )

    execute_sell_orders(
        today=today,
        sell_list=sell_list,
        price_data=price_data,
        fx_row=fx_row,
        regime=regime,
    )

    # reload portfolio after sells
    positions = load_positions()

    # ========================================================
    # 6. BUY / REBALANCE — ONLY MONTHLY (10th)
    # ========================================================
    if is_rebalance_day(today_dt):
        print("[BUY] Dzisiaj dzień miesięcznego rebalancingu.")

        # Stała wpłata 2000 PLN (dla testów wstecznych)
        contribution = 2000
        print(f"[BUY] Dodaję miesięczną wpłatę: {contribution} PLN")
        record_contribution(today, contribution)

        positions = load_positions()
        equity = estimate_total_equity(positions, fx_row)
        print(f"[BUY] Equity po wpłacie = {equity:,.2f} PLN")

        # Budujemy target allocation
        alloc_df = build_target_allocation(
            tickers=top5,
            weights=None,   # equal weight
            equity_pln=equity,
            fx_row=fx_row,
        )

        print("\n[BUY] Target allocation:")
        print(alloc_df)

        print("\n[BUY] ✨ Tu w kolejnych krokach dodamy wykonywanie BUY transaction ✨\n")
    else:
        print("[BUY] Dziś NIE jest dzień rebalancingu.\n")

    print("\n=== ENGINE COMPLETE ===\n")


# ============================================================
# RUN
# ============================================================
if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("\n[ERROR] Wystąpił błąd w engine:")
        print(traceback.format_exc())
        exit(1)
