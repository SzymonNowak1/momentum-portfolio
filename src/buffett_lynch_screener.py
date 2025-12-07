import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime
from buffett_lynch_portfolio import build_portfolio
from universe_dynamic import load_universe_for_date, BASE_UNIVERSE
from datetime import datetime

# Jeśli dynamiczne uniwersum jest zbudowane → wczytamy je z pliku,
# jeśli nie → load_universe_for_date() zwróci BASE_UNIVERSE.
UNIVERSE_TICKERS = load_universe_for_date(datetime.now())


# =============================================================
# HELPERY
# =============================================================

def safe_get(info: dict, key: str, default=np.nan):
    v = info.get(key, default)
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def percentile_rank(series: pd.Series, higher_is_better=True) -> pd.Series:
    if series.nunique() <= 1:
        return pd.Series(50.0, index=series.index)

    if higher_is_better:
        return series.rank(pct=True) * 100
    else:
        return (1 - series.rank(pct=True)) * 100


# =============================================================
# FUNDAMENTALS
# =============================================================

def fetch_fundamentals(tickers):
    records = []
    for t in tickers:
        print(f"[DATA] Pobieram info dla {t}...")
        try:
            info = yf.Ticker(t).info
        except Exception as e:
            print(f"[WARN] Nie udało się pobrać info dla {t}: {e}")
            continue

        rec = {
            "ticker": t,
            "sector": info.get("sector", "Unknown"),
            # QUALITY
            "roic_approx": safe_get(info, "returnOnEquity"),
            "roe": safe_get(info, "returnOnEquity"),
            "gross_margin": safe_get(info, "grossMargins"),
            "oper_margin": safe_get(info, "operatingMargins"),
            "profit_margin": safe_get(info, "profitMargins"),
            # GROWTH
            "revenue_growth": safe_get(info, "revenueGrowth"),
            "earnings_growth": safe_get(info, "earningsGrowth"),
            # VALUE
            "pe": safe_get(info, "trailingPE"),
            "forward_pe": safe_get(info, "forwardPE"),
            "pb": safe_get(info, "priceToBook"),
            "ps": safe_get(info, "priceToSalesTrailing12Months"),
            "pfcf": safe_get(info, "priceToFreeCashFlows"),
            "ev_to_ebitda": safe_get(info, "enterpriseToEbitda"),
            "ev_to_revenue": safe_get(info, "enterpriseToRevenue"),
            # RISK
            "beta": safe_get(info, "beta"),
            "debt_to_equity": safe_get(info, "debtToEquity"),
            "current_ratio": safe_get(info, "currentRatio"),
            "quick_ratio": safe_get(info, "quickRatio"),
            "total_debt": safe_get(info, "totalDebt"),
            "free_cashflow": safe_get(info, "freeCashflow"),
        }
        records.append(rec)

    df = pd.DataFrame(records).set_index("ticker")
    return df


# =============================================================
# PRICE VOLATILITY
# =============================================================

def fetch_price_volatility(tickers, window_days=252):
    print("[DATA] Pobieram dane cenowe dla zmienności...")

    data = yf.download(
        tickers,
        period="1y",
        interval="1d",
        auto_adjust=True,
        threads=True,
        progress=False,
    )

    close = data["Close"] if "Close" in data.columns else data
    vols = {}

    for t in tickers:
        if t not in close.columns:
            vols[t] = np.nan
            continue
        s = close[t].dropna()
        if len(s) < 30:
            vols[t] = np.nan
            continue
        rets = np.log(s).diff().dropna()
        vols[t] = rets.std() * np.sqrt(252)

    return pd.DataFrame({"price_vol": vols})


# =============================================================
# SCORE CALCULATION
# =============================================================

def compute_scores(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()

    # QUALITY
    df["q_roic"] = percentile_rank(df["roic_approx"])
    df["q_gross"] = percentile_rank(df["gross_margin"])
    df["q_oper"] = percentile_rank(df["oper_margin"])
    df["q_profit"] = percentile_rank(df["profit_margin"])
    df["q_fcf_flag"] = df["free_cashflow"].apply(lambda x: 100 if (not pd.isna(x) and x > 0) else 0)

    df["QualityScore"] = (
        0.35 * df["q_roic"] +
        0.20 * df["q_gross"] +
        0.20 * df["q_oper"] +
        0.15 * df["q_profit"] +
        0.10 * df["q_fcf_flag"]
    )

    # VALUE
    df["v_pfcf"] = percentile_rank(df["pfcf"], higher_is_better=False)
    df["v_pe"] = percentile_rank(df["pe"], higher_is_better=False)
    df["v_ev_ebitda"] = percentile_rank(df["ev_to_ebitda"], higher_is_better=False)
    df["v_ev_sales"] = percentile_rank(df["ev_to_revenue"], higher_is_better=False)

    df["ValueScore"] = (
        0.40 * df["v_pfcf"] +
        0.30 * df["v_ev_ebitda"] +
        0.20 * df["v_pe"] +
        0.10 * df["v_ev_sales"]
    )

    # GROWTH
    df["g_rev"] = percentile_rank(df["revenue_growth"])
    df["g_earn"] = percentile_rank(df["earnings_growth"])

    df["GrowthScore"] = 0.5 * df["g_rev"] + 0.5 * df["g_earn"]

    # RISK
    df["r_de"] = percentile_rank(df["debt_to_equity"], higher_is_better=False)
    df["r_beta"] = percentile_rank(df["beta"], higher_is_better=False)
    df["r_vol"] = percentile_rank(df["price_vol"], higher_is_better=False)

    df["RiskScore"] = (
        0.40 * df["r_de"] +
        0.30 * df["r_beta"] +
        0.30 * df["r_vol"]
    )

    # TOTAL
    df["TotalScore"] = (
        0.40 * df["QualityScore"] +
        0.25 * df["ValueScore"] +
        0.20 * df["GrowthScore"] +
        0.15 * df["RiskScore"]
    )

    return df


# =============================================================
# MARKET REGIME
# =============================================================

def get_market_regime():
    spy = yf.download("SPY", start="1990-01-01", progress=False)

    if spy.empty:
        return "BULL"

    close = spy["Close"]
    sma200 = close.rolling(200).mean()

    if close.iloc[-1] >= sma200.iloc[-1]:
        return "BULL"
    return "BEAR"


# =============================================================
# MAIN SCREENER
# =============================================================

def run_screener(top_n=15):
    print("\n=== Buffett/Lynch 2.0 – Fundamental Screener ===\n")

    regime = get_market_regime()
    print(f"[REGIME] SPY: {regime}\n")

    # FUNDAMENTY
    fundamentals = fetch_fundamentals(UNIVERSE_TICKERS)

    # ZMIENNOŚĆ
    vol_df = fetch_price_volatility(UNIVERSE_TICKERS)

    # MERGE
    df = fundamentals.join(vol_df, how="left")
    df.dropna(how="all", inplace=True)

    if df.empty:
        print("[ERROR] Brak danych!")
        return

    # SCORE
    scored = compute_scores(df)

    # FILTRY
    filtered = scored[(scored["ValueScore"] >= 40) & (~scored["price_vol"].isna())]

    if filtered.empty:
        print("[WARN] Po filtrach: 0 spółek → T-Bill.")
        return

    # SORT
    filtered = filtered.sort_values("TotalScore", ascending=False)

    # LIMITY SEKTOROWE
    max_sector = max(1, int(top_n * 0.30))
    final = []
    sector_count = {}

    for t, row in filtered.iterrows():
        sec = row["sector"]
        if sector_count.get(sec, 0) < max_sector:
            final.append(row)
            sector_count[sec] = sector_count.get(sec, 0) + 1
        if len(final) == top_n:
            break

    final_df = pd.DataFrame(final)

    # ================================
    #   WYŚWIETLENIE TOP LISTY
    # ================================

    print("\n=== TOP kandydaci do portfela (Buffett/Lynch 2.0) ===\n")
    print(final_df[[
        "sector", "TotalScore", "QualityScore",
        "ValueScore", "GrowthScore", "RiskScore",
        "roic_approx", "gross_margin", "oper_margin",
        "profit_margin", "revenue_growth", "earnings_growth",
        "pfcf", "pe", "ev_to_ebitda", "debt_to_equity",
        "beta", "price_vol"
    ]].round(3))

    # zapis listy
    import os
    os.makedirs("reports", exist_ok=True)
    final_df.to_csv("reports/buffett_lynch_candidates.csv")
    print("\n[OK] Zapisano kandydatów → reports/buffett_lynch_candidates.csv")

    # ================================
    #     PORTFEL Z WAGAMI
    # ================================

    # final_df ma indeks = tickery → build_portfolio rozpozna ticker
    portfolio = build_portfolio(final_df, top_n=top_n, quality_col="QualityScore")

    print("\n=== PORTFEL (Buffett/Lynch 2.0) — wagi jakościowe ===\n")
    print(portfolio[["ticker", "sector", "QualityScore", "TotalScore", "weight"]])

    portfolio.to_csv("reports/buffett_lynch_portfolio_today.csv", index=False)
    print("\n[OK] Zapisano portfel → reports/buffett_lynch_portfolio_today.csv")


if __name__ == "__main__":
    run_screener(top_n=15)
