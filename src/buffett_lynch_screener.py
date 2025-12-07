import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime

# =============================================================
# UNIVERSE – na start możesz wrzucić tu S&P100 / swoje tickery
# =============================================================

UNIVERSE_TICKERS = [
    # Na razie przykładowy zestaw dużych spółek – ZMIEŃ / ROZBUDUJ
    "AAPL", "MSFT", "GOOGL", "AMZN", "META",
    "NVDA", "BRK-B", "JPM", "JNJ", "XOM",
    "PG", "HD", "V", "MA", "AVGO",
    "LLY", "UNH", "PEP", "KO", "MRK",
    "ABBV", "ADBE", "CSCO", "PFE", "TMO",
    "MCD", "CRM", "NKE", "INTC", "DIS",
    "WMT", "BAC", "COST", "LIN", "AMD",
    "TXN", "ORCL", "QCOM", "IBM", "AMAT",
    "HON", "NEE", "LOW", "PM", "UPS",
    "SBUX", "RTX", "CVX", "CAT", "GE",
]


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
    """Zamienia serię na percentyle 0–100."""
    if series.nunique() <= 1:
        return pd.Series(50.0, index=series.index)

    if higher_is_better:
        return series.rank(pct=True) * 100.0
    else:
        # niższa wartość = lepiej
        return (1.0 - series.rank(pct=True)) * 100.0


# =============================================================
# POBIERANIE FUNDAMENTÓW Z YFINANCE
# =============================================================

def fetch_fundamentals(tickers):
    """
    Pobiera podstawowe dane fundamentalne z yfinance.Ticker.info.
    UWAGA: to są DANE BIEŻĄCE, nie historyczne!
    """
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
            "roic_approx": safe_get(info, "returnOnEquity"),  # przybliżenie
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
            # RISK / BALANCE
            "beta": safe_get(info, "beta"),
            "debt_to_equity": safe_get(info, "debtToEquity"),
            "current_ratio": safe_get(info, "currentRatio"),
            "quick_ratio": safe_get(info, "quickRatio"),
            "total_debt": safe_get(info, "totalDebt"),
            "free_cashflow": safe_get(info, "freeCashflow"),
        }

        records.append(rec)

    df = pd.DataFrame(records)
    df.set_index("ticker", inplace=True)
    return df


def fetch_price_volatility(tickers, window_days=252):
    """
    Liczy roczną zmienność cenową na podstawie log-zwrotów.
    """
    print("[DATA] Pobieram dane cenowe dla zmienności...")
    data = yf.download(
        tickers,
        period="1y",
        interval="1d",
        auto_adjust=True,
        threads=True,
        progress=False,
    )

    if isinstance(data, pd.DataFrame) and "Close" in data.columns:
        close = data["Close"]
    else:
        close = data

    vols = {}
    for t in tickers:
        if t not in close.columns:
            vols[t] = np.nan
            continue
        series = close[t].dropna()
        if len(series) < 30:
            vols[t] = np.nan
            continue
        rets = np.log(series).diff().dropna()
        vols[t] = rets.std() * np.sqrt(252)

    vol_df = pd.DataFrame({"price_vol": vols})
    return vol_df


# =============================================================
# LICZENIE SCORE’ÓW: QUALITY / VALUE / GROWTH / RISK
# =============================================================

def compute_scores(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()

    # QUALITY
    df["q_roic"] = percentile_rank(df["roic_approx"], higher_is_better=True)
    df["q_gross"] = percentile_rank(df["gross_margin"], higher_is_better=True)
    df["q_oper"] = percentile_rank(df["oper_margin"], higher_is_better=True)
    df["q_profit"] = percentile_rank(df["profit_margin"], higher_is_better=True)

    # EPS stability i FCF pozytywny na razie w przybliżeniu
    df["fcf_positive"] = df["free_cashflow"].apply(
        lambda x: 1.0 if (not pd.isna(x) and x > 0) else 0.0
    )
    df["q_fcf_flag"] = df["fcf_positive"] * 100.0

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
    df["g_rev"] = percentile_rank(df["revenue_growth"], higher_is_better=True)
    df["g_earn"] = percentile_rank(df["earnings_growth"], higher_is_better=True)

    df["GrowthScore"] = (
        0.50 * df["g_rev"] +
        0.50 * df["g_earn"]
    )

    # RISK
    df["r_de"] = percentile_rank(df["debt_to_equity"], higher_is_better=False)
    df["r_beta"] = percentile_rank(df["beta"], higher_is_better=False)
    df["r_vol"] = percentile_rank(df["price_vol"], higher_is_better=False)

    # interest coverage nie mamy łatwo, więc na razie pomijamy
    df["RiskScore"] = (
        0.40 * df["r_de"] +
        0.30 * df["r_beta"] +
        0.30 * df["r_vol"]
    )

    # TOTAL SCORE
    df["TotalScore"] = (
        0.40 * df["QualityScore"] +
        0.25 * df["ValueScore"] +
        0.20 * df["GrowthScore"] +
        0.15 * df["RiskScore"]
    )

    return df


# =============================================================
# MARKET REGIME – SPY vs SMA200 (prosty filtr)
# =============================================================

def get_market_regime():
    """
    Określa czy rynek jest w BULL czy BEAR na podstawie SPY vs SMA200.
    """
    import yfinance as yf

    spy = yf.download("SPY", start="1990-01-01", progress=False)

    if spy.empty:
        print("[WARN] SPY data not loaded – assuming BULL regime")
        return "BULL"

    close = spy["Close"]
    sma200 = close.rolling(200).mean()

    last_close = float(close.iloc[-1])
    last_sma = float(sma200.iloc[-1])

    if last_close >= last_sma:
        return "BULL"
    else:
        return "BEAR"

# =============================================================
# GŁÓWNA FUNKCJA: SCREENER BUFFETT/LYNCH 2.0
# =============================================================

def run_screener(top_n=15):
    print("\n=== Buffett/Lynch 2.0 – Fundamental Screener ===\n")
    today = datetime.now().strftime("%Y-%m-%d")
    print(f"[INFO] Dzisiaj: {today}\n")

    regime = get_market_regime()
    print(f"[REGIME] SPY regime: {regime}\n")

    # 1. Dane fundamentalne
    fundamentals = fetch_fundamentals(UNIVERSE_TICKERS)

    # 2. Zmienność cenowa (risk)
    vol_df = fetch_price_volatility(UNIVERSE_TICKERS)
    data = fundamentals.join(vol_df, how="left")

    # 3. Wywalenie śmieci z kompletnie pustymi danymi
    data = data.dropna(how="all")
    if data.empty:
        print("[ERROR] Brak danych po połączeniu – sprawdź tickery / yfinance.")
        return

    # 4. Liczymy score’y
    scored = compute_scores(data)

    # 5. Filtry bezpieczeństwa
    #    (ValueScore ≥ 40, cokolwiek z Quality > 0, price_vol nie NaN)
    filtered = scored[
        (scored["ValueScore"] >= 40) &
        (~scored["price_vol"].isna())
    ].copy()

    if filtered.empty:
        print("[WARN] Po filtrach nic nie zostało – strategia byłaby w T-Billach.\n")
        return

    # 6. Sortujemy po TotalScore malejąco
    filtered.sort_values("TotalScore", ascending=False, inplace=True)

    # 7. Sektorowy sanity check: max 30% sektor
    max_sector_weight = 0.30
    max_by_sector = max(1, int(top_n * max_sector_weight))

    final_rows = []
    sector_counts = {}

    for ticker, row in filtered.iterrows():
        sec = row["sector"]
        cnt = sector_counts.get(sec, 0)
        if cnt >= max_by_sector:
            continue
        final_rows.append(row)
        sector_counts[sec] = cnt + 1
        if len(final_rows) >= top_n:
            break

    if not final_rows:
        print("[WARN] Po limitach sektorowych nic nie weszło do TOP listy.\n")
        return

    final_df = pd.DataFrame(final_rows)
    final_df = final_df.sort_values("TotalScore", ascending=False)

    # 8. Wynik
    print("\n=== TOP kandydaci do portfela (Buffett/Lynch 2.0) ===\n")
    cols_show = [
        "sector",
        "TotalScore",
        "QualityScore",
        "ValueScore",
        "GrowthScore",
        "RiskScore",
        "roic_approx",
        "gross_margin",
        "oper_margin",
        "profit_margin",
        "revenue_growth",
        "earnings_growth",
        "pfcf",
        "pe",
        "ev_to_ebitda",
        "debt_to_equity",
        "beta",
        "price_vol",
    ]
    print(final_df[cols_show].round(3))

    # 9. Zapis do CSV (opcjonalnie)
    out_path = "reports/buffett_lynch_candidates.csv"
    import os
    os.makedirs("reports", exist_ok=True)
    final_df.to_csv(out_path)
    print(f"\n[OK] Zapisano listę kandydatów do: {out_path}\n")


if __name__ == "__main__":
    run_screener(top_n=15)
