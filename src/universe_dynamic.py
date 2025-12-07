import yfinance as yf
import pandas as pd
from pathlib import Path
from datetime import datetime, date

# Ścieżka do CSV z dynamicznym uniwersum
UNIVERSE_CSV = Path("data/top100_universe_by_year.csv")

# BAZOWA LISTA – używana:
#  - jako kandydaci do budowy TOP100
#  - jako fallback, jeśli CSV jeszcze nie istnieje
BASE_UNIVERSE = [
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


def build_top100_universe(start_year: int = 2000, end_year: int | None = None):
    """
    Buduje przybliżone TOP100 po kapitalizacji rynkowej
    dla każdego roku w zakresie [start_year, end_year].

    Uwaga:
      - używamy BASE_UNIVERSE jako kandydatów (czyli duże spółki, nie cały rynek),
      - kapitalizacja ≈ last_close * sharesOutstanding (dzisiaj),
      - to NIE jest idealnie akademicko czyste, ale:
          * minimalizuje ręczne dopasowywanie spółek,
          * daje dynamiczny ranking po latach,
          * jest realistyczne do zrobienia na darmowych danych.

    Wynik zapisuje do:
      data/top100_universe_by_year.csv
      z kolumnami: year, ticker, rank, market_cap, close_price, shares_outstanding
    """
    if end_year is None:
        end_year = datetime.now().year

    years = list(range(start_year, end_year + 1))

    UNIVERSE_CSV.parent.mkdir(parents=True, exist_ok=True)

    # Cache sharesOutstanding, żeby nie pytać API 100x na rok
    shares_cache: dict[str, float] = {}

    rows = []

    for year in years:
        print(f"\n[UNIVERSE] Buduję TOP100 dla roku {year}...")
        start = f"{year}-01-01"
        end = f"{year}-12-31"

        # multi-ticker download, group_by='ticker' daje MultiIndex: [ticker][OHLCV]
        data = yf.download(
            BASE_UNIVERSE,
            start=start,
            end=end,
            auto_adjust=True,
            group_by="ticker",
            progress=False,
            threads=True,
        )

        if data.empty:
            print(f"[WARN] Brak danych cenowych dla {year}, pomijam ten rok.")
            continue

        # Jeśli jest MultiIndex, pierwszy poziom = ticker
        if isinstance(data.columns, pd.MultiIndex):
            tickers_in_data = sorted({c[0] for c in data.columns})
        else:
            # teoretycznie nie powinno się zdarzyć przy wielu tickerach,
            # ale zostawiamy fallback
            tickers_in_data = BASE_UNIVERSE

        year_rows = []

        for ticker in tickers_in_data:
            # Bezpieczne wyjęcie serii Close
            try:
                if isinstance(data.columns, pd.MultiIndex):
                    if ticker not in data.columns.levels[0]:
                        continue
                    close_series = data[ticker]["Close"].dropna()
                else:
                    # w teorii: jeden ticker → one set of OHLC
                    close_series = data["Close"].dropna()
            except Exception:
                continue

            if close_series.empty:
                continue

            close_price = float(close_series.iloc[-1])

            # sharesOutstanding – z cache albo z API
            if ticker not in shares_cache:
                try:
                    info = yf.Ticker(ticker).info
                    so = info.get("sharesOutstanding", None)
                    if so is None:
                        shares_cache[ticker] = float("nan")
                    else:
                        shares_cache[ticker] = float(so)
                except Exception:
                    shares_cache[ticker] = float("nan")

            so = shares_cache.get(ticker, float("nan"))
            if not pd.notna(so) or so <= 0:
                # jeśli nie mamy sharesOutstanding, nie policzymy market cap sensownie
                continue

            mcap = close_price * so  # w USD, przybliżenie

            year_rows.append(
                {
                    "year": year,
                    "ticker": ticker,
                    "close_price": close_price,
                    "shares_outstanding": so,
                    "market_cap": mcap,
                }
            )

        if not year_rows:
            print(f"[WARN] Rok {year}: nie udało się wyliczyć market cap dla żadnej spółki.")
            continue

        # sort po market_cap malejąco, TOP100
        year_df = pd.DataFrame(year_rows)
        year_df = year_df.sort_values("market_cap", ascending=False).head(100)
        year_df["rank"] = range(1, len(year_df) + 1)

        rows.extend(year_df.to_dict(orient="records"))

        print(
            f"[UNIVERSE] Rok {year}: zapisano {len(year_df)} spółek "
            f"(TOP{len(year_df)}) na podstawie market cap."
        )

    if not rows:
        print("[ERROR] Nie udało się zbudować żadnego roku TOP100 – plik nie zostanie zapisany.")
        return

    out_df = pd.DataFrame(rows)
    out_df = out_df.sort_values(["year", "rank"])
    out_df.to_csv(UNIVERSE_CSV, index=False)

    print(f"\n[OK] Zapisano dynamiczne uniwersum do: {UNIVERSE_CSV}")


def _load_universe_csv() -> pd.DataFrame | None:
    if not UNIVERSE_CSV.exists():
        print("[WARN] Plik dynamicznego uniwersum nie istnieje – używam BASE_UNIVERSE.")
        return None
    try:
        df = pd.read_csv(UNIVERSE_CSV)
        return df
    except Exception as e:
        print(f"[WARN] Nie udało się wczytać {UNIVERSE_CSV}: {e}")
        return None


def load_universe_for_year(year: int) -> list[str]:
    """
    Zwraca listę tickerów TOP100 dla podanego roku.
    Jeśli CSV nie istnieje lub brak danych dla tego roku → zwraca BASE_UNIVERSE.
    """
    df = _load_universe_csv()
    if df is None or df.empty:
        return BASE_UNIVERSE

    # wybieramy najbliższy rok <= requested
    uniq_years = sorted(df["year"].unique())
    candidate_years = [y for y in uniq_years if y <= year]
    if not candidate_years:
        target_year = max(uniq_years)
    else:
        target_year = max(candidate_years)

    df_year = df[df["year"] == target_year].sort_values("rank")
    if df_year.empty:
        print(f"[WARN] Brak wpisów dla roku {year}, używam BASE_UNIVERSE.")
        return BASE_UNIVERSE

    tickers = df_year["ticker"].tolist()
    return tickers


def load_universe_for_date(as_of: datetime | date | None = None) -> list[str]:
    """
    Wygodny wrapper: podajesz datę → dostajesz uniwersum dla danego roku.
    Jeśli as_of = None → bierze dzisiejszą datę.
    """
    if as_of is None:
        as_of = datetime.now().date()
    elif isinstance(as_of, datetime):
        as_of = as_of.date()

    year = as_of.year
    return load_universe_for_year(year)


if __name__ == "__main__":
    # Pozwala ręcznie wywołać w GitHub Actions albo lokalnie (gdybyś kiedyś mógł)
    build_top100_universe(start_year=2000)
