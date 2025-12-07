# src/data_loader.py

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Dict, Union

import pandas as pd
import yfinance as yf

# Katalog z danymi: data/prices/*.csv
DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "prices"
DATA_DIR.mkdir(parents=True, exist_ok=True)


# -------------------------------------------------------------
# Pomocnicze: ścieżka, odczyt, zapis CSV
# -------------------------------------------------------------
def _csv_path(ticker: str) -> Path:
    return DATA_DIR / f"{ticker}.csv"


def _read_from_csv(ticker: str) -> pd.DataFrame | None:
    path = _csv_path(ticker)
    if not path.exists():
        return None

    df = pd.read_csv(path, parse_dates=["Date"])
    df.set_index("Date", inplace=True)
    df.sort_index(inplace=True)
    df["Ticker"] = ticker
    return df


def _download_full_from_yahoo(
    ticker: str,
    start: str = "2005-01-01",
    end: str | None = None,
) -> pd.DataFrame:
    """Pobiera pełną historię z Yahoo i nadpisuje CSV."""
    print(f"[data_loader] Pobieram pełną historię z Yahoo dla {ticker} ({start} → {end or 'today'})...")
    df = yf.download(
        ticker,
        start=start,
        end=end,
        auto_adjust=False,
        progress=False,
    )

    if df.empty:
        raise ValueError(f"[data_loader] Yahoo Finance zwrócił puste dane dla {ticker}")

    df = df.rename_axis("Date").reset_index()
    df.to_csv(_csv_path(ticker), index=False)

    df.set_index("Date", inplace=True)
    df.sort_index(inplace=True)
    df["Ticker"] = ticker
    return df


def _append_missing_from_yahoo(
    ticker: str,
    df: pd.DataFrame,
    end: str | None,
) -> pd.DataFrame:
    """Dociąga brakujące dni do istniejącego df i aktualizuje CSV."""
    if df.empty:
        return _download_full_from_yahoo(ticker, start="2005-01-01", end=end)

    last_date = df.index.max().normalize()
    end_ts = pd.to_datetime(end).normalize() if end else pd.Timestamp.today().normalize()

    if last_date >= end_ts:
        # Dane są aktualne
        return df

    start_dl = (last_date + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"[data_loader] Dociągam nowe dane z Yahoo dla {ticker} ({start_dl} → {end or 'today'})...")

    new_df = yf.download(
        ticker,
        start=start_dl,
        end=end,
        auto_adjust=False,
        progress=False,
    )

    if new_df.empty:
        # Nic nowego – zostawiamy stare dane
        return df

    new_df = new_df.rename_axis("Date")
    # Usuń ewentualne duplikaty
    new_df = new_df[~new_df.index.isin(df.index)]

    merged = pd.concat([df.drop(columns=["Ticker"]), new_df], axis=0)
    merged.sort_index(inplace=True)

    # Zapis z powrotem do CSV
    merged.reset_index().to_csv(_csv_path(ticker), index=False)

    merged["Ticker"] = ticker
    return merged


# -------------------------------------------------------------
# Publiczne API: pojedynczy ticker
# -------------------------------------------------------------
def load_single_history(
    ticker: str,
    start: str = "2005-01-01",
    end: str | None = None,
    allow_download: bool = True,
) -> pd.DataFrame:
    """
    Ładuje historię dla pojedynczego tickera:

    - próbuje czytać z CSV,
    - jeśli trzeba i allow_download=True, dociąga brakujące dni z Yahoo,
    - zwraca DataFrame z indexem Date.
    """
    end_ts = pd.to_datetime(end).normalize() if end else pd.Timestamp.today().normalize()
    start_ts = pd.to_datetime(start).normalize()

    df = _read_from_csv(ticker)

    if df is None:
        if not allow_download:
            raise FileNotFoundError(f"[data_loader] Brak lokalnego pliku CSV dla {ticker}")
        df = _download_full_from_yahoo(ticker, start=start, end=end)
    else:
        if allow_download:
            try:
                df = _append_missing_from_yahoo(ticker, df, end=end)
            except Exception as exc:  # pragma: no cover
                print(f"[data_loader] Ostrzeżenie: nie udało się dociągnąć danych dla {ticker}: {exc}")

    # Przycięcie zakresu
    df = df[(df.index >= start_ts) & (df.index <= end_ts)]
    df["Ticker"] = ticker
    return df.copy()


# -------------------------------------------------------------
# Publiczne API: główna funkcja wykorzystywana w main.py
# -------------------------------------------------------------
def _period_to_days(period: str) -> int:
    """Bardzo prosty parser '15y', '5y', '12mo' itp."""
    period = period.lower()
    if period.endswith("y"):
        years = int(period[:-1])
        return int(years * 365.25)
    if period.endswith("mo"):
        months = int(period[:-2])
        return months * 30
    # fallback: 10 lat
    return 3650


def load_price_history(
    tickers: Union[str, Iterable[str]],
    as_of: pd.Timestamp | None = None,
    period: str = "15y",
    allow_download: bool = True,
) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
    """
    Uniwersalny loader:

    1) JEŚLI tickers jest stringiem (np. "SPY"):
       - ignoruje as_of,
       - używa parametru period (np. '15y'),
       - zwraca pojedynczy DataFrame.

       Używane m.in. do SPY w Strategii A.

    2) JEŚLI tickers jest listą/tuplą (np. universe) ORAZ as_of nie jest None:
       - liczy start = as_of - 20 lat,
       - dla każdego tickera zwraca historię w słowniku: {ticker: DataFrame}.

       Używane do wszechświata w Strategii B.
    """
    # --- przypadek 1: pojedynczy ticker, np. "SPY" ---
    if isinstance(tickers, str):
        end_ts = as_of if as_of is not None else pd.Timestamp.today().normalize()
        end_str = end_ts.strftime("%Y-%m-%d")

        days = _period_to_days(period)
        start_ts = end_ts - pd.Timedelta(days=days)
        start_str = start_ts.strftime("%Y-%m-%d")

        return load_single_history(
            tickers,
            start=start_str,
            end=end_str,
            allow_download=allow_download,
        )

    # --- przypadek 2: lista tickerów (universe) ---
    if as_of is None:
        raise ValueError("load_price_history(tickers=list, ...) wymaga parametru 'as_of'.")

    end_ts = pd.to_datetime(as_of).normalize()
    end_str = end_ts.strftime("%Y-%m-%d")

    # maksymalnie 20 lat wstecz dla wszechświata
    start_ts = end_ts - pd.DateOffset(years=20)
    start_str = start_ts.strftime("%Y-%m-%d")

    result: Dict[str, pd.DataFrame] = {}
    missing: list[str] = []

    for ticker in tickers:
        try:
            df = load_single_history(
                ticker,
                start=start_str,
                end=end_str,
                allow_download=allow_download,
            )
            result[ticker] = df
        except Exception as exc:
            print(f"[data_loader] BŁĄD dla {ticker}: {exc}")
            missing.append(ticker)

    if missing:
        # Przy realnym użyciu wolimy wiedzieć że brakuje danych
        raise ValueError(f"[data_loader] Brak danych dla: {missing}")

    return result
