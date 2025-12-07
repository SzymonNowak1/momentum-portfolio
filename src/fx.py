import yfinance as yf
import pandas as pd

# Ticker-y FX z yfinance
FX_TICKERS = {
    "USD": "USDPLN=X",
    "EUR": "EURPLN=X",
}

# Fallback, gdyby nie udało się pobrać danych
FALLBACK = {
    "USD": 4.00,
    "EUR": 4.40,
}


def _download_single_ccy(ccy: str, ticker: str, period: str) -> pd.Series:
    """Pomocniczo pobiera jedną walutę, a jak się nie uda – używa fallback."""
    try:
        df = yf.download(ticker, period=period, auto_adjust=True)
        if df.empty:
            raise ValueError("empty data")
        s = df["Close"].copy()
        s.name = ccy
        return s
    except Exception as e:
        print(f"[FX] ERROR dla {ticker}: {e}, używam fallback={FALLBACK[ccy]}")
        return pd.Series(
            FALLBACK[ccy],
            index=pd.date_range(end=pd.Timestamp.today().normalize(), periods=1),
            name=ccy,
        )


def load_fx_history(period: str = "10y") -> pd.DataFrame:
    """
    Zwraca historię kursów USD/PLN i EUR/PLN.
    Kolumny: 'USD', 'EUR', 'PLN'.
    """
    series_list = []
    for ccy, ticker in FX_TICKERS.items():
        s = _download_single_ccy(ccy, ticker, period)
        series_list.append(s)

    fx = pd.concat(series_list, axis=1)
    fx["PLN"] = 1.0
    fx = fx.ffill()
    return fx


def load_fx_row() -> pd.Series:
    """
    Zwraca OSTATNI dostępny wiersz z kursami (dzisiejszy / ostatni dzień).
    Używamy tego w main.py jako 'dzisiejsze FX'.
    """
    fx = load_fx_history("10y")
    row = fx.iloc[-1]
    row.name = "FX_TODAY"
    return row
