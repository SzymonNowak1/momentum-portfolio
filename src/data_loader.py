import yfinance as yf
import pandas as pd


def load_price_history(ticker: str, period: str = "10y") -> pd.DataFrame:
    """
    Pobiera dane dla pojedynczego tickera (np. SPY, AAPL).
    Zwraca DataFrame: index = daty, jedna kolumna = ticker.
    """
    raw = yf.download(ticker, period=period, auto_adjust=True)

    if raw.empty:
        raise ValueError(f"[data_loader] Brak danych dla {ticker}")

    close = raw["Close"].copy()
    close.name = ticker

    df = pd.DataFrame(close)
    return df


def load_price_history_multi(tickers, period: str = "10y") -> pd.DataFrame:
    """
    Pobiera dane dla wielu tickerów.
    Zwraca DataFrame: index = daty, kolumny = tickery.
    """
    data = yf.download(tickers, period=period, auto_adjust=True)

    if isinstance(data.columns, pd.MultiIndex):
        # standard: poziom 0 = 'Close' / 'Adj Close', poziom 1 = ticker
        if "Close" in data.columns.get_level_values(0):
            close = data["Close"]
        elif "Adj Close" in data.columns.get_level_values(0):
            close = data["Adj Close"]
        else:
            raise ValueError("[data_loader] Nie ma 'Close' ani 'Adj Close' w danych multi-ticker.")
    else:
        # pojedynczy poziom – zakładamy, że to już są ceny
        close = data

    # zostawiamy tylko te tickery, o które prosiliśmy
    cols = [c for c in close.columns if c in tickers]
    close = close.loc[:, cols]

    if close.empty:
        raise ValueError("[data_loader] Brak dopasowanych kolumn dla podanych tickerów.")

    return close
