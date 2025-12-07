import yfinance as yf
import pandas as pd

def load_price_history(ticker: str, period: str = "10y") -> pd.DataFrame:
    """
    Pobiera dane cenowe dla 1 ticker'a i zwraca DataFrame z jedną kolumną (Close),
    nazwaną tickerem.
    """

    raw = yf.download(ticker, period=period, auto_adjust=True)

    # wybieramy tylko Close, upewniamy się że to Series
    close = raw["Close"].copy()

    # nadajemy nazwę kolumnie
    close.name = ticker

    # zamieniamy na DataFrame
    df = pd.DataFrame(close)

    return df

def load_price_history_multi(tickers, period: str = "10y") -> pd.DataFrame:
    """
    Pobiera dane dla wielu tickerów naraz.
    Zwraca DataFrame: wiersze = daty, kolumny = tickery.
    """
    data = yf.download(tickers, period=period, auto_adjust=True)

    # yfinance dla wielu tickerów zwykle zwraca MultiIndex (np. ('Close','AAPL'), ('Close','MSFT'), ...)
    if isinstance(data.columns, pd.MultiIndex):
        # bierzemy tylko poziom 'Close'
        if "Close" in data.columns.get_level_values(0):
            close = data["Close"]
        else:
            # fallback: próbujemy 'Adj Close'
            close = data["Adj Close"]
    else:
        # jeden poziom kolumn - zakładamy, że to same ceny
        close = data

    # upewniamy się, że kolumny to dokładnie nasze tickery (o ile się da)
    # usuwamy kolumny, których nie ma w liście tickers
    close = close.loc[:, [c for c in close.columns if c in tickers]]

    return close
