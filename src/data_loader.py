import yfinance as yf
import pandas as pd

def load_price_history(ticker: str, period: str = "10y") -> pd.DataFrame:
    """
    Pobiera dane dla wybranego tickera (np. SPY, AAPL, MSFT)
    z yfinance, z auto-adjust (total return).
    """
    data = yf.download(ticker, period=period, auto_adjust=True)
    data = data[["Close"]].rename(columns={"Close": ticker})
    return data
