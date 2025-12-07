import yfinance as yf
import pandas as pd

FX_TICKERS = {
    "USD": "USDPLN=X",
    "EUR": "EURPLN=X",
    # PLN jako baza
}

def load_fx_history(period: str = "10y") -> pd.DataFrame:
    """
    Pobiera historię kursów USD/PLN i EUR/PLN.
    Zwraca DataFrame z kolumnami 'USD', 'EUR', 'PLN'.
    """
    frames = []

    for ccy, fx_ticker in FX_TICKERS.items():
        data = yf.download(fx_ticker, period=period, auto_adjust=True)
        close = data["Close"].copy()
        close.name = ccy
        frames.append(close)

    fx = pd.concat(frames, axis=1)
    fx["PLN"] = 1.0  # kurs PLN/PLN

    return fx
