import yfinance as yf
import pandas as pd

FX_TICKERS = {
    "USD": "USDPLN=X",
    "EUR": "EURPLN=X",
}

FALLBACK = {
    "USD": 4.00,
    "EUR": 4.40,
}


def load_fx_history(period: str = "10y") -> pd.DataFrame:
    """
    Pobiera historię kursów USD/PLN i EUR/PLN z yfinance.
    Zawsze zwraca kolumny: 'USD', 'EUR', 'PLN'.
    Przy problemach z danymi używa fallbacków.
    """
    fx_data = {}

    for ccy, ticker in FX_TICKERS.items():
        try:
            df = yf.download(ticker, period=period, auto_adjust=True)

            if df.empty:
                print(f"[FX] WARNING: brak danych dla {ticker}, używam fallback={FALLBACK[ccy]}")
                s = pd.Series(FALLBACK[ccy], index=pd.date_range(end=pd.Timestamp.today(), periods=1))
            else:
                s = df["Close"].copy()

            s.name = ccy
            fx_data[ccy] = s

        except Exception as e:
            print(f"[FX] ERROR przy {ticker}: {e}, używam fallback={FALLBACK[ccy]}")
            fx_data[ccy] = pd.Series(FALLBACK[ccy], index=pd.date_range(end=pd.Timestamp.today(), periods=1))

    fx = pd.concat(fx_data, axis=1)
    fx["PLN"] = 1.0
    fx = fx.ffill()

    return fx
