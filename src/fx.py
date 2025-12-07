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
    Pobiera historię kursów USD/PLN i EUR/PLN.
    Gdy Yahoo zwróci pusty wynik -> używa fallback.
    Zawsze zwraca kolumny: USD, EUR, PLN.
    """

    fx_data = {}

    for ccy, ticker in FX_TICKERS.items():
        try:
            df = yf.download(ticker, period=period, auto_adjust=True)

            # Jeśli Yahoo zwróci pusty DataFrame → fallback
            if df.empty:
                print(f"[FX] WARNING: Yahoo did not return data for {ticker}. Using fallback={FALLBACK[ccy]}")
                fx_series = pd.Series(FALLBACK[ccy], index=pd.date_range(end=pd.Timestamp.today(), periods=1))
            else:
                fx_series = df["Close"]

            fx_series.name = ccy
            fx_data[ccy] = fx_series

        except Exception as e:
            # Awaryjne fallback w razie błędu pobrania
            print(f"[FX] ERROR downloading {ticker}: {e}. Using fallback={FALLBACK[ccy]}")
            fx_data[ccy] = pd.Series(FALLBACK[ccy], index=pd.date_range(end=pd.Timestamp.today(), periods=1))

    # scal dane
    fx = pd.concat(fx_data, axis=1)

    # dodaj PLN jako stałą wartość
    fx["PLN"] = 1.0

    # forward-fill, żeby wszystkie daty były dostępne
    fx = fx.ffill()

    return fx
