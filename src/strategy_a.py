import pandas as pd


def compute_regime(spy):
    """
    Wylicza tryb rynku SP500 (BULL / BEAR) na podstawie cen SPY.

    Parametr `spy` może być:
      - Series z cenami zamknięcia
      - DataFrame z kolumną 'Close' / 'price' / 'Adj Close' itp.
    """

    # --- Upewniamy się, że mamy Series z ceną ---
    if isinstance(spy, pd.DataFrame):
        # preferowane nazwy kolumn
        preferred_cols = ["Close", "close", "Adj Close", "adj_close", "price"]

        col = None
        for c in preferred_cols:
            if c in spy.columns:
                col = c
                break

        # jeśli nic nie znaleziono, bierzemy pierwszą kolumnę
        if col is None:
            spy_series = spy.iloc[:, 0]
        else:
            spy_series = spy[col]
    else:
        # zakładamy, że to już jest Series
        spy_series = spy

    # sprzątamy NaN-y
    spy_series = spy_series.dropna()
    if spy_series.empty:
        print("[Strategy A] Brak danych SPY – zwracam UNKNOWN")
        return "UNKNOWN"

    # --- Budujemy pomocniczy DataFrame ---
    df = pd.DataFrame({"price": spy_series})
    df["SMA200"] = df["price"].rolling(200).mean()

    last = df.iloc[-1]

    # jeśli jeszcze nie mamy wyliczonej SMA200 (za mało danych)
    if pd.isna(last["SMA200"]):
        print("[Strategy A] Za mało danych do SMA200 – zwracam UNKNOWN")
        return "UNKNOWN"

    # prosty warunek trendu
    regime = "BULL" if last["price"] >= last["SMA200"] else "BEAR"
    return regime
