# src/datasets/sp500_source.py
import pandas as pd
import os


def fetch_sp500_slickcharts():
    """
    Pobiera aktualny skład S&P500 z SlickCharts.
    Struktura tabeli jest stabilna od wielu lat.
    """
    url = "https://www.slickcharts.com/sp500"
    df = pd.read_html(url)[0]

    df = df[["Company", "Symbol", "Weight", "Sector"]]
    df.columns = ["name", "ticker", "weight", "sector"]

    # BRK.B → BRK-B
    df["ticker"] = df["ticker"].str.replace(".", "-")

    return df


def load_sp500_constituents(year: int):
    """
    Jeśli istnieje historyczny plik w repo → ładujemy.
    Jeśli nie istnieje → pobieramy bieżący skład SP500 z slickcharts,
    zapisujemy go do CSV jako wszechświat dla roku 'year'.
    """

    path = f"data/sp500/{year}.csv"

    # --------------------------------------
    # 1. Jeśli plik istnieje → ładujemy
    # --------------------------------------
    if os.path.exists(path):
        return pd.read_csv(path)

    # --------------------------------------
    # 2. Jeśli brak pliku → pobieramy z internetu
    # --------------------------------------
    print(f"[INFO] Brak {path}. Pobieram aktualny skład SP500 z slickcharts…")

    df = fetch_sp500_slickcharts()

    os.makedirs("data/sp500", exist_ok=True)
    df.to_csv(path, index=False)

    print(f"[INFO] Zapisano nowy wszechświat SP500 → {path}")

    return df
