# src/buffett_lynch_portfolio.py

import pandas as pd


def compute_quality_weights(df: pd.DataFrame,
                            quality_col: str = "QualityScore",
                            min_weight: float = 0.02,
                            max_weight: float = 0.25) -> pd.Series:
    """
    Zwraca wektor wag portfela opartych o QualityScore.

    Wagi ~ QualityScore_i / sum(QualityScore_j),
    z ograniczeniami:
      - min_weight (np. 2%)
      - max_weight (np. 25%)

    Parametry:
        df          : DataFrame z kolumną quality_col
        quality_col : nazwa kolumny z QualityScore
        min_weight  : minimalna waga na spółkę (po capowaniu)
        max_weight  : maksymalna waga na spółkę (po capowaniu)

    Zwraca:
        pd.Series z wagami (index = tickery, suma = 1.0)
    """
    if quality_col not in df.columns:
        raise ValueError(f"Brak kolumny '{quality_col}' w df. "
                         f"Masz kolumny: {list(df.columns)}")

    # Bierzemy tylko dodatnie QualityScore
    q = df[quality_col].clip(lower=0.01)

    # Wstępne proporcje
    w = q / q.sum()

    # Cap/floor
    w = w.clip(lower=min_weight, upper=max_weight)

    # Normalizacja do 1
    w = w / w.sum()

    return w


def build_portfolio(candidates: pd.DataFrame,
                    top_n: int = 15,
                    quality_col: str = "QualityScore") -> pd.DataFrame:
    """
    Buduje portfel Buffett/Lynch 2.0 na podstawie kandydatów.

    Zakłada, że `candidates` ma przynajmniej kolumny:
      - 'ticker' (lub index = ticker)
      - 'QualityScore'
      - 'TotalScore' (do sortowania)

    Kroki:
      1. Sortuj po TotalScore malejąco
      2. Wybierz TOP N
      3. Wylicz wagi wg QualityScore
      4. Zwróć DataFrame z kolumną 'weight'
    """

    df = candidates.copy()

    # Jeżeli tickery są w indeksie, przerzuć do kolumny
    if "ticker" not in df.columns:
        df = df.reset_index().rename(columns={"index": "ticker"})

    if "TotalScore" not in df.columns:
        raise ValueError("Brak kolumny 'TotalScore' w candidates.")

    # TOP N po TotalScore (Buffett/Lynch 2.0)
    df_top = df.sort_values("TotalScore", ascending=False).head(top_n).copy()

    # Wagi wg QualityScore
    weights = compute_quality_weights(df_top, quality_col=quality_col)

    df_top["weight"] = weights.values

    # Dla porządku: posortuj malejąco po wadze
    df_top = df_top.sort_values("weight", ascending=False).reset_index(drop=True)

    return df_top
