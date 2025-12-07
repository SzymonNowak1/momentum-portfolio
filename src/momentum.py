import pandas as pd


def compute_momentum_scores(prices: pd.DataFrame) -> pd.DataFrame:
    """
    Liczy ROC 3M / 6M / 12M i wynikowy score dla każdej kolumny (tickera).
    prices: DataFrame, index = daty, kolumny = tickery.
    Zwraca DataFrame z ostatniego dnia z kolumnami:
      ['ticker', 'roc3', 'roc6', 'roc12', 'score']
    posortowany malejąco po score.
    """

    # ~21 sesji w miesiącu
    roc3 = prices.pct_change(63)    # 3 miesiące
    roc6 = prices.pct_change(126)   # 6 miesięcy
    roc12 = prices.pct_change(252)  # 12 miesięcy

    last_date = prices.index[-1]

    roc3_last = roc3.loc[last_date]
    roc6_last = roc6.loc[last_date]
    roc12_last = roc12.loc[last_date]

    df = pd.DataFrame({
        "ticker": prices.columns,
        "roc3": roc3_last.values * 100.0,
        "roc6": roc6_last.values * 100.0,
        "roc12": roc12_last.values * 100.0,
    })

    # prosty momentum score (wagi można później stroić)
    df["score"] = 0.3 * df["roc3"] + 0.3 * df["roc6"] + 0.4 * df["roc12"]

    df = df.dropna()
    df = df.sort_values("score", ascending=False).reset_index(drop=True)

    return df
