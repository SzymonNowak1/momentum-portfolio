import pandas as pd
import numpy as np


def compute_momentum(df: pd.DataFrame) -> pd.DataFrame:
    """
    Dodaje kolumny:
        ROC3, ROC6, ROC12, score
    """
    df = df.copy()

    df["ROC3"] = df["Close"].pct_change(63) * 100     # 3 months
    df["ROC6"] = df["Close"].pct_change(126) * 100    # 6 months
    df["ROC12"] = df["Close"].pct_change(252) * 100   # 12 months

    df["score"] = df["ROC3"] * 0.2 + df["ROC6"] * 0.3 + df["ROC12"] * 0.5

    return df


def compute_top5_momentum(price_data: dict) -> list:
    """
    Przyjmuje:
       price_data: dict[ticker] = DataFrame z kolumną 'Close'

    Zwraca:
        list TOP5 tickerów na podstawie score
    """

    results = []

    for ticker, df in price_data.items():
        mom = compute_momentum(df)
        score = mom["score"].iloc[-1]
        roc3 = mom["ROC3"].iloc[-1]
        roc6 = mom["ROC6"].iloc[-1]
        roc12 = mom["ROC12"].iloc[-1]

        results.append({
            "ticker": ticker,
            "score": score,
            "roc3": roc3,
            "roc6": roc6,
            "roc12": roc12
        })

    mom_df = pd.DataFrame(results)
    mom_df = mom_df.sort_values("score", ascending=False).reset_index(drop=True)

    # Debug wypis TOP5
    print("\n[Strategy B] TOP 5 momentum today:")
    for i, row in mom_df.head(5).iterrows():
        print(f"{i+1}. {row['ticker']}: score={row['score']:.2f}, "
              f"roc3={row['roc3']:.1f}%, roc6={row['roc6']:.1f}%, roc12={row['roc12']:.1f}%")

    return mom_df["ticker"].head(5).tolist()
