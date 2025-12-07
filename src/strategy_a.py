import pandas as pd

def compute_regime(spy: pd.Series) -> pd.DataFrame:
    """
    Strategy A: filtr makro dla SP500.
    Zwraca ramkę z kolumną "regime" = 'BULL' albo 'BEAR'.
    """
    df = pd.DataFrame(spy)
    df["SMA200"] = df[spy.name].rolling(200).mean()
    df["ROC12"] = (df[spy.name] / df[spy.name].shift(252) - 1.0) * 100

    df["regime"] = df.apply(
        lambda row: "BULL" if row[spy.name] > row["SMA200"] and row["ROC12"] > 0 else "BEAR",
        axis=1
    )

    return df
