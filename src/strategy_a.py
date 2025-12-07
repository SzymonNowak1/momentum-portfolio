import pandas as pd


def compute_regime(spy: pd.Series) -> pd.DataFrame:
    """
    Strategy A: filtr makro dla SP500 oparty na SMA200 i ROC12.
    spy – Series z cenami SPY (Close).
    Zwraca DataFrame z kolumnami:
      ['price', 'SMA200', 'ROC12', 'regime']
    gdzie 'regime' to 'BULL' albo 'BEAR'.
    """
    df = spy.to_frame(name="price")

    df["SMA200"] = df["price"].rolling(200).mean()
    df["ROC12"] = (df["price"] / df["price"].shift(252) - 1.0) * 100.0

    def _regime(row):
        if row["price"] > row["SMA200"] and row["ROC12"] > 0:
            return "BULL"
        return "BEAR"

    df["regime"] = df.apply(_regime, axis=1)
    return df
