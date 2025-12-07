import pandas as pd

def compute_regime(spy: pd.Series) -> pd.DataFrame:
    """
    Strategy A: filtr makro dla SP500.
    Zwraca ramkę z kolumną "regime" = 'BULL' albo 'BEAR'.
    spy – to powinna być Series z cenami SPY.
    """
    # Z Series robimy DataFrame z kolumną "price"
    df = spy.to_frame(name="price")

    # SMA200
    df["SMA200"] = df["price"].rolling(200).mean()

    # ROC 12 miesięcy
    df["ROC12"] = (df["price"] / df["price"].shift(252) - 1.0) * 100

    # Reżim rynku
    df["regime"] = df.apply(
        lambda row: "BULL" if row["price"] > row["SMA200"] and row["ROC12"] > 0 else "BEAR",
        axis=1
    )

    return df
