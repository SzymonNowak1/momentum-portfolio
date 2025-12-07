from __future__ import annotations
import pandas as pd
from typing import List, Dict


def generate_sell_signals(
    regime: str,
    top5: List[str],
    full_universe: List[str],
    momentum_df: pd.DataFrame,
    roc12_threshold: float = -0.05,
) -> Dict[str, str]:
    """
    Zwraca słownik:
      { "TICKER": "REASON", ... }

    Możliwe powody:
      - "BEAR"
      - "MOMENTUM"
      - "NOT_IN_TOP5"
    """

    signals = {}

    # PRIORYTET 1 → BEAR = SELL EVERYTHING
    if regime == "BEAR":
        for ticker in full_universe:
            signals[ticker] = "BEAR"
        return signals

    # PRIORYTET 2 → Momentum SELL
    for ticker in full_universe:
        roc12 = float(momentum_df.loc[momentum_df["ticker"] == ticker, "roc12"])
        if roc12 < roc12_threshold:
            signals[ticker] = "MOMENTUM"

    # PRIORYTET 3 → nie jest już w TOP5
    for ticker in full_universe:
        if ticker not in top5:
            # ale tylko jeśli akcja istnieje w portfelu
            signals.setdefault(ticker, "NOT_IN_TOP5")

    return signals
