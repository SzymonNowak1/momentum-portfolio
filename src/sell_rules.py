from __future__ import annotations
import pandas as pd
from typing import List, Dict


def generate_sell_signals(
    regime: str,
    top5: List[str],
    held_tickers: List[str],
    momentum_df: pd.DataFrame,
    roc12_threshold: float = -5.0,
) -> Dict[str, str]:
    """
    Zwraca słownik:
      { "TICKER": "REASON", ... }

    Możliwe powody:
      - "BEAR"
      - "MOMENTUM"
      - "NOT_IN_TOP5"

    Zakładamy, że momentum_df ma kolumny:
      ['ticker', 'roc3', 'roc6', 'roc12', 'score']
    i wartości 'roc12' są wyrażone w procentach (np. 12.3, -4.5).
    """

    signals: Dict[str, str] = {}

    # PRIORYTET 1 → BEAR = SELL EVERYTHING (na razie tylko na posiadanych tickerach)
    if regime == "BEAR":
        for ticker in held_tickers:
            signals[ticker] = "BEAR"
        return signals

    # Mamy BULL – analizujemy tylko tickery, które faktycznie trzymamy
    mom_by_ticker = momentum_df.set_index("ticker")

    # PRIORYTET 2 → Momentum SELL (ROC12 < threshold)
    for ticker in held_tickers:
        if ticker not in mom_by_ticker.index:
            continue
        roc12 = float(mom_by_ticker.loc[ticker, "roc12"])
        if roc12 < roc12_threshold:
            signals[ticker] = "MOMENTUM"

    # PRIORYTET 3 → już nie jest w TOP5
    for ticker in held_tickers:
        if ticker not in top5:
            # tylko jeśli nie ma już innego powodu
            signals.setdefault(ticker, "NOT_IN_TOP5")

    return signals
