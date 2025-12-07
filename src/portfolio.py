import pandas as pd


def detect_currency(ticker: str) -> str:
    """
    Bardzo proste rozpoznawanie waluty po tickerze:
    - '.DE' -> EUR (np. ZPR1.DE)
    - '.PL' -> PLN (GPW)
    - inaczej -> USD
    """
    if ticker.endswith(".DE"):
        return "EUR"
    if ticker.endswith(".PL"):
        return "PLN"
    return "USD"


def build_target_allocation(
    equity_pln: float,
    regime: str,
    top5: pd.DataFrame | None,
    fx_row: pd.Series,
    safe_asset: str = "ZPR1.DE",
) -> pd.DataFrame:
    """
    Buduje docelową alokację portfela:
    - jeśli regime == 'BULL' i mamy top5 -> równy podział między nimi
    - jeśli regime == 'BEAR' -> 100% w safe_asset (np. ZPR1.DE)
    Zwraca DataFrame z kolumnami:
      ['ticker', 'currency', 'weight', 'target_value_pln', 'target_value_ccy']
    """

    records: list[dict] = []

    if regime == "BULL" and top5 is not None and len(top5) > 0:
        n = len(top5)
        weight_per = 1.0 / n

        for _, row in top5.iterrows():
            ticker = row["ticker"]
            ccy = detect_currency(ticker)
            fx = fx_row[ccy]

            target_value_pln = equity_pln * weight_per
            target_value_ccy = target_value_pln / fx

            records.append(
                {
                    "ticker": ticker,
                    "currency": ccy,
                    "weight": weight_per,
                    "target_value_pln": round(target_value_pln, 2),
                    "target_value_ccy": round(target_value_ccy, 4),
                }
            )

    else:
        # BEAR -> cały kapitał w bezpiecznym ETF
        ticker = safe_asset
        ccy = detect_currency(ticker)
        fx = fx_row[ccy]

        target_value_pln = equity_pln
        target_value_ccy = target_value_pln / fx

        records.append(
            {
                "ticker": ticker,
                "currency": ccy,
                "weight": 1.0,
                "target_value_pln": round(target_value_pln, 2),
                "target_value_ccy": round(target_value_ccy, 4),
            }
        )

    return pd.DataFrame(records)
