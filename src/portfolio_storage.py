from __future__ import annotations

from datetime import datetime
import pandas as pd

from db import get_connection


def load_portfolio_positions() -> pd.DataFrame:
    """
    Wczytuje aktualne pozycje z bazy.
    Zwraca DataFrame z kolumnami:
      ['ticker', 'currency', 'units', 'avg_price_ccy', 'last_update']
    Jeżeli brak pozycji -> pusty DataFrame o tych kolumnach.
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT ticker, currency, units, avg_price_ccy, last_update FROM portfolio_positions"
    )
    rows = cur.fetchall()
    conn.close()

    cols = ["ticker", "currency", "units", "avg_price_ccy", "last_update"]
    if not rows:
        return pd.DataFrame(columns=cols)

    df = pd.DataFrame(rows, columns=cols)
    return df


def save_portfolio_positions(df: pd.DataFrame) -> None:
    """
    Zapisuje cały DataFrame pozycji do bazy.
    Obecny stan tabeli 'portfolio_positions' jest nadpisywany.
    """
    required_cols = {"ticker", "currency", "units", "avg_price_ccy", "last_update"}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"[portfolio_storage] Brakuje kolumn w df. Wymagane: {required_cols}")

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM portfolio_positions")

    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO portfolio_positions (ticker, currency, units, avg_price_ccy, last_update)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                row["ticker"],
                row["currency"],
                float(row["units"]),
                float(row["avg_price_ccy"]),
                str(row["last_update"]),
            ),
        )

    conn.commit()
    conn.close()


def upsert_single_position(
    ticker: str,
    currency: str,
    delta_units: float,
    trade_price_ccy: float,
) -> None:
    """
    Będzie używana przy BUY/SELL.

    - jeśli pozycja nie istnieje -> tworzy z given delta_units jako units
    - jeśli istnieje i delta_units > 0 (BUY) -> aktualizuje avg_price_ccy
    - jeśli istnieje i delta_units < 0 (SELL) -> zmniejsza units
      (jeśli units schodzą ≤ 0 -> usuwa pozycję)
    """
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        "SELECT units, avg_price_ccy FROM portfolio_positions WHERE ticker=?",
        (ticker,),
    )
    row = cur.fetchone()

    now_str = datetime.utcnow().isoformat(timespec="seconds")

    if row is None:
        # nowa pozycja – zakładamy BUY
        cur.execute(
            """
            INSERT INTO portfolio_positions (ticker, currency, units, avg_price_ccy, last_update)
            VALUES (?, ?, ?, ?, ?)
            """,
            (ticker, currency, delta_units, trade_price_ccy, now_str),
        )
    else:
        old_units, old_avg = row
        new_units = old_units + delta_units

        if new_units <= 0:
            cur.execute("DELETE FROM portfolio_positions WHERE ticker=?", (ticker,))
        else:
            if delta_units > 0:
                new_avg = (old_units * old_avg + delta_units * trade_price_ccy) / new_units
            else:
                new_avg = old_avg

            cur.execute(
                """
                UPDATE portfolio_positions
                SET units=?, avg_price_ccy=?, last_update=?
                WHERE ticker=?
                """,
                (new_units, new_avg, now_str, ticker),
            )

    conn.commit()
    conn.close()
