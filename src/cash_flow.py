from __future__ import annotations

from datetime import date
from typing import Literal

import pandas as pd

from db import get_connection


FlowType = Literal["DEPOSIT", "WITHDRAW"]


def add_cash_flow(dt: date, amount_pln: float, flow_type: FlowType, note: str = "") -> None:
    """
    Zapisuje dopłatę (DEPOSIT) lub wypłatę (WITHDRAW) w tabeli cash_flows.
    amount_pln > 0
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO cash_flows (date, amount_pln, type, note)
        VALUES (?, ?, ?, ?)
        """,
        (dt.isoformat(), float(amount_pln), flow_type, note),
    )
    conn.commit()
    conn.close()


def load_cash_flows() -> pd.DataFrame:
    """
    Zwraca wszystkie dopłaty/wypłaty jako DataFrame:
      ['id', 'date', 'amount_pln', 'type', 'note']
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, date, amount_pln, type, note FROM cash_flows ORDER BY date;")
    rows = cur.fetchall()
    conn.close()

    cols = ["id", "date", "amount_pln", "type", "note"]
    if not rows:
        return pd.DataFrame(columns=cols)

    return pd.DataFrame(rows, columns=cols)
