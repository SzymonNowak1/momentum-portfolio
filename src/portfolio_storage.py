from __future__ import annotations
from datetime import date
from typing import Optional, Iterable

import sqlite3
from db import get_connection, init_db


def add_contribution(
    dt: date,
    amount_pln: float,
    note: str = "",
    conn: sqlite3.Connection | None = None,
) -> None:
    """
    Zapisuje dopłatę do portfela w PLN.
    """
    close_after = False
    if conn is None:
        conn = get_connection()
        close_after = True

    cur = conn.cursor()
    cur.execute(
        "INSERT INTO contributions (date, amount_pln, note) VALUES (?, ?, ?)",
        (dt.isoformat(), float(amount_pln), note),
    )
    conn.commit()

    if close_after:
        conn.close()


def list_contributions(conn: sqlite3.Connection | None = None) -> list[tuple]:
    """
    Zwraca listę wszystkich dopłat (id, date, amount_pln, note).
    """
    close_after = False
    if conn is None:
        conn = get_connection()
        close_after = True

    cur = conn.cursor()
    cur.execute("SELECT id, date, amount_pln, note FROM contributions ORDER BY date;")
    rows = cur.fetchall()

    if close_after:
        conn.close()

    return rows


def set_equity_snapshot(
    dt: date,
    equity_pln: float,
    conn: sqlite3.Connection | None = None,
) -> None:
    """
    Zapisuje / nadpisuje wartość portfela w danym dniu.
    """
    close_after = False
    if conn is None:
        conn = get_connection()
        close_after = True

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO equity_history (date, equity_pln)
        VALUES (?, ?)
        ON CONFLICT(date) DO UPDATE SET equity_pln=excluded.equity_pln;
        """,
        (dt.isoformat(), float(equity_pln)),
    )
    conn.commit()

    if close_after:
        conn.close()
