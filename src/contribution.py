from __future__ import annotations

from datetime import date, timedelta
import pandas_market_calendars as mcal


nyse = mcal.get_calendar("NYSE")


def next_trading_day(dt: date) -> date:
    """Zwraca dt jeśli to dzień handlowy, inaczej najbliższy kolejny."""
    while True:
        schedule = nyse.schedule(start_date=dt, end_date=dt)
        if not schedule.empty:
            return dt
        dt += timedelta(days=1)


def is_contribution_day(today: date) -> bool:
    """
    Dopłata przypada na:
    - 10. dzień miesiąca
    - jeśli to dzień wolny → przesunięcie
    """
    target = date(today.year, today.month, 10)
    target = next_trading_day(target)
    return today == target
