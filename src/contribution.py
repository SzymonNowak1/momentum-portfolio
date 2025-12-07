from datetime import datetime, timedelta
import pandas as pd

# CONTRIBUTION AMOUNT FOR BACKTEST (later manual)
MONTHLY_CONTRIBUTION_PLN = 2000


def is_weekend(date):
    return date.weekday() >= 5  # 5 = Saturday, 6 = Sunday


def next_business_day(date):
    """Return the next weekday (Mon–Fri)."""
    while is_weekend(date):
        date += timedelta(days=1)
    return date


def check_contribution_day(today: datetime):
    """
    Returns how much PLN to contribute today.
    Logic:
    - Contribution day = 10th of month
    - If 10th is weekend → next business day
    - If today is that business day → contribute 2000 PLN
    """

    # Expected contribution day this month
    target = today.replace(day=10)

    # If target day already passed this month, no contribution
    if today.date() < target.date():
        return 0

    # Move 10th to next business day if needed
    target = next_business_day(target)

    # Only contribute if TODAY == calculated contribution day
    if today.date() == target.date():
        return MONTHLY_CONTRIBUTION_PLN

    return 0
