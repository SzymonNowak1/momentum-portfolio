import pandas as pd
from db import get_connection


# ---------------------------------------------------------
# Load current positions
# ---------------------------------------------------------
def load_positions():
    conn = get_connection()
    try:
        df = pd.read_sql("SELECT * FROM portfolio_positions", conn)
    except Exception:
        # jeśli tabela nie istnieje / jest pusta -> zwracamy pusty DF
        df = pd.DataFrame(columns=["ticker", "quantity", "currency", "avg_price_ccy", "avg_price_pln"])
    finally:
        conn.close()
    return df


# ---------------------------------------------------------
# Save monthly contribution
# ---------------------------------------------------------
def record_contribution(timestamp, amount_pln):
    """
    Zapisuje miesięczną wpłatę do tabeli contributions.
    """
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO contributions (timestamp, amount_pln)
        VALUES (?, ?)
        """,
        (timestamp, amount_pln),
    )

    conn.commit()
    conn.close()

    print(f"[CONTRIBUTION] Saved contribution: {amount_pln} PLN on {timestamp}")


# ---------------------------------------------------------
# Compute total equity in PLN
# ---------------------------------------------------------
def estimate_total_equity(price_data, fx_row):
    """
    Zwraca całkowitą wartość portfela w PLN.

    price_data  : dict[ticker] -> DataFrame z kolumną 'Close'
    fx_row      : Series z kursami FX: fx_row['USD'], fx_row['EUR']
    """

    positions = load_positions()
    if positions.empty:
        return 0.0

    total_pln = 0.0

    for _, pos in positions.iterrows():
        ticker = pos["ticker"]
        qty = pos["quantity"]
        currency = pos["currency"]

        df = price_data.get(ticker)
        if df is None or df.empty:
            continue

        price_ccy = df["Close"].iloc[-1]

        # FX conversion
        if currency == "USD":
            fx = fx_row["USD"]
        elif currency == "EUR":
            fx = fx_row["EUR"]
        else:
            fx = 1.0

        total_pln += qty * price_ccy * fx

    return round(total_pln, 2)
