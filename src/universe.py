import pandas as pd


def load_universe():
    """
    Zwraca listę tickerów wykorzystywanych w strategii momentum (Strategy B).
    Możesz dowolnie rozszerzyć wszechświat akcji.
    """

    # US mega-cap momentum universe
    tickers = [
        "AAPL",
        "MSFT",
        "NVDA",
        "GOOGL",
        "AMZN",
        "AVGO",
        "META",
        "LLY",
        "JPM",
        "TSLA"
    ]

    return tickers
