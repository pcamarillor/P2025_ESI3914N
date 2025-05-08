import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta

def get_historical_data(ticker: str, start_date: str = "2025-01-01", end_date: str = None):
    """
    Downloads historical close prices for a given ticker using yfinance.

    Args:
        ticker (str): Stock ticker symbol (e.g., "AAPL").
        start_date (str): Start date in "YYYY-MM-DD" format.
        end_date (str): End date in "YYYY-MM-DD" format. Defaults to today.

    Returns:
        pd.DataFrame: DataFrame with "Close" prices.
    """
    if end_date is None:
        end_date = datetime.today().strftime("%Y-%m-%d")

    return yf.download(ticker, start=start_date, end=end_date)[["Close"]]


def simulate_gbm_prices(S0, r, sigma, n_periods, period_seconds):
    """
    Simulates stock prices using the Geometric Brownian Motion (GBM) model.

    Args:
        S0 (float): Initial stock price.
        r (float): Risk-free interest rate (annualized).
        sigma (float): Volatility of the stock (annualized).
        n_periods (int): Number of time steps to simulate.
        period_seconds (int): Time window between each simulated price in seconds.

    Returns:
        List[float]: Simulated stock prices including the initial price.
    """
    dt = period_seconds / (365 * 24 * 60 * 60)
    prices = [S0]

    for _ in range(n_periods):
        z = np.random.normal()
        S_new = prices[-1] * np.exp((r - 0.5 * sigma ** 2) * dt + sigma * np.sqrt(dt) * z)
        prices.append(S_new)
    

    return prices


def simulate_prices(tickers, window_seconds=300, n_periods=50, r=0.01, sigma=0.2):
    """
    Simulates future stock prices using GBM for each ticker.

    Args:
        tickers (List[str]): List of ticker symbols.
        window_seconds (int): Time window in seconds between each simulated price.
        n_periods (int): Number of future prices to simulate.
        r (float): Risk-free interest rate (annualized).
        sigma (float): Volatility (annualized).

    Returns:
        pd.DataFrame: DataFrame with timestamps as index and ticker columns.
    """
    now = datetime.utcnow()
    df_final = pd.DataFrame()

    for ticker in tickers:
        try:
            hist = get_historical_data(ticker)
        except Exception as e:
            print(f"Error downloading {ticker}: {e}")
            continue

        if hist.empty:
            print(f"No data for {ticker}. Skipping.")
            continue

        last_price = hist["Close"].iloc[-1, 0]  
        times = [now + timedelta(seconds=i * window_seconds) for i in range(n_periods + 1)]
        prices = simulate_gbm_prices(S0=last_price, r=r, sigma=sigma, n_periods=n_periods, period_seconds=window_seconds)

        df = pd.DataFrame({ticker: prices}, index=pd.to_datetime(times))
        df_final = pd.concat([df_final, df], axis=1)

    return df_final
