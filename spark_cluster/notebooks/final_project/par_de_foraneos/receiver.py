import pandas as pd
import ta  # Technical Analysis library: pip install ta
def resample_and_aggregate(df, new_window='15min'):
    """
    Processes simulated prices into OHLC format, computes indicators, and adds lag features.

    Args:
        df (pd.DataFrame): DataFrame with timestamps as index and ticker symbols as columns.
        new_window (str): Pandas-compatible resampling window (e.g., '5min', '15min').

    Returns:
        List[pd.DataFrame]: List of dataframes (one per ticker) with:
            - OHLC prices
            - 4 technical indicators: RSI, Williams %R, Ultimate Oscillator, EMA
            - 5 lagged close prices
    """
    tickers = df.columns
    result = []
    
    for ticker in tickers:
        # Create OHLC DataFrame directly from price data
        ticker_data = df[[ticker]].copy()
        
        # Force float type to prevent dtype=object errors
        ticker_data[ticker] = pd.to_numeric(ticker_data[ticker], errors='coerce')
        
        ticker_data = ticker_data.dropna()
       
        # Ensure timestamp index is datetime
        ticker_data.index = pd.to_datetime(ticker_data.index)
        
        ohlc_df = pd.DataFrame()
        
        # Resample to get OHLC
        ohlc_df['open'] = ticker_data[ticker].resample(new_window).first()
        ohlc_df['high'] = ticker_data[ticker].resample(new_window).max()
        ohlc_df['low'] = ticker_data[ticker].resample(new_window).min()
        ohlc_df['close'] = ticker_data[ticker].resample(new_window).last()
        ohlc_df = ohlc_df.dropna()
        
        if ohlc_df.empty:
            print(f"Warning: No data for {ticker} after resampling.")
            continue

        # Technical indicators
        ohlc_df['williams_r'] = ta.momentum.WilliamsRIndicator(
            high=ohlc_df['high'], low=ohlc_df['low'], close=ohlc_df['close']
        ).williams_r()

        ohlc_df['rsi'] = ta.momentum.RSIIndicator(close=ohlc_df['close']).rsi()
        ohlc_df['ultimate_osc'] = ta.momentum.UltimateOscillator(
            high=ohlc_df['high'], low=ohlc_df['low'], close=ohlc_df['close']
        ).ultimate_oscillator()
        ohlc_df['ema'] = ta.trend.EMAIndicator(close=ohlc_df['close'], window=14).ema_indicator()

        # Lag features
        for lag in range(1, 6):
            ohlc_df[f'close_lag_{lag}'] = ohlc_df['close'].shift(lag)

        ohlc_df = ohlc_df.dropna()
        ohlc_df['ticker'] = ticker
        
        result.append(ohlc_df)

    return result
