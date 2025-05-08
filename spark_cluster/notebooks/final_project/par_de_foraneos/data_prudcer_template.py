# simulate_and_prepare.py
from producer import simulate_prices
from receiver import resample_and_aggregate

# Simulate future prices for AAPL and TSLA
simulated_df = simulate_prices(['AAPL', 'TSLA'], window_seconds=300, n_periods=100)

# Transform to indicator-rich datasets
processed_datasets = resample_and_aggregate(simulated_df, new_window='15min')

# Example: view AAPL dataset
# Dataset for training the model 
print(processed_datasets[0].head())
