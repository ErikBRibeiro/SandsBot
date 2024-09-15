import pandas as pd
import numpy as np
import ta  # Ensure this library is installed
import matplotlib.pyplot as plt
from datetime import datetime

# Load your dataset into a Pandas DataFrame
# Replace 'data.csv' with the path to your dataset
df = pd.read_csv('testes_iniciais/BYBIT_BTCUSDT.P_1h.csv', parse_dates=['time'])

# Parameters (These can be adjusted as needed)
# Parâmetros de Entrada para Ajuste
emaShortLength = 11  # Período da EMA Curta
emaLongLength = 55   # Período da EMA Longa
rsiLength = 22       # Período do RSI
macdShort = 15       # Período Curto MACD
macdLong = 34        # Período Longo MACD
macdSignal = 11      # Período de Sinal MACD
adxThreshold = 12.0  # Nível de ADX
bbLength = 14        # Período do Bollinger Bands
bbMultiplier = 1.7   # Multiplicador do Bollinger Bands
lateralThreshold = 0.005  # Limite de Lateralização

# Parâmetros para Stop Loss e Take Profit em mercado lateral
stopLossLateralLong = 0.973
takeProfitLateralLong = 1.11
stopLossLateralShort = 1.09
takeProfitLateralShort = 0.973

# Parâmetros para Stop Loss e Take Profit em mercado de tendência
stopLossTrendingLong = 0.92
takeProfitTrendingLong = 1.32
stopLossTrendingShort = 1.12
takeProfitTrendingShort = 0.77

# Entrada de Parâmetros de Data
startDate = pd.to_datetime("2020-01-01")
endDate = pd.to_datetime("2024-10-10 23:59")

# Ensure 'time' is datetime and set it as the index
df['time'] = pd.to_datetime(df['time'])
df = df.set_index('time')

# Calculating Indicators
# EMAs
df['emaShort'] = df['close'].ewm(span=emaShortLength, adjust=False).mean()
df['emaLong'] = df['close'].ewm(span=emaLongLength, adjust=False).mean()

# RSI
df['rsi'] = ta.momentum.RSIIndicator(close=df['close'], window=rsiLength).rsi()

# MACD
macd_indicator = ta.trend.MACD(
    close=df['close'],
    window_slow=macdLong,
    window_fast=macdShort,
    window_sign=macdSignal
)
df['macdLine'] = macd_indicator.macd()
df['signalLine'] = macd_indicator.macd_signal()
df['macdHist'] = macd_indicator.macd_diff()

# ADX
adx_length = 16  # DI Length from Pine Script
adx_smoothing = 13  # ADX Smoothing from Pine Script
adx_indicator = ta.trend.ADXIndicator(
    high=df['high'],
    low=df['low'],
    close=df['close'],
    window=adx_length,
    fillna=False
)
df['adx'] = adx_indicator.adx()

# Bollinger Bands
bb_indicator = ta.volatility.BollingerBands(
    close=df['close'],
    window=bbLength,
    window_dev=bbMultiplier
)
df['basis'] = bb_indicator.bollinger_mavg()
df['upperBand'] = bb_indicator.bollinger_hband()
df['lowerBand'] = bb_indicator.bollinger_lband()
df['bandWidth'] = (df['upperBand'] - df['lowerBand']) / df['basis']

# Detect Lateral Market
df['isLateral'] = df['bandWidth'] < lateralThreshold
df['trendingMarket'] = df['adx'] > adxThreshold

# Helper functions for crossover and crossunder
def crossover(series1, series2):
    return (series1 > series2) & (series1.shift(1) <= series2.shift(1))

def crossunder(series1, series2):
    return (series1 < series2) & (series1.shift(1) >= series2.shift(1))

# In Date Range
df['inDateRange'] = (df.index >= startDate) & (df.index <= endDate)

# Long and Short Conditions for Trending Market
df['longCondition'] = (
    ~df['isLateral'] &
    crossover(df['emaShort'], df['emaLong']) &
    (df['rsi'] < 60) &
    df['inDateRange'] &
    (df['macdHist'] > 0.5) &
    df['trendingMarket']
)

df['shortCondition'] = (
    ~df['isLateral'] &
    crossunder(df['emaShort'], df['emaLong']) &
    (df['rsi'] > 40) &
    df['inDateRange'] &
    (df['macdHist'] < -0.5) &
    df['trendingMarket']
)

# Long and Short Conditions for Lateral Market
df['longConditionLateral'] = (
    df['isLateral'] &
    crossover(df['close'], df['lowerBand']) &
    df['inDateRange']
)

df['shortConditionLateral'] = (
    df['isLateral'] &
    crossunder(df['close'], df['upperBand']) &
    df['inDateRange']
)

# Initialize position tracking columns
df['position'] = np.nan
df['entry_price'] = np.nan
df['exit_price'] = np.nan
df['trade_returns'] = np.nan
df['cumulative_returns'] = np.nan
df['stop_loss'] = np.nan
df['take_profit'] = np.nan

# Collect trade information
trades = []

# Initialize variables
position = None
entry_price = 0.0
stop_loss_price = 0.0
take_profit_price = 0.0
cumulative_returns = 1.0  # Starting with 1.0 to multiply returns

for index, row in df.iterrows():
    if position is None:
        # Check for entry conditions
        if row['longCondition'] or row['longConditionLateral']:
            position = 'long'
            entry_price = row['close']
            entry_index = index  # Store the time of entry
            if row['longCondition']:
                # Trending market parameters
                stop_loss_price = entry_price * stopLossTrendingLong
                take_profit_price = entry_price * takeProfitTrendingLong
            else:
                # Lateral market parameters
                stop_loss_price = entry_price * stopLossLateralLong
                take_profit_price = entry_price * takeProfitLateralLong
            df.at[index, 'position'] = position
            df.at[index, 'entry_price'] = entry_price
            df.at[index, 'stop_loss'] = stop_loss_price
            df.at[index, 'take_profit'] = take_profit_price
        elif row['shortCondition'] or row['shortConditionLateral']:
            position = 'short'
            entry_price = row['close']
            entry_index = index  # Store the time of entry
            if row['shortCondition']:
                # Trending market parameters
                stop_loss_price = entry_price * stopLossTrendingShort
                take_profit_price = entry_price * takeProfitTrendingShort
            else:
                # Lateral market parameters
                stop_loss_price = entry_price * stopLossLateralShort
                take_profit_price = entry_price * takeProfitLateralShort
            df.at[index, 'position'] = position
            df.at[index, 'entry_price'] = entry_price
            df.at[index, 'stop_loss'] = stop_loss_price
            df.at[index, 'take_profit'] = take_profit_price
    else:
        # Check for exit conditions
        exit_trade = False
        if position == 'long':
            if row['high'] >= take_profit_price:
                # Take Profit Hit
                exit_price = take_profit_price
                exit_trade = True
            elif row['low'] <= stop_loss_price:
                # Stop Loss Hit
                exit_price = stop_loss_price
                exit_trade = True
        elif position == 'short':
            if row['low'] <= take_profit_price:
                # Take Profit Hit
                exit_price = take_profit_price
                exit_trade = True
            elif row['high'] >= stop_loss_price:
                # Stop Loss Hit
                exit_price = stop_loss_price
                exit_trade = True

        if exit_trade:
            if position == 'long':
                trade_return = (exit_price - entry_price) / entry_price
            else:  # short position
                trade_return = (entry_price - exit_price) / entry_price

            cumulative_returns *= (1 + trade_return)
            df.at[index, 'position'] = None
            df.at[index, 'exit_price'] = exit_price
            df.at[index, 'trade_returns'] = trade_return
            df.at[index, 'cumulative_returns'] = cumulative_returns

            # Record the trade
            trades.append({
                'Entry Time': entry_index,
                'Exit Time': index,
                'Position': position,
                'Entry Price': entry_price,
                'Exit Price': exit_price,
                'Return (%)': trade_return * 100
            })

            # Reset position
            position = None
        else:
            # Holding Position
            df.at[index, 'position'] = position
            df.at[index, 'entry_price'] = entry_price
            df.at[index, 'stop_loss'] = stop_loss_price
            df.at[index, 'take_profit'] = take_profit_price

# Clean up NaN values in cumulative_returns
df['cumulative_returns'] = df['cumulative_returns'].ffill().fillna(1.0)

# Calculate overall strategy performance
total_return = df['cumulative_returns'].iloc[-1]
print(f"Total Return: {(total_return - 1) * 100:.2f}%")

# Create a DataFrame from the trades
trades_df = pd.DataFrame(trades)

# Print trade statistics
print("Number of Trades:", len(trades_df))
print("Average Return per Trade: {:.2f}%".format(trades_df['Return (%)'].mean()))
print("Total Return: {:.2f}%".format((cumulative_returns - 1) * 100))

# Display the trades
print(trades_df)
