# Ribeiro's Trading Bot translated from Pine Script to Python
# Libraries required: pandas, numpy, pandas_ta, matplotlib

import pandas as pd
import numpy as np
import pandas_ta as ta
import matplotlib.pyplot as plt

# Load your data into a pandas DataFrame
# Replace 'testes_iniciais/BYBIT_BTCUSDT.P_1h.csv' with the path to your CSV file containing OHLCV data
# The CSV should have columns: 'timestamp', 'open', 'high', 'low', 'close', 'volume'
data = pd.read_csv(
    'testes_iniciais/BYBIT_BTCUSDT.P_1h.csv',
    parse_dates=['timestamp'],  # Adjust this if your date column has a different name
    index_col='timestamp'
)

# If necessary, rename columns to match expected names
data.rename(columns={
    'open': 'Open',
    'high': 'High',
    'low': 'Low',
    'close': 'Close',
    'volume': 'Volume'
}, inplace=True)

# Parameters (Adjust these as needed)
emaShortLength = 11
emaLongLength = 55
rsiLength = 22
macdShort = 15
macdLong = 34
macdSignal = 11
adxThreshold = 12
bbLength = 14
bbMultiplier = 1.7
lateralThreshold = 0.005

# Stop Loss and Take Profit parameters for lateral market
stopLossLateralLong = 0.973  # As a multiplier
takeProfitLateralLong = 1.11
stopLossLateralShort = 1.09
takeProfitLateralShort = 0.973

# Stop Loss and Take Profit parameters for trending market
stopLossTrendingLong = 0.92
takeProfitTrendingLong = 1.32
stopLossTrendingShort = 1.12
takeProfitTrendingShort = 0.77

# Date range parameters
startDate = pd.to_datetime("2020-01-01")
endDate = pd.to_datetime("2024-10-10")

# Filter data within the date range
data = data[(data.index >= startDate) & (data.index <= endDate)]

# Calculate Indicators
# Exponential Moving Averages
data['emaShort'] = ta.ema(data['Close'], length=emaShortLength)
data['emaLong'] = ta.ema(data['Close'], length=emaLongLength)

# Relative Strength Index
data['rsi'] = ta.rsi(data['Close'], length=rsiLength)

# Moving Average Convergence Divergence
macd = ta.macd(data['Close'], fast=macdShort, slow=macdLong, signal=macdSignal)
data = pd.concat([data, macd], axis=1)

data['macdLine'] = data[f'MACD_{macdShort}_{macdLong}_{macdSignal}']
data['signalLine'] = data[f'MACDs_{macdShort}_{macdLong}_{macdSignal}']
data['macdHist'] = data[f'MACDh_{macdShort}_{macdLong}_{macdSignal}']

# Average Directional Movement Index
dmi = ta.adx(data['High'], data['Low'], data['Close'], length=16)
data = pd.concat([data, dmi], axis=1)

data['diplus'] = data['DMP_16']  # +DI
data['diminus'] = data['DMN_16']  # -DI
data['adxValue'] = data['ADX_16']

# Bollinger Bands
bb = ta.bbands(data['Close'], length=bbLength, std=bbMultiplier)
data = pd.concat([data, bb], axis=1)

data['upperBand'] = data[f'BBU_{bbLength}_{bbMultiplier}']
data['lowerBand'] = data[f'BBL_{bbLength}_{bbMultiplier}']
data['basis'] = data[f'BBM_{bbLength}_{bbMultiplier}']

# Bollinger Bandwidth
data['bandWidth'] = (data['upperBand'] - data['lowerBand']) / data['basis']

# Market Conditions
data['isLateral'] = data['bandWidth'] < lateralThreshold
data['trendingMarket'] = data['adxValue'] > adxThreshold

# Entry Conditions for Trending Market
data['longCondition'] = (
    (data['emaShort'] > data['emaLong']) & (data['emaShort'].shift(1) <= data['emaLong'].shift(1)) &
    (data['rsi'] < 60) &
    (data['macdHist'] > 0.5) &
    (data['trendingMarket']) &
    (~data['isLateral'])
)

data['shortCondition'] = (
    (data['emaShort'] < data['emaLong']) & (data['emaShort'].shift(1) >= data['emaLong'].shift(1)) &
    (data['rsi'] > 40) &
    (data['macdHist'] < -0.5) &
    (data['trendingMarket']) &
    (~data['isLateral'])
)

# Entry Conditions for Lateral Market (Mean Reversion)
data['longConditionLateral'] = (
    (data['Close'] > data['lowerBand']) & (data['Close'].shift(1) <= data['lowerBand'].shift(1)) &
    (data['isLateral'])
)

data['shortConditionLateral'] = (
    (data['Close'] < data['upperBand']) & (data['Close'].shift(1) >= data['upperBand'].shift(1)) &
    (data['isLateral'])
)

# Initialize variables for trade simulation
position = 0  # 0 = Flat, 1 = Long, -1 = Short
entryPrice = 0.0
stopLoss = 0.0
takeProfit = 0.0
trade_log = []

# Simulate Trades
for index, row in data.iterrows():
    if position == 0:
        # Check for entry signals
        if row['longCondition']:
            position = 1
            entryPrice = row['Close']
            stopLoss = entryPrice * stopLossTrendingLong
            takeProfit = entryPrice * takeProfitTrendingLong
            positionType = 'Long Trending'
            trade = {'EntryDate': index, 'Position': 'Long', 'EntryPrice': entryPrice,
                     'StopLoss': stopLoss, 'TakeProfit': takeProfit, 'Type': positionType}
        elif row['shortCondition']:
            position = -1
            entryPrice = row['Close']
            stopLoss = entryPrice * stopLossTrendingShort
            takeProfit = entryPrice * takeProfitTrendingShort
            positionType = 'Short Trending'
            trade = {'EntryDate': index, 'Position': 'Short', 'EntryPrice': entryPrice,
                     'StopLoss': stopLoss, 'TakeProfit': takeProfit, 'Type': positionType}
        elif row['longConditionLateral']:
            position = 1
            entryPrice = row['Close']
            stopLoss = entryPrice * stopLossLateralLong
            takeProfit = entryPrice * takeProfitLateralLong
            positionType = 'Long Lateral'
            trade = {'EntryDate': index, 'Position': 'Long', 'EntryPrice': entryPrice,
                     'StopLoss': stopLoss, 'TakeProfit': takeProfit, 'Type': positionType}
        elif row['shortConditionLateral']:
            position = -1
            entryPrice = row['Close']
            stopLoss = entryPrice * stopLossLateralShort
            takeProfit = entryPrice * takeProfitLateralShort
            positionType = 'Short Lateral'
            trade = {'EntryDate': index, 'Position': 'Short', 'EntryPrice': entryPrice,
                     'StopLoss': stopLoss, 'TakeProfit': takeProfit, 'Type': positionType}
    else:
        # Check for exit signals
        if position == 1:
            # Long position
            if row['Low'] <= stopLoss and row['High'] >= takeProfit:
                # Both levels hit; prioritize Take Profit
                exitPrice = takeProfit
                position = 0
                trade['ExitDate'] = index
                trade['ExitPrice'] = exitPrice
                trade['Profit'] = exitPrice - entryPrice
                trade['ExitReason'] = 'Take Profit'
                trade_log.append(trade)
            elif row['Low'] <= stopLoss:
                # Stop Loss hit
                exitPrice = stopLoss
                position = 0
                trade['ExitDate'] = index
                trade['ExitPrice'] = exitPrice
                trade['Profit'] = exitPrice - entryPrice
                trade['ExitReason'] = 'Stop Loss'
                trade_log.append(trade)
            elif row['High'] >= takeProfit:
                # Take Profit hit
                exitPrice = takeProfit
                position = 0
                trade['ExitDate'] = index
                trade['ExitPrice'] = exitPrice
                trade['Profit'] = exitPrice - entryPrice
                trade['ExitReason'] = 'Take Profit'
                trade_log.append(trade)
        elif position == -1:
            # Short position
            if row['High'] >= stopLoss and row['Low'] <= takeProfit:
                # Both levels hit; prioritize Take Profit
                exitPrice = takeProfit
                position = 0
                trade['ExitDate'] = index
                trade['ExitPrice'] = exitPrice
                trade['Profit'] = entryPrice - exitPrice
                trade['ExitReason'] = 'Take Profit'
                trade_log.append(trade)
            elif row['High'] >= stopLoss:
                # Stop Loss hit
                exitPrice = stopLoss
                position = 0
                trade['ExitDate'] = index
                trade['ExitPrice'] = exitPrice
                trade['Profit'] = entryPrice - exitPrice
                trade['ExitReason'] = 'Stop Loss'
                trade_log.append(trade)
            elif row['Low'] <= takeProfit:
                # Take Profit hit
                exitPrice = takeProfit
                position = 0
                trade['ExitDate'] = index
                trade['ExitPrice'] = exitPrice
                trade['Profit'] = entryPrice - exitPrice
                trade['ExitReason'] = 'Take Profit'
                trade_log.append(trade)

# Convert trade log to DataFrame
trades = pd.DataFrame(trade_log)

# Calculate Performance Metrics
total_profit = trades['Profit'].sum()
number_of_trades = len(trades)
winning_trades = trades[trades['Profit'] > 0]
win_rate = len(winning_trades) / number_of_trades if number_of_trades > 0 else 0

print(f"Total Profit: {total_profit}")
print(f"Number of Trades: {number_of_trades}")
print(f"Win Rate: {win_rate * 100:.2f}%")

