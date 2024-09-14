from binance import Client
import pandas as pd
from datetime import datetime
import time
import numpy as np

script_start = time.time_ns()

api_key = "ok9V7x0ETItBjXJJXp3HZNQbx1rAN26OiGIaPey7DMDm2d2612gU5aVQdT0E82bz"
api_secret = "k15suaXdqzbwfvrYX0qcvNAkXET8EnqjO9JraQhduShjIGQ0YX0kiqXEntTdlRq0"

client = Client(api_key, api_secret)

symbol = "BTCUSDT"
interval = Client.KLINE_INTERVAL_5MINUTE
start_str = "1 Jan, 2021"

klines = client.get_historical_klines(symbol, interval, start_str)

data = pd.DataFrame(klines, columns=['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time',
                                     'quote_asset_volume', 'number_of_trades',
                                     'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'])

data['open_time'] = pd.to_datetime(data['open_time'], unit='ms')
data['close'] = data['close'].astype(float)
data['low'] = data['low'].astype(float)
data['high'] = data['high'].astype(float)

def get_bollinger_bands(data, window, no_of_std):
    rolling_mean = data['close'].rolling(window=window).mean()
    rolling_std = data['close'].rolling(window=window).std()
    data['Bollinger_High'] = rolling_mean + (rolling_std * no_of_std)
    data['Bollinger_Low'] = rolling_mean - (rolling_std * no_of_std)
    data['SMA'] = rolling_mean
    return data

data = get_bollinger_bands(data, window=20, no_of_std=2)

comprado = False

results = {
    'trades': 0,
    'lucro': 0,
    'successful_trades': 0,
    'failed_trades': 0,
    'perda_percentual_total': 0,
}

time_frame = len(data)

for i in range(20, time_frame):
    current_day_of_week = data['open_time'].iloc[i - 1].dayofweek
    if data['close'].iloc[i - 1] < data['Bollinger_Low'].iloc[i - 1] and not comprado and current_day_of_week in [5, 6]:
        results['trades'] += 1
        buy_price = data['close'].iloc[i - 1]
        stoploss = data['low'].iloc[i - 1]
        stopgain = data['SMA'].iloc[i]

        comprado = True
        continue
    elif comprado:
        if data['low'].iloc[i - 1] <= stoploss:
            loss_percentage = (buy_price - stoploss) / buy_price * 100
            results['failed_trades'] += 1
            results['perda_percentual_total'] += loss_percentage
            comprado = False
            continue
        elif data['close'].iloc[i - 1] >= stopgain:
            profit = (stopgain - buy_price) / buy_price * 100
            results['lucro'] += profit
            results['successful_trades'] += 1
            comprado = False
            continue

print("Avaliação das operações:")
print(f"Operações realizadas: {results['trades']}")
print(f"Trades de sucesso: {results['successful_trades']}")
print(f"Lucro obtido: {results['lucro']}")
print(f"Trades em prejuízo: {results['failed_trades']}")
print(f"Percentual de perda total: {results['perda_percentual_total']}")
