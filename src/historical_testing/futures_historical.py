import requests
import pandas as pd
import time
from datetime import datetime, timedelta

def get_historical_klines(symbol, interval, start_time, end_time):
    url = 'https://fapi.binance.com/fapi/v1/klines'
    params = {
        'symbol': symbol,
        'interval': interval,
        'startTime': start_time,
        'endTime': end_time,
        'limit': 1000
    }
    response = requests.get(url, params=params)
    data = response.json()
    return data

def collect_all_data(symbol, interval, start_str):
    start_time = int(datetime.timestamp(datetime.strptime(start_str, '%Y-%m-%d')) * 1000)
    end_time = int(datetime.timestamp(datetime.utcnow()) * 1000)

    all_data = []
    while start_time < end_time:
        data = get_historical_klines(symbol, interval, start_time, end_time)
        if not data:
            break
        all_data.extend(data)
        start_time = data[-1][0] + 1
        time.sleep(0.5)

    return all_data

def calculate_loss_percentage(buy_price, sell_price):
    return (buy_price - sell_price) / buy_price * 100

def calculate_gain_percentage(buy_price, sell_price):
    return (sell_price - buy_price) / buy_price * 100

def calculate_fee(amount, fee_rate=0.045):
    return amount * fee_rate / 100

symbol = 'BTCUSDT'
interval = '5m'
start_str = '2020-06-25'

data = collect_all_data(symbol, interval, start_str)

columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
df = pd.DataFrame(data, columns=columns)

df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
df['close'] = df['close'].astype(float)
df['low'] = df['low'].astype(float)
df['high'] = df['high'].astype(float)
df['EMA_9'] = df['close'].ewm(span=9, adjust=False).mean()

saldo = 1000

print("Início do período:", datetime.fromtimestamp(df['timestamp'].iloc[0].timestamp()))
print("Final do período: ", datetime.fromtimestamp(df['timestamp'].iloc[-1].timestamp()))
print("Preço atual:", df['close'].iloc[-1])
print("-------------------")
print("Iniciando avaliação de trades...")

comprado = False
results = {}

for i in range(50, len(df)):
    year = int(df['timestamp'].iloc[i - 1].year)
    month = int(df['timestamp'].iloc[i - 1].month)

    if year not in results:
        results[year] = {}
    if month not in results[year]:
        results[year][month] = {
            'open_trades': 0,
            'lucro': 0,
            'successful_trades': 0,
            'failed_trades': 0,
            'perda_percentual_total': 0,
        }

    if df['EMA_9'].iloc[i - 2] > df['EMA_9'].iloc[i - 3] and not comprado:
        if df['high'].iloc[i - 1] > df['high'].iloc[i - 2]:
            results[year][month]['open_trades'] += 1
            buy_price = df['high'].iloc[i - 2]
            stoploss = df['low'].iloc[i - 2]
            stopgain = buy_price * 1.085
            comprado = True
            fee = calculate_fee(saldo)
            saldo -= fee
            continue
    elif comprado:
        if df['low'].iloc[i - 1] <= df['low'].iloc[i - 2]:
            if df['low'].iloc[i - 2] < buy_price:
                loss_percentage = calculate_loss_percentage(buy_price, df['low'].iloc[i - 2])
                results[year][month]['failed_trades'] += 1
                results[year][month]['perda_percentual_total'] += loss_percentage
                saldo -= saldo * loss_percentage / 100
                fee = calculate_fee(saldo)
                saldo -= fee
                comprado = False
                continue
            elif df['low'].iloc[i - 2] >= buy_price:
                profit = calculate_gain_percentage(buy_price, df['low'].iloc[i - 2])
                results[year][month]['lucro'] += profit
                results[year][month]['successful_trades'] += 1
                saldo += saldo * profit / 100
                fee = calculate_fee(saldo)
                saldo -= fee
                comprado = False
                continue
        elif df['high'].iloc[i - 1] >= stopgain:
            profit = calculate_gain_percentage(buy_price, stopgain)
            results[year][month]['lucro'] += profit
            results[year][month]['successful_trades'] += 1
            saldo += saldo * profit / 100
            fee = calculate_fee(saldo)
            saldo -= fee
            comprado = False
            continue

for year in results:
    print(f"Ano: {year}")
    print(f"  Operações realizadas: {sum([results[year][month]['open_trades'] for month in results[year]])}")
    print(f"  Trades de sucesso: {sum([results[year][month]['successful_trades'] for month in results[year]])}")
    print(f"  Lucro obtido: {sum([results[year][month]['lucro'] for month in results[year]])}")
    total_successful_trades = sum([results[year][month]['successful_trades'] for month in results[year]])
    if total_successful_trades != 0:
        avg_gain_per_trade = sum([results[year][month]['lucro'] for month in results[year]]) / total_successful_trades
    else:
        avg_gain_per_trade = 0
    print(f"  Ganho médio por trade: {avg_gain_per_trade}")
    print(f"  Trades em prejuízo: {sum([results[year][month]['failed_trades'] for month in results[year]])}")
    print(f"  Perda total: {sum([results[year][month]['perda_percentual_total'] for month in results[year]])}")

    total_loss = sum([results[year][month]['perda_percentual_total'] for month in results[year]])
    total_failed_trades = sum([results[year][month]['failed_trades'] for month in results[year]])

    if total_failed_trades != 0:
        avg_loss_per_trade = total_loss / total_failed_trades
    else:
        avg_loss_per_trade = 0

    print(f"  Perda média por trade: {avg_loss_per_trade}")
    print(f"  Resultado final: {sum([results[year][month]['lucro'] - results[year][month]['perda_percentual_total'] for month in results[year]])}")
    print("Detalhes:")
    for month in results[year]:
        print(f"  Mês: {month}")
        print(f"    Operações realizadas: {results[year][month]['open_trades']}")
        print(f"    Trades de sucesso: {results[year][month]['successful_trades']}")
        print(f"    Lucro obtido: {results[year][month]['lucro']}")
        if results[year][month]['successful_trades'] != 0:
            avg_gain_per_trade_month = results[year][month]['lucro'] / results[year][month]['successful_trades']
        else:
            avg_gain_per_trade_month = 0
        print(f"    Ganho médio por trade: {avg_gain_per_trade_month}")
        print(f"    Trades em prejuízo: {results[year][month]['failed_trades']}")
        print(f"    Perda total: {results[year][month]['perda_percentual_total']}")

        failed_trades = results[year][month]['failed_trades']
        if failed_trades != 0:
            avg_loss_per_trade = results[year][month]['perda_percentual_total'] / failed_trades
        else:
            avg_loss_per_trade = 0

        print(f"    Perda média por trade: {avg_loss_per_trade}")
        print(f"    Resultado final: {results[year][month]['lucro'] - results[year][month]['perda_percentual_total']}")
        print("-------------------")

print("Total:")
print(f"Operações realizadas: {sum([results[year][month]['open_trades'] for year in results for month in results[year]])}")
print(f"Taxa de acerto: {sum([results[year][month]['successful_trades'] for year in results for month in results[year]]) / sum([results[year][month]['open_trades'] for year in results for month in results[year]]) * 100}%")
print(f"Trades de sucesso: {sum([results[year][month]['successful_trades'] for year in results for month in results[year]])}")
print(f"Lucro obtido: {sum([results[year][month]['lucro'] for year in results for month in results[year]])}")
print(f"Ganho médio por trade: {sum([results[year][month]['lucro'] for year in results for month in results[year]]) / sum([results[year][month]['successful_trades'] for year in results for month in results[year]])}")
print(f"Trades em prejuízo: {sum([results[year][month]['failed_trades'] for year in results for month in results[year]])}")
print(f"Perda total: {sum([results[year][month]['perda_percentual_total'] for year in results for month in results[year]])}")
print(f"Perda média por trade: {sum([results[year][month]['perda_percentual_total'] for year in results for month in results[year]]) / sum([results[year][month]['failed_trades'] for year in results for month in results[year]])}")
print(f"Resultado final: {sum([results[year][month]['lucro'] - results[year][month]['perda_percentual_total'] for year in results for month in results[year]])}")
print(f"Saldo final: {saldo}")
