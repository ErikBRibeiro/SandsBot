import pandas as pd
from datetime import datetime
import requests

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import utils as utils
import setups.emas as emas
import setups.stopgain as StopGain
import setups.stoploss as StopLoss

def fetch_candles(symbol, interval, start_str, end_str=None):
    url = 'https://api.binance.com/api/v3/klines'
    data = []
    limit = 1000
    start_time = int(pd.to_datetime(start_str).timestamp() * 1000)
    end_time = int(pd.to_datetime(end_str).timestamp() * 1000) if end_str else None

    while True:
        params = {
            'symbol': symbol,
            'interval': interval,
            'startTime': start_time,
            'limit': limit
        }
        if end_time:
            params['endTime'] = end_time

        response = requests.get(url, params=params)
        new_data = response.json()
        if not new_data:
            break
        data.extend(new_data)
        start_time = new_data[-1][0] + 1

    columns = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time',
               'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume',
               'taker_buy_quote_asset_volume', 'ignore']
    df = pd.DataFrame(data, columns=columns)
    df = df[['open', 'high', 'low', 'close', 'open_time', 'close_time']]
    return df

start_date = '2023-01-01'
# end_date = '2024-06-27'
end_date = datetime.now().strftime('%Y-%m-%d')

ativo = 'BTCUSDT'
timeframe = '15m'

data = fetch_candles(ativo, timeframe, start_date, end_date)
data['close'] = data['close'].astype(float)
data['low'] = data['low'].astype(float)
data['high'] = data['high'].astype(float)
data['EMA_9'] = data['close'].ewm(span=9, adjust=False).mean()

saldo = 1000

taxa_por_operacao = 0.0153 # futuros usdc
# taxa_por_operacao = 0.04125 # spot usdc
# taxa_por_operacao = 0.045 # spot e futuros usdt
# taxa_por_operacao = 0 # sem taxa de entrada e de saída

# print(data)

print("Início do período:", datetime.fromtimestamp(data['open_time'].iloc[0] / 1000))
print("Final do período: ", datetime.fromtimestamp(data['open_time'].iloc[-1] / 1000))
print("Preço atual:", data['close'].iloc[-1])
print("-------------------")
print("Iniciando avaliação de trades...")

comprado = False

results = {}

for i in range(50, len(data)):
    year = int(datetime.fromtimestamp(data['close_time'].iloc[i - 1] / 1000).year)
    month = int(datetime.fromtimestamp(data['close_time'].iloc[i - 1] / 1000).month)

    if year not in results:
        results[year] = {}
    if month not in results[year]:
        results[year][month] = {
            'open_trades': 0,
            'lucro': 0,
            'successful_trades': 0,
            'failed_trades': 0,
            'perda_percentual_total': 0,
            'saldo_inicial': saldo,
            'saldo_final': saldo
        }

    if comprado:
        if StopLoss.sell_stoploss(data['low'].iloc[i - 1], data['low'].iloc[i - 2]):
            if StopLoss.sell_stoploss(data['low'].iloc[i - 2], buy_price):
                loss_percentage = utils.calculate_loss_percentage(buy_price, data['low'].iloc[i - 2])
                results[year][month]['failed_trades'] += 1
                results[year][month]['perda_percentual_total'] += loss_percentage + taxa_por_operacao
                saldo -= saldo * ((loss_percentage + taxa_por_operacao) / 100)
                results[year][month]['saldo_final'] = saldo
                comprado = False
                # print(datetime.fromtimestamp(data['open_time'].iloc[i - 1] / 1000), "- Vendemos a", stoploss, "com PREJUÍZO percentual de", loss_percentage)
                continue
            elif StopGain.sell_stopgain(data['low'].iloc[i - 2], buy_price):
                profit = utils.calculate_gain_percentage(buy_price, data['low'].iloc[i - 2])
                results[year][month]['lucro'] += profit - taxa_por_operacao
                results[year][month]['successful_trades'] += 1
                saldo += saldo * ((profit - taxa_por_operacao) / 100)
                results[year][month]['saldo_final'] = saldo
                comprado = False
                # print(datetime.fromtimestamp(data['open_time'].iloc[i - 1] / 1000), "- Vendemos a", stopgain, "com LUCRO percentual de", profit)
                continue
        elif StopGain.sell_stopgain(data['high'].iloc[i - 1], stopgain):
            profit = utils.calculate_gain_percentage(buy_price, stopgain)
            results[year][month]['lucro'] += profit - taxa_por_operacao
            results[year][month]['successful_trades'] += 1
            saldo += saldo * ((profit - taxa_por_operacao) / 100)
            results[year][month]['saldo_final'] = saldo
            comprado = False
            # print(datetime.fromtimestamp(data['open_time'].iloc[i - 1] / 1000), "- Vendemos a", stopgain, "com LUCRO percentual de", profit)
            continue

    if not comprado:
        if emas.buy_ema_breakout(data['EMA_9'].iloc[i - 2], data['EMA_9'].iloc[i - 3], data['high'].iloc[i - 2], data['high'].iloc[i - 1]):
            results[year][month]['open_trades'] += 1
            buy_price = data['high'].iloc[i - 2]
            stoploss = data['low'].iloc[i - 2]
            if taxa_por_operacao != 0:
                saldo -= saldo * taxa_por_operacao / 100
            results[year][month]['saldo_final'] = saldo
            
            # stopgain = buy_price * 1.35 # para 1d
            # stopgain = buy_price * 1.1 # para 4h
            # stopgain = buy_price * 1.10 # para 1h
            # stopgain = buy_price * 1.05 # para 1h no ETH
            # stopgain = buy_price * 1.18 # para 15m
            stopgain = StopGain.set_sell_stopgain_percentage(buy_price, 18)
            # stopgain = buy_price * 1.085 # para 15m no ETH
            # stopgain = buy_price * 1.085 # para 5m (valor atual no bot em operação real para ETH)
            # stopgain = buy_price * 1.05 # para 5m (valor atual no bot em operação real para BTC)
            
            comprado = True
            # print(datetime.fromtimestamp(data['open_time'].iloc[i - 1] / 1000), "- COMPRAMOS a", buy_price, "com stoploss em", stoploss, "e stopgain em", stopgain)
            continue

descricao_setup = "EMA 9/21 rompimento com follow stop, stopgain em 18% e stoploss na min do candle anterior"

for year in results:
    print(f"Ano: {year}")
    print(f"  Operações realizadas: {sum([results[year][month]['open_trades'] for month in results[year]])}")
    print(f"  Trades de sucesso: {sum([results[year][month]['successful_trades'] for month in results[year]])}")
    print(f"  Soma dos ganhos: {sum([results[year][month]['lucro'] for month in results[year]]):.2f}%")
    try:
        print(f"  Ganho médio por trade: {sum([results[year][month]['lucro'] for month in results[year]]) / sum([results[year][month]['successful_trades'] for month in results[year]]):.2f}%")
    except ZeroDivisionError:
        print(f"  Ganho médio por trade: 0")
    print(f"  Trades em prejuízo: {sum([results[year][month]['failed_trades'] for month in results[year]])}")
    print(f"  Soma das perdas: {sum([results[year][month]['perda_percentual_total'] for month in results[year]]):.2f}%")
    
    total_loss = sum([results[year][month]['perda_percentual_total'] for month in results[year]])
    total_failed_trades = sum([results[year][month]['failed_trades'] for month in results[year]])

    if total_failed_trades != 0:
        avg_loss_per_trade = total_loss / total_failed_trades
    else:
        avg_loss_per_trade = 0

    print(f"  Perda média por trade: {avg_loss_per_trade:.2f}%")
    
    if results[year][list(results[year].keys())[0]]['saldo_inicial'] <= results[year][list(results[year].keys())[-1]]['saldo_final']:
        print(f"  Resultado final: {((results[year][list(results[year].keys())[-1]]['saldo_final'] / results[year][list(results[year].keys())[0]]['saldo_inicial']) - 1) * 100:.2f}%")
    else:
        print(f"  Resultado final: {((1 - (results[year][list(results[year].keys())[-1]]['saldo_final'] / results[year][list(results[year].keys())[0]]['saldo_inicial'])) * -1) * 100:.2f}%")
    
    print(f"  Saldo inicial: {results[year][list(results[year].keys())[0]]['saldo_inicial']:.2f}")
    print(f"  Saldo final: {results[year][list(results[year].keys())[-1]]['saldo_final']:.2f}")
    print("Detalhes:")
    for month in results[year]:
        print(f"  Mês: {month}")
        print(f"    Operações realizadas: {results[year][month]['open_trades']}")
        print(f"    Trades de sucesso: {results[year][month]['successful_trades']}")
        print(f"    Soma dos ganhos: {results[year][month]['lucro']:.2f}%")
        try:
            print(f"    Ganho médio por trade: {results[year][month]['lucro'] / results[year][month]['successful_trades']:.2f}%")
        except ZeroDivisionError:
            print(f"    Ganho médio por trade: 0")
        print(f"    Trades em prejuízo: {results[year][month]['failed_trades']}")
        print(f"    Soma das perdas: {results[year][month]['perda_percentual_total']:.2f}%")
        
        failed_trades = results[year][month]['failed_trades']

        if failed_trades != 0:
            avg_loss_per_trade = results[year][month]['perda_percentual_total'] / failed_trades
        else:
            avg_loss_per_trade = 0

        print(f"    Perda média por trade: {avg_loss_per_trade:.2f}%")

        if results[year][month]['saldo_inicial'] <= results[year][month]['saldo_final']:
            print(f"    Resultado final: {(results[year][month]['saldo_final'] / results[year][month]['saldo_inicial'] - 1) * 100:.2f}%")
        else:
            print(f"    Resultado final: {((1 - (results[year][month]['saldo_final'] / results[year][month]['saldo_inicial'])) * -1) * 100:.2f}%")

        print(f"    Saldo inicial: {results[year][month]['saldo_inicial']:.2f}")
        print(f"    Saldo final: {results[year][month]['saldo_final']:.2f}")
        print("-------------------")

print("Total:")
print(f"Operações realizadas: {sum([results[year][month]['open_trades'] for year in results for month in results[year]])}")

try:
    print(f"Taxa de acerto: {sum([results[year][month]['successful_trades'] for year in results for month in results[year]]) / sum([results[year][month]['open_trades'] for year in results for month in results[year]]) * 100:.2f}%")
except ZeroDivisionError:
    print(f"Taxa de acerto: 0")
print(f"Trades de sucesso: {sum([results[year][month]['successful_trades'] for year in results for month in results[year]])}")
print(f"Soma dos ganhos: {sum([results[year][month]['lucro'] for year in results for month in results[year]]):.2f}%")

try:
    print(f"Ganho médio por trade: {sum([results[year][month]['lucro'] for year in results for month in results[year]]) / sum([results[year][month]['successful_trades'] for year in results for month in results[year]]):.2f}%")
except ZeroDivisionError:
    print(f"Ganho médio por trade: 0")

print(f"Trades em prejuízo: {sum([results[year][month]['failed_trades'] for year in results for month in results[year]])}")
print(f"Soma das perdas: {sum([results[year][month]['perda_percentual_total'] for year in results for month in results[year]]):.2f}%")
try:
    print(f"Perda média por trade: {sum([results[year][month]['perda_percentual_total'] for year in results for month in results[year]]) / sum([results[year][month]['failed_trades'] for year in results for month in results[year]]):.2f}%")
except ZeroDivisionError:
    print(f"Perda média por trade: 0")

saldo_inicial = results[list(results.keys())[0]][list(results[list(results.keys())[0]].keys())[0]]['saldo_inicial']
saldo_final = results[list(results.keys())[-1]][list(results[list(results.keys())[-1]].keys())[-1]]['saldo_final']

if saldo_inicial <= saldo_final:
    print(f"Resultado final: {(saldo_final / saldo_inicial - 1) * 100:.2f}%")
else:
    print(f"Resultado final: {((1 - (saldo_final / saldo_inicial)) * -1) * 100:.2f}%")

print(f"Saldo inicial: {results[list(results.keys())[0]][list(results[list(results.keys())[0]].keys())[0]]['saldo_inicial']:.2f}")
print(f"Saldo final: {results[list(results.keys())[-1]][list(results[list(results.keys())[-1]].keys())[-1]]['saldo_final']:.2f}")
print("-------------------")
print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: Teste finalizado: {ativo} - {timeframe}.")

print(f"Setup: {descricao_setup}")