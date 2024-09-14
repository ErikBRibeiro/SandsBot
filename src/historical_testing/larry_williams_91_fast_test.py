from binance import Client
import pandas as pd
from datetime import datetime
import time
import os

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import setups.emas as LarryWilliamsHeterodoxo
import setups.stopgain as StopGain
import setups.stoploss as StopLoss

script_start = time.time_ns()

#TODO: trocar por variáveis de ambiente
api_key = os.getenv('BINANCE_API_KEY')
api_secret = os.getenv('BINANCE_API_SECRET')

client = Client(api_key, api_secret)

symbol = "BTCUSDT"
# interval = "1h"
interval = "15m"
# interval = "5m"

time_frame = 150
candles_skip = time_frame - 1
# candles_skip = 50 para precisão da EMA_9

klines = client.get_klines(symbol=symbol, interval=interval, limit=time_frame)
data = pd.DataFrame(klines, columns=['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'])
data['close'] = data['close'].astype(float)
data['low'] = data['low'].astype(float)
data['high'] = data['high'].astype(float)
data['EMA_9'] = data['close'].ewm(span=9, adjust=False).mean()
data['EMA_21'] = data['close'].ewm(span=21, adjust=False).mean()
# data['EMA_90'] = data['close'].ewm(span=90, adjust=False).mean()
# data['EMA_200'] = data['close'].ewm(span=200, adjust=False).mean()

saldo = 1000

taxa_por_operacao = 0.0153 # futuros usdc

print("Início do período:", datetime.fromtimestamp(data['open_time'].iloc[0] / 1000))
print("Final do período - abertura última vela: ", datetime.fromtimestamp(data['open_time'].iloc[-1] / 1000))
print("Preço atual:", data['close'].iloc[-1])
# print("-------------------")
# print("Iniciando avaliação de trades...")

comprado = False

results = {}

print(f"Ema 9: {data['EMA_9'].iloc[-2]}")
print(f"Ema 21: {data['EMA_21'].iloc[-2]}")

# for i in range(candles_skip, time_frame):
#     year = int(datetime.fromtimestamp(data['close_time'].iloc[i - 1] / 1000).year)
#     month = int(datetime.fromtimestamp(data['close_time'].iloc[i - 1] / 1000).month)

#     if year not in results:
#         results[year] = {}
#     if month not in results[year]:
#         results[year][month] = {
#             'open_trades': 0,
#             'lucro': 0,
#             'successful_trades': 0,
#             'failed_trades': 0,
#             'perda_percentual_total': 0,
#         }
#     print(f"mínima das últimas velas: {min(data['low'].tail(3).tolist())}")

#     if not comprado and is_weekday(data):
#         # if data['EMA_90'].iloc[i - 2] < data['close'].iloc[i - 2] and data['EMA_21'].iloc[i - 2] < data['close'].iloc[i - 2]:
#         if LarryWilliamsHeterodoxo.buy_ema_breakout(data['EMA_9'].iloc[i - 2], data['EMA_9'].iloc[i - 3], data['high'].iloc[i - 2], data['high'].iloc[i - 1]):
#             results[year][month]['open_trades'] += 1
#             buy_price = data['high'].iloc[i - 2]
#             stoploss = min(data['low'].iloc[i - 3], data['low'].iloc[i - 2], data['low'].iloc[i - 1])
#             if taxa_por_operacao != 0:
#                 saldo -= saldo * taxa_por_operacao / 100
#             results[year][month]['saldo_final'] = saldo
#             stopgain = StopGain.set_venda_percentage(buy_price, 10)
#             comprado = True
#             # print(datetime.fromtimestamp(data['open_time'].iloc[i - 1] / 1000), "- COMPRAMOS a", buy_price, "com stoploss em", stoploss, "e stopgain em", stopgain)
#             continue

#     if data['EMA_9'].iloc[i - 2] > data['EMA_9'].iloc[i - 3] and not comprado: # Encontrou a vela referência -> para gatilho e stop do trade::
#     # if data['EMA_9'].iloc[i - 2] > data['EMA_9'].iloc[i - 3] and data['EMA_9'].iloc[i - 3] < data['EMA_9'].iloc[i - 4] and not comprado: # Encontrou a vela referência -> para gatilho e stop do trade
#         if (data['high'].iloc[i - 1] > data['high'].iloc[i - 2]): # Superou a máxima da vela referência -> ativou o gatilho
#             results[year][month]['open_trades'] += 1
#             buy_price = data['high'].iloc[i - 2]
#             stoploss = data['low'].iloc[i - 2]
            
#             # stopgain = buy_price * 1.35 # para 1d
#             # stopgain = buy_price * 1.25 # para 4h
#             # stopgain = buy_price * 1.02 # para 1h
#             # stopgain = buy_price * 1.05 # para 1h no ETH
#             # stopgain = buy_price * 1.015 # para 15m
#             stopgain = buy_price * 1.05 # para 5m (valor atual no bot em operação real)
            
#             comprado = True
#             print(datetime.fromtimestamp(data['open_time'].iloc[i - 1] / 1000), "- COMPRAMOS a", buy_price, "com stoploss em", stoploss, "e stopgain em", stopgain)
#             continue
#     elif comprado:
#         if data['low'].iloc[i - 1] <= data['low'].iloc[i - 2]:
#             # loss_percentage = (buy_price - data['low'].iloc[i - 2]) / buy_price * 100
#             if data['low'].iloc[i - 2] < buy_price:
#                 loss_percentage = calculate_loss_percentage(buy_price, data['low'].iloc[i - 2])
#                 results[year][month]['failed_trades'] += 1
#                 results[year][month]['perda_percentual_total'] += loss_percentage
#                 saldo -= saldo * loss_percentage / 100
#                 comprado = False
#                 print(datetime.fromtimestamp(data['open_time'].iloc[i - 1] / 1000), "- Vendemos a", data['low'].iloc[i - 2], "com PREJUÍZO percentual de", loss_percentage)
#                 continue
#             elif data['low'].iloc[i - 2] >= buy_price:
#                 profit = calculate_gain_percentage(buy_price, data['low'].iloc[i - 2])
#                 results[year][month]['lucro'] += profit
#                 results[year][month]['successful_trades'] += 1
#                 saldo += saldo * profit / 100
#                 comprado = False
#                 print(datetime.fromtimestamp(data['open_time'].iloc[i - 1] / 1000), "- Vendemos a", data['low'].iloc[i - 2], "com LUCRO percentual de", profit)
#                 continue
#         elif data['high'].iloc[i - 1] >= stopgain:
#             # profit = (data['close'].iloc[i - 1] - buy_price) / buy_price * 100
#             profit = calculate_gain_percentage(buy_price, stopgain)
#             results[year][month]['lucro'] += profit
#             results[year][month]['successful_trades'] += 1
#             saldo += saldo * profit / 100
#             comprado = False
#             print(datetime.fromtimestamp(data['open_time'].iloc[i - 1] / 1000), "- Vendemos a", stopgain, "com LUCRO percentual de", profit)
#             continue
#         # else:
#         #     print(datetime.fromtimestamp(data['open_time'].iloc[i - 1] / 1000), "Ainda estamos comprados, esperando a vela fechar para avaliar a próxima ação")
#         #     continue
#         # elif data['EMA_9'].iloc[i - 2] < data['EMA_9'].iloc[i - 3] and data['EMA_9'].iloc[i - 3] > data['EMA_9'].iloc[i - 4]:
#         #     if (data['low'].iloc[i - 1] > data['low'].iloc[i - 2]):
#         #         profit = calculate_gain_percentage(buy_price, data['low'].iloc[i - 2])
#         #         results[year][month]['lucro'] += profit
#         #         results[year][month]['successful_trades'] += 1
#         #         comprado = False
#         #         # print(datetime.fromtimestamp(data['open_time'].iloc[i - 1] / 1000), "- Vendemos a", stopgain, "com LUCRO percentual de", profit)
#         #         continue

# # for year in results:
# #     print(f"Ano: {year}")
# #     print(f"  Operações realizadas: {sum([results[year][month]['open_trades'] for month in results[year]])}")
# #     print(f"  Trades de sucesso: {sum([results[year][month]['successful_trades'] for month in results[year]])}")
# #     print(f"  Lucro obtido: {sum([results[year][month]['lucro'] for month in results[year]])}")
# #     print(f"  Trades em prejuízo: {sum([results[year][month]['failed_trades'] for month in results[year]])}")
# #     print(f"  Perda total: {sum([results[year][month]['perda_percentual_total'] for month in results[year]])}")
# #     print(f"  Perda média por trade: {sum([results[year][month]['perda_percentual_total'] for month in results[year]]) / sum([results[year][month]['failed_trades'] for month in results[year]])}")
# #     print(f"  Resultado final: {sum([results[year][month]['lucro'] - results[year][month]['perda_percentual_total'] for month in results[year]])}")
# #     print("Detalhes:")
# #     for month in results[year]:
# #         print(f"  Mês: {month}")
# #         print(f"    Operações realizadas: {results[year][month]['open_trades']}")
# #         print(f"    Trades de sucesso: {results[year][month]['successful_trades']}")
# #         print(f"    Lucro obtido: {results[year][month]['lucro']}")
# #         print(f"    Trades em prejuízo: {results[year][month]['failed_trades']}")
# #         print(f"    Perda total: {results[year][month]['perda_percentual_total']}")
# #         print(f"    Perda média por trade: {results[year][month]['perda_percentual_total'] / results[year][month]['failed_trades']}")
# #         print(f"    Resultado final: {results[year][month]['lucro'] - results[year][month]['perda_percentual_total']}")
# #         print("-------------------")

# # print("Total:")
# # print(f"Operações realizadas: {sum([results[year][month]['open_trades'] for year in results for month in results[year]])}")
# # print(f"Trades de sucesso: {sum([results[year][month]['successful_trades'] for year in results for month in results[year]])}")
# # print(f"Lucro obtido: {sum([results[year][month]['lucro'] for year in results for month in results[year]])}")
# # print(f"Trades em prejuízo: {sum([results[year][month]['failed_trades'] for year in results for month in results[year]])}")
# # print(f"Perda total: {sum([results[year][month]['perda_percentual_total'] for year in results for month in results[year]])}")
# # print(f"Perda média por trade: {sum([results[year][month]['perda_percentual_total'] for year in results for month in results[year]]) / sum([results[year][month]['failed_trades'] for year in results for month in results[year]])}")
# # print(f"Resultado final: {sum([results[year][month]['lucro'] - results[year][month]['perda_percentual_total'] for year in results for month in results[year]])}")
# print(f"Saldo final: {saldo}")