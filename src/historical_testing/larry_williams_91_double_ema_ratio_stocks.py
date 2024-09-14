import pandas as pd
from datetime import datetime, timedelta
import requests
import plotly.graph_objects as go
from alpha_vantage.timeseries import TimeSeries
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import utils as utils
import setups.emas as emas
import setups.stopgain as StopGain
import setups.stoploss as StopLoss

api_key = 'EUWCI69Q5QIR94Z4'

ts = TimeSeries(key=api_key, output_format='pandas')

def fetch_candles(symbol, interval, start_str, end_str=None):
    start_date = pd.to_datetime(start_str)
    end_date = pd.to_datetime(end_str) if end_str else None

    interval_map = {
        '1m': '1min',
        '5m': '5min',
        '15m': '15min',
        '30m': '30min',
        '1h': '60min',
        '1d': '1day'
    }

    if interval not in interval_map:
        raise ValueError(f"Intervalo '{interval}' não suportado. Use um dos seguintes: {list(interval_map.keys())}")

    data, meta_data = ts.get_intraday(symbol=symbol, interval=interval_map[interval], outputsize='full')

    if data.empty:
        print("No data fetched.")
        return pd.DataFrame()  

    df = data[(data.index >= start_date) & (data.index <= end_date)].copy()

    if '5. adjusted close' in df.columns:
        df = df.rename(columns={'1. open': 'open', '2. high': 'high', '3. low': 'low', '5. adjusted close': 'close'})
    else:
        df = df.rename(columns={'1. open': 'open', '2. high': 'high', '3. low': 'low', '4. close': 'close'})

    df['open_time'] = df.index
    df['close_time'] = df.index.shift(-1, freq=f'{interval}')

    return df

def plot_trades(data, trades):
    fig = go.Figure()

    fig.add_trace(go.Candlestick(
        x=data['open_time'],
        open=data['open'],
        high=data['high'],
        low=data['low'],
        close=data['close'],
        name='Candlesticks'
    ))

    fig.add_trace(go.Scatter(
        x=data['open_time'],
        y=data['EMA_9'],
        mode='lines',
        name='EMA 9',
        line=dict(color='green', width=1)
    ))
    fig.add_trace(go.Scatter(
        x=data['open_time'],
        y=data['EMA_21'],
        mode='lines',
        name='EMA 21',
        line=dict(color='white', width=1)
    ))

    for trade in trades:
        fig.add_trace(go.Scatter(
            x=[trade['open_time']],
            y=[trade['buy_price']],
            hovertext=[f"Preço de Compra: {trade['buy_price']}, Stoploss: {trade['stoploss']}, Stopgain: {trade['stopgain']}"],
            mode='markers',
            marker=dict(color='blue', size=15, symbol='triangle-down'),
            name='Buy'
        ))

        color = 'magenta' if trade['result'] == 'StopLoss' else 'cyan'
        
        fig.add_trace(go.Scatter(
            x=[trade['close_time']],
            y=[trade['close_price']],
            hovertext=[f"Preço de Compra: {trade['buy_price']}, Resultado: {trade['outcome']}"],
            mode='markers',
            marker=dict(color=color, size=7),
            name=trade['result']
        ))

    fig.update_layout(
        title='Trades',
        xaxis_title='Time',
        yaxis_title='Price',
        template='plotly_dark'
    )

    fig.update_yaxes(
        fixedrange=False,
        autorange=True 
    )

    fig.show()

def adjust_date(start_date):
    start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
    
    days_to_subtract = 10.40625
    new_datetime = start_datetime - timedelta(days=days_to_subtract)
    
    return new_datetime.strftime('%Y-%m-%d')

start_date = '2024-06-25'
end_date = '2024-08-27'

start_date = adjust_date(start_date)

ativo = 'NVDA'
timeframe = '15m'
alavancagem = 1  

data = fetch_candles(ativo, timeframe, start_date, end_date)
if data.empty:
    print("No data available for the given period.")
    sys.exit()

data['close'] = data['close'].astype(float)
data['low'] = data['low'].astype(float)
data['high'] = data['high'].astype(float)
data['EMA_9'] = data['close'].ewm(span=9, adjust=False).mean()
data['EMA_21'] = data['close'].ewm(span=21, adjust=False).mean()
data['EMA_80'] = data['close'].ewm(span=80, adjust=False).mean()

saldo = 1000 * alavancagem  
taxa_por_operacao = 0.016  

comprado = False

results = {}
trades = []

for i in range(999, len(data)):
    year = data['close_time'].iloc[i - 1].year
    month = data['close_time'].iloc[i - 1].month

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
        if StopLoss.sell_stoploss(data['low'].iloc[i - 1], stoploss):
            loss_percentage = utils.calculate_loss_percentage(buy_price, stoploss)
            results[year][month]['failed_trades'] += 1
            results[year][month]['perda_percentual_total'] += loss_percentage + taxa_por_operacao
            saldo -= saldo * ((loss_percentage + taxa_por_operacao) / 100)
            results[year][month]['saldo_final'] = saldo
            comprado = False
            
            print(f"{data['open_time'].iloc[i - 1]} - VENDEMOS a {round(stoploss, 2)} com PREJUÍZO percentual de {round(loss_percentage, 2)}, indo para {round(saldo, 2)} de saldo")

            trade['close_price'] = stoploss
            trade['close_time'] = data['open_time'].iloc[i - 1]
            trade['outcome'] = loss_percentage
            trade['result'] = 'StopLoss'
            trades.append(trade)

            continue
            
        elif StopGain.sell_stopgain(data['high'].iloc[i - 1], stopgain):
            profit = utils.calculate_gain_percentage(buy_price, stopgain)
            results[year][month]['lucro'] += profit - taxa_por_operacao
            results[year][month]['successful_trades'] += 1
            saldo += saldo * ((profit - taxa_por_operacao) / 100)
            results[year][month]['saldo_final'] = saldo
            comprado = False

            print(f"{data['open_time'].iloc[i - 1]} - VENDEMOS a {round(stopgain, 2)} com LUCRO percentual de {round(profit, 2)}, indo para {round(saldo, 2)} de saldo")

            trade['close_price'] = stopgain
            trade['close_time'] = data['open_time'].iloc[i - 1]
            trade['outcome'] = profit
            trade['result'] = 'StopGain'
            trades.append(trade)

            continue

    if not comprado:
        if emas.buy_double_ema_breakout(data.iloc[i - 5:i], 'EMA_9', 'EMA_21'):
            results[year][month]['open_trades'] += 1
            buy_price = data['high'].iloc[i - 2]
            stoploss = StopLoss.set_sell_stoploss_min_candles(data.iloc[i - 15:i], 14)
            if taxa_por_operacao != 0:
                saldo -= saldo * taxa_por_operacao / 100
            results[year][month]['saldo_final'] = saldo
            ratio = 3.5
            stopgain = StopGain.set_sell_stopgain_ratio(buy_price, stoploss, ratio)
            comprado = True

            print(f"{data['open_time'].iloc[i - 1]} - COMPRAMOS a {round(buy_price, 2)} com stoploss em {round(stoploss, 2)} e stopgain em {round(stopgain, 2)}")

            trade = {
                'open_time': data['open_time'].iloc[i - 1],
                'buy_price': buy_price,
                'stoploss': stoploss,
                'stopgain': stopgain,
                'close_price': 0,
                'close_time': 0,
                'outcome': 0,
                'result': ''
            }
            continue


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

    avg_loss_per_trade = total_loss / total_failed_trades if total_failed_trades != 0 else 0

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
        
        avg_loss_per_trade = results[year][month]['perda_percentual_total'] / results[year][month]['failed_trades'] if results[year][month]['failed_trades'] != 0 else 0

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

plot_trades(data, trades)
