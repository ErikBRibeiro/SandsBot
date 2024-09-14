import pandas as pd
from datetime import datetime, timedelta
import requests
import plotly.graph_objects as go
import numpy as np

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import utils as utils
import setups.emas as emas
import setups.stopgain as StopGain
import setups.stoploss as StopLoss

def fetch_candles(symbol, interval, start_str, end_str=None):
    url = 'https://fapi.binance.com/fapi/v1/klines'  
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
        
        if response.status_code != 200:
            break
        
        new_data = response.json()
        if not new_data:
            break
        
        data.extend(new_data)
        start_time = new_data[-1][0] + 1  # Add 1 ms to avoid overlap
        
    if not data:
        return pd.DataFrame()  # Return empty DataFrame
    
    columns = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time',
               'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume',
               'taker_buy_quote_asset_volume', 'ignore']
    df = pd.DataFrame(data, columns=columns)
    df = df[['open', 'high', 'low', 'close', 'open_time', 'close_time']]
    
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
    
    return df

def plot_trades(data, trades, start_date):
    fig = go.Figure()

    data = data[data['open_time'] >= start_date]  # Filtra os dados a partir do start_date

    fig.add_trace(go.Candlestick(
        x=data['open_time'],
        open=data['open'],
        high=data['high'],
        low=data['low'],
        close=data['close'],
        name='Candlesticks',
        increasing_line_color='rgba(144, 238, 144, 0.7)',  # Verde um pouco mais claro
        decreasing_line_color='rgba(255, 99, 71, 0.7)',  # Vermelho um pouco mais claro
        increasing_fillcolor='rgba(144, 238, 144, 0.5)',
        decreasing_fillcolor='rgba(255, 99, 71, 0.5)',
    ))

    fig.add_trace(go.Scatter(
        x=data['open_time'],
        y=data['EMA_9'],
        mode='lines',
        name='EMA 9',
        line=dict(color='yellow', width=1)
    ))
    fig.add_trace(go.Scatter(
        x=data['open_time'],
        y=data['EMA_21'],
        mode='lines',
        name='EMA 21',
        line=dict(color='rgb(148,0,211)', width=1)
    ))

    for trade in trades:
        if trade['open_time'] >= start_date:  # Plota apenas trades a partir do start_date
            fig.add_trace(go.Scatter(
                x=[trade['open_time']],
                y=[trade['buy_price']],
                hovertext=[{
                    'Preço de Compra': f"{trade['buy_price']:.2f}", 
                    'Stoploss': f"{trade['stoploss']:.2f}", 
                    'Stopgain': f"{trade['stopgain']:.2f}"
                }],
                mode='markers',
                marker=dict(color='rgb(100, 149, 237)', size=15, symbol='circle'),  # Azul mais forte
                name='Buy'
            ))

            if trade['result'] == 'StopLoss':
                color = 'rgb(255, 69, 0)'  # Vermelho mais forte
                symbol = 'triangle-down'
                result_text = f"-{trade['outcome']:.2f}%"
            elif trade['result'] == 'StopGain':
                color = 'rgb(60, 179, 113)'  # Verde mais forte
                symbol = 'triangle-up'
                result_text = f"+{trade['outcome']:.2f}%"
            
            fig.add_trace(go.Scatter(
                x=[trade['close_time']],
                y=[trade['close_price']],
                hovertext=[{
                    'Fechou em': f"{trade['close_price']:.2f}", 
                    'Preço de Compra': f"{trade['buy_price']:.2f}", 
                    'Resultado': result_text
                }],
                mode='markers',
                marker=dict(color=color, size=15, symbol=symbol),
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

def calculate_sharpe_ratio(returns, risk_free_rate=0.05):
    excess_returns = returns - risk_free_rate
    mean_excess_return = np.mean(excess_returns)
    std_excess_return = np.std(excess_returns)
    sharpe_ratio = mean_excess_return / std_excess_return if std_excess_return != 0 else 0
    return sharpe_ratio

# Configurações iniciais
start_date = '2024-07-01'
end_date = datetime.now().strftime('%Y-%m-%d')
adjusted_start_date = adjust_date(start_date)

ativo = 'BTCUSDT'
timeframe = '15m'
alavancagem = 1  # Ajuste a alavancagem conforme necessário

data = fetch_candles(ativo, timeframe, adjusted_start_date, end_date)
if data.empty:
    print("No data available for the given period.")
    sys.exit()

data['close'] = data['close'].astype(float)
data['low'] = data['low'].astype(float)
data['high'] = data['high'].astype(float)
data['EMA_9'] = data['close'].ewm(span=9, adjust=False).mean()
data['EMA_21'] = data['close'].ewm(span=21, adjust=False).mean()
data['EMA_80'] = data['close'].ewm(span=80, adjust=False).mean()

saldo = 1000 * alavancagem  # Ajustando o saldo para considerar a alavancagem
taxa_por_operacao = 0.016  # Taxa padrão

max_saldo = saldo
min_saldo_since_max = saldo
max_drawdown = 0
perdas = []
ganhos = []

risk_free_rate = 0.05  # determinar taxa de juros de títulos públicos para o período testado

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
            'saldo_final': saldo,
            'max_drawdown': 0
        }

    if comprado:
        if StopLoss.sell_stoploss(data['low'].iloc[i - 1], stoploss):
            loss_percentage = utils.calculate_loss_percentage(buy_price, stoploss)
            results[year][month]['failed_trades'] += 1
            results[year][month]['perda_percentual_total'] += loss_percentage + taxa_por_operacao
            saldo -= saldo * ((loss_percentage + taxa_por_operacao) / 100)
            results[year][month]['saldo_final'] = saldo
            comprado = False
            
            print(f"{data['open_time'].iloc[i - 1]} - VENDEMOS a {round(stoploss, 2)} com PREJUÍZO de {round(loss_percentage, 2)}% indo para {round(saldo, 2)} de saldo")

            trade['close_price'] = stoploss
            trade['close_time'] = data['open_time'].iloc[i - 1]
            trade['outcome'] = loss_percentage
            trade['result'] = 'StopLoss'
            trades.append(trade)
            perdas.append(-(loss_percentage + taxa_por_operacao))

            if saldo < min_saldo_since_max:
                min_saldo_since_max = saldo
            
            drawdown = (max_saldo - min_saldo_since_max) / max_saldo * 100
                
            if drawdown > max_drawdown:
                 max_drawdown = drawdown
                    
            results[year][month]['max_drawdown'] = max_drawdown

            continue
            
        elif StopGain.sell_stopgain(data['high'].iloc[i - 1], stopgain):
            profit = utils.calculate_gain_percentage(buy_price, stopgain)
            results[year][month]['lucro'] += profit - taxa_por_operacao
            results[year][month]['successful_trades'] += 1
            saldo += saldo * ((profit - taxa_por_operacao) / 100)
            results[year][month]['saldo_final'] = saldo
            comprado = False

            print(f"{data['open_time'].iloc[i - 1]} - VENDEMOS a {round(stopgain, 2)} com LUCRO de {round(profit, 2)}% indo para {round(saldo, 2)} de saldo")

            trade['close_price'] = stopgain
            trade['close_time'] = data['open_time'].iloc[i - 1]
            trade['outcome'] = profit
            trade['result'] = 'StopGain'
            trades.append(trade)

            ganhos.append(profit - taxa_por_operacao)

            if saldo > max_saldo:
                max_saldo = saldo
                min_saldo_since_max = saldo

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

            loss_percentage = utils.calculate_loss_percentage(buy_price, stoploss)
            gain_percentage = utils.calculate_gain_percentage(buy_price, stopgain)

            print(f"{data['open_time'].iloc[i - 1]} - COMPRAMOS a {round(buy_price, 2)} com stoploss em {round(stoploss, 2)} ({round(loss_percentage, 2)}% de perda) e stopgain em {round(stopgain, 2)} ({round(gain_percentage, 2)}% de ganho)")

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

descricao_setup = "EMA 9/21 rompimento, stopgain ratio " + str(ratio) + " e stoploss 14 candles"

overall_sharpe_ratio = calculate_sharpe_ratio(np.array(ganhos + perdas), 0.15)

for year in results:
    print(f"Ano: {year}")
    print(f"  Operações realizadas: {sum([results[year][month]['open_trades'] for month in results[year]])}")
    print(f"  Trades de sucesso: {sum([results[year][month]['successful_trades'] for month in results[year]])}")
    print(f"  Soma dos ganhos: {sum([results[year][month]['lucro'] for month in results[year]]):.2f}%")
    try:
        print(f"  Ganho médio por trade: {sum([results[year][month]['lucro'] for month in results[year]]) / sum([results[year][month]['successful_trades'] for month in results[year]]) :.2f}%")
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
    print(f"  Drawdown máximo do ano: {max([results[year][month]['max_drawdown'] for month in results[year]])}%")
    
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
        print(f"    Drawdown máximo: {results[year][month]['max_drawdown']:.2f}%")

        if results[year][month]['saldo_inicial'] <= results[year][month]['saldo_final']:
            print(f"    Resultado final: {(results[year][month]['saldo_final'] / results[year][month]['saldo_inicial'] - 1) * 100:.2f}%")
        else:
            print(f"    Resultado final: {((1 - (results[year][month]['saldo_final'] / results[year][month]['saldo_inicial'])) * -1) * 100:.2f}%")

        print(f"    Saldo inicial: {results[year][month]['saldo_inicial']:.2f}")
        print(f"    Saldo final: {results[year][month]['saldo_final']:.2f}")
        print("-------------------")

print("Total:")
print(f"Operações realizadas: {sum([results[year][month]['open_trades'] for year in results for month in results[year]])}")
print(f"Sharpe Ratio: {overall_sharpe_ratio:.2f}")
try:
    print(f"Taxa de acerto: {sum([results[year][month]['successful_trades'] for year in results for month in results[year]]) / sum([results[year][month]['open_trades'] for year in results for month in results[year]]) * 100:.2f}%")
except ZeroDivisionError:
    print(f"Taxa de acerto: 0")
print(f"Trades de sucesso: {sum([results[year][month]['successful_trades'] for year in results for month in results[year]])}")
print(f"Soma dos ganhos: {sum([results[year][month]['lucro'] for year in results for month in results[year]]):.2f}%")

try:
    print(f"Ganho médio por trade: {sum([results[year][month]['lucro'] for year in results for month in results[year]]) / sum([results[year][month]['successful_trades'] for year in results for month in results[year]]) :.2f}%")
except ZeroDivisionError:
    print(f"Ganho médio por trade: 0")

print(f"Trades em prejuízo: {sum([results[year][month]['failed_trades'] for year in results for month in results[year]])}")
print(f"Soma das perdas: {sum([results[year][month]['perda_percentual_total'] for year in results for month in results[year]]):.2f}%")
try:
    print(f"Perda média por trade: {sum([results[year][month]['perda_percentual_total'] for year in results for month in results[year]]) / sum([results[year][month]['failed_trades'] for year in results for month in results[year]]) :.2f}%")
except ZeroDivisionError:
    print(f"Perda média por trade: 0")
print(f"Drawdown máximo: {max_drawdown:.2f}%")

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

plot_trades(data, trades, pd.to_datetime(start_date))
