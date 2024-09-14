import sys
import os

# Defina o caminho base manualmente
base_path = os.path.abspath(os.path.join(os.getcwd(), '..', '..'))

# Adicione o diretório base ao caminho de pesquisa de módulos
sys.path.append(base_path)

# Agora você pode importar utils e outros módulos
#import utils
#import setups.emas as emas
#import setups.stopgain as StopGain
#import setups.stoploss as StopLoss

from evaluated_strategy import EvaluatedStrategy
from evaluator import StrategyEvaluator

import numpy as np
import itertools
import pandas as pd
from datetime import datetime
from datetime import datetime, timedelta

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

# Carregar os dados dos arquivos CSV
df_15m = pd.read_csv('BTC_15m_candles.csv')
df_5m = pd.read_csv('BTC_5m_candles.csv')

# Padronizar os nomes das colunas para minúsculas e substituir espaços por underscores
df_15m.columns = [col.lower().replace(' ', '_') for col in df_15m.columns]
df_5m.columns = [col.lower().replace(' ', '_') for col in df_5m.columns]

# Definir o tempo inicial e o tempo final
start_date = '2023-08-11'
end_date = '2024-08-01'

adjusted_start_date = adjust_date(start_date)

# Converter strings de data para datetime
start_datetime = pd.to_datetime(start_date)
end_datetime = pd.to_datetime(end_date)

# Filtrar os dados com base no tempo inicial e final
df_15m['open_time'] = pd.to_datetime(df_15m['open_time'])
df_5m['open_time'] = pd.to_datetime(df_5m['open_time'])
df_15m = df_15m[(df_15m['open_time'] >= start_datetime) & (df_15m['open_time'] <= end_datetime)]
df_5m = df_5m[(df_5m['open_time'] >= start_datetime) & (df_5m['open_time'] <= end_datetime)]

# Definir os parâmetros e seus intervalos
param_grid = {
    'short_period': list(range(5, 10, 1)),
    'long_period': list(range(44, 61, 1)),
    'ratio': np.round(np.arange(1.0, 4.0, 0.1), 1).tolist(),
    'timeframe': ['15m'],
    'stop_candles': list(range(20, 30, 1)),
    'ativo': ['BTCUSDT'],
    'setup': ['EMA']
}

param_combinations = list(itertools.product(*param_grid.values()))

strategies = []
starting_balance = 1000000
trading_tax = 0.03

if param_grid['timeframe'] == ['15m']:
    data = df_15m.copy()
else:
    data = df_5m.copy()

for combination in param_combinations:
    params = dict(zip(param_grid.keys(), combination))
    strategy = EvaluatedStrategy(params, starting_balance, trading_tax, data, params['short_period'], params['long_period'], params['stop_candles'], params['ratio'])
    strategies.append(strategy)

evaluator = StrategyEvaluator(data, strategies, lambda strategy: strategy.balance - 1000, 999)
while not evaluator.is_done_evaluating():
    evaluator.evaluate_next_candle()
results = evaluator.results()
best_performance = results['performance']
best_params = results['identifier']
best_result = {
    'final_saldo': results['metrics']['balance'],
    'trades': results['metrics']['trades'],
    'results': results['metrics']['monthly_results'],
    'max_drawdown': results['metrics']['max_drawdown'],
    'ganhos': results['metrics']['gains'],
    'perdas': results['metrics']['losses']
}
# Imprimir os resultados detalhados do melhor desempenho
if best_result:
    saldo = best_result['final_saldo']
    trades = best_result['trades']
    results = best_result['results']
    max_drawdown = best_result['max_drawdown']
    ganhos = best_result['ganhos']
    perdas = best_result['perdas']

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
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: Teste finalizado: {params['ativo']} - {params['timeframe']}.")
    print(f"Setup: EMA {params['short_period']}/{params['long_period']} rompimento, stopgain ratio {params['ratio']} e stoploss {params['stop_candles']} candles")