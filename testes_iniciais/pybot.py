import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import talib

# Carregar o CSV com nome atualizado
df = pd.read_csv('BYBIT_BTCUSDT.P_1h.csv')
#df['time'] = pd.to_datetime(df['time'], unit='s')

# Atribuir os dados às variáveis
timestamp = df['time']
open_price = df['open']
high_price = df['high']
low_price = df['low']
close_price = df['close']
#volume = df['Volume']

# Parâmetros configuráveis
emaShortLength = 11  # Período da EMA Curta
emaLongLength = 55  # Período da EMA Longa
rsiLength = 22  # Período do RSI
macdShort = 15  # Período Curto MACD
macdLong = 34  # Período Longo MACD
macdSignal = 11  # Período de Sinal MACD
adxLength = 16  # Período ADX (igual ao DI Length)
adxSmoothing = 13  # ADX Smoothing
adxThreshold = 12  # Nível de ADX para indicar tendência
bbLength = 14  # Período do Bollinger Bands
bbMultiplier = 1.7  # Multiplicador do Bollinger Bands
lateralThreshold = 0.005  # Limite de Lateralização
stopgain_lateral_long = 1.11
stoploss_lateral_long = 0.973
stopgain_lateral_short = 0.973
stoploss_lateral_short = 1.09
stopgain_normal_long = 1.32
stoploss_normal_long = 0.92
stopgain_normal_short = 0.77
stoploss_normal_short = 1.12
# Funções para cálculo manual do MACD
def macd(series, fast_period, slow_period, signal_period):
    ema_fast = talib.EMA(series, fast_period)
    ema_slow = talib.EMA(series, slow_period)
    macd_line = ema_fast - ema_slow
    signal_line = talib.EMA(macd_line, signal_period)
    macd_hist = macd_line - signal_line
    return macd_line, signal_line, macd_hist

def get_adx_manual(high, low, close, di_lookback, adx_smoothing):
    # Inicializando listas para armazenar os resultados
    tr = []
    previous_close = close[0]  # Primeiro valor de fechamento
    
    plus_dm = []
    minus_dm = []

    for i in range(1, len(close)):
        # Calcular +DM e -DM
        current_plus_dm = high[i] - high[i-1]
        current_minus_dm = low[i-1] - low[i]
        plus_dm.append(max(current_plus_dm, 0) if current_plus_dm > current_minus_dm else 0)
        minus_dm.append(max(current_minus_dm, 0) if current_minus_dm > current_plus_dm else 0)
        
        # Calcular True Range (TR)
        tr1 = high[i] - low[i]
        tr2 = abs(high[i] - previous_close)
        tr3 = abs(low[i] - previous_close)
        true_range = max(tr1, tr2, tr3)
        tr.append(true_range)
        
        # Atualizar o fechamento anterior
        previous_close = close[i]
    
    # Adiciona um valor NaN para a primeira posição, pois não há fechamento anterior para calcular
    tr.insert(0, None)
    plus_dm.insert(0, None)
    minus_dm.insert(0, None)
    
    # Converter listas para Series do pandas
    tr = pd.Series(tr)
    plus_dm = pd.Series(plus_dm)
    minus_dm = pd.Series(minus_dm)
    
    # Calcular o ATR usando o lookback fornecido para DI
    atr = tr.rolling(window=di_lookback).mean()
    
    # Calcular DI+ e DI- com o lookback de DI
    plus_di = 100 * (plus_dm.ewm(alpha=1/di_lookback).mean() / atr)
    minus_di = 100 * (minus_dm.ewm(alpha=1/di_lookback).mean() / atr)
    
    # Calcular o DX
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
    
    # Calcular o ADX usando o smoothing específico para o ADX
    adx = dx.ewm(alpha=1/adx_smoothing).mean()
    
    # Retornar os valores calculados
    return plus_di, minus_di, adx


emaShort = (talib.EMA(close_price, emaShortLength)).round(0)
emaLong = (talib.EMA(close_price, emaLongLength)).round(0)
rsi = talib.RSI(close_price, timeperiod=rsiLength)
plus_di, minus_di, adx = get_adx_manual(high_price, low_price, close_price, adxLength, adxSmoothing)
adx = adx.fillna(0).astype(int)
macdLine, signalLine, macdHist = macd(close_price, macdShort, macdLong, macdSignal)
upperBand, middleBand, lowerBand = talib.BBANDS(close_price, timeperiod=bbLength, nbdevup=bbMultiplier, nbdevdn=bbMultiplier, matype=0)


def crossover(series1, series2):
    return (series1 > series2) & (series1.shift(1) <= series2.shift(1))



# Função para detectar crossover (cruzamento ascendente)
def crossover(series1, series2):
    series1_vals = series1.values
    series2_vals = series2.values
    cross = (series1_vals[1:] > series2_vals[1:]) & (series1_vals[:-1] <= series2_vals[:-1])
    cross = np.concatenate(([False], cross))  # Prepend False to align lengths
    return pd.Series(cross, index=series1.index)

# Função para detectar crossunder (cruzamento descendente)
def crossunder(series1, series2):
    series1_vals = series1.values
    series2_vals = series2.values
    cross = (series1_vals[1:] < series2_vals[1:]) & (series1_vals[:-1] >= series2_vals[:-1])
    cross = np.concatenate(([False], cross))  # Prepend False to align lengths
    return pd.Series(cross, index=series1.index)


# Condição de mercado em tendência (baseada no ADX)
trendingMarket = adx >= adxThreshold

# Condições de lateralização com Bollinger Bands
bandWidth = (upperBand - lowerBand) / middleBand
isLateral = bandWidth < lateralThreshold

# Saldo inicial
saldo = 1_000_000  # Saldo inicial de 1.000.000
quantidade = 0  # Quantidade de BTC comprada/vendida na transação
position_open = False  # Indica se estamos em uma transação
current_position = None  # 'long' ou 'short'
entry_price = None  # Preço de entrada da posição
orders = []
trade_count = 0  # Contador de trades

# Revisão na lógica de tendência para evitar inversões incorretas
# Condição Long
longCondition = (crossover(emaShort, emaLong)) & (rsi < 60) & (macdHist > 0.5) & trendingMarket

# Condição Short
shortCondition = (crossunder(emaShort, emaLong)) & (rsi > 40) & (macdHist < -0.5) & trendingMarket

# Loop para verificar e executar as ordens
for i in range(len(df)):
    adjusted_timestamp = timestamp[i]

    # Exibir os valores dos indicadores manuais para cada vela
    print(f"Vela: {adjusted_timestamp}")
    print(f"EMA Curta: {emaShort[i]}, EMA Longa: {emaLong[i]}")
    print(f"RSI: {rsi[i]}")
    print(f"MACD Line: {macdLine[i]}, Signal Line: {signalLine[i]}, MACD Hist: {macdHist[i]}")
    print(f"ADX: {adx[i]}")
    print(f"Bollinger Upper: {upperBand[i]}, Bollinger Middle {middleBand[i]}, Bollinger Lower: {lowerBand[i]}")
    print(f"bandWidth: {bandWidth[i]}")
    print("-" * 50)

    # Estratégia de Mean Reversion para mercado lateral
    if isLateral[i]:
        if not position_open:
            if (close_price[i] < lowerBand[i]) and crossover(close_price, lowerBand)[i] and longCondition[i]:
                entry_price = close_price[i]
                quantidade = saldo / entry_price
                orders.append(f"entrar em transação (long lateral) em {adjusted_timestamp} com preço {entry_price}, Stop Loss: {stopLossLong}, Take Profit: {takeProfitLong}")
                position_open = True
                current_position = 'long'
                trade_count += 1
            elif (close_price[i] > upperBand[i]) and crossover(close_price, upperBand)[i] and shortCondition[i]:
                entry_price = close_price[i]
                quantidade = saldo / entry_price
                orders.append(f"entrar em transação (short lateral) em {adjusted_timestamp} com preço {entry_price}, Stop Loss: {stopLossShort}, Take Profit: {takeProfitShort}")
                position_open = True
                current_position = 'short'
                trade_count += 1
        elif position_open:
            if current_position == 'long':
                if (close_price[i] > upperBand[i]) and crossover(close_price, upperBand)[i] and shortCondition[i]:
                    if shortCondition[i]:
                        saldo = quantidade * close_price[i]
                        orders.append(f"sair de transação (long) em {adjusted_timestamp} com preço {close_price[i]} (Inversão para Short), saldo atualizado: {saldo:.2f}")
                        entry_price = close_price[i]
                        quantidade = saldo / entry_price
                        orders.append(f"entrar em transação (short lateral) em {adjusted_timestamp} com preço {entry_price}, Stop Loss: {stopLossShort}, Take Profit: {takeProfitShort}")
                        current_position = 'short'
                        trade_count += 1
            elif current_position == 'short':
                if (close_price[i] < lowerBand[i]) and crossover(close_price, lowerBand)[i] and longCondition[i]:
                    if longCondition[i]:
                        saldo = saldo + (quantidade * (entry_price - close_price[i]))
                        orders.append(f"sair de transação (short) em {adjusted_timestamp} com preço {close_price[i]} (Inversão para Long), saldo atualizado: {saldo:.2f}")
                        entry_price = close_price[i]
                        quantidade = saldo / entry_price
                        orders.append(f"entrar em transação (long lateral) em {adjusted_timestamp} com preço {entry_price}, Stop Loss: {stopLossLong}, Take Profit: {takeProfitLong}")
                        current_position = 'long'
                        trade_count += 1
            if current_position == 'long':
                stopLossLong = entry_price * stoploss_lateral_long
                takeProfitLong = entry_price * stopgain_lateral_long
                if low_price[i] <= stopLossLong:
                    saldo = quantidade * stopLossLong
                    orders.append(f"sair de transação (long) em {adjusted_timestamp} com preço {stopLossLong} (Stoploss), saldo atualizado: {saldo:.2f}")
                    position_open = False
                elif high_price[i] >= takeProfitLong:
                    saldo = quantidade * takeProfitLong
                    orders.append(f"sair de transação (long) em {adjusted_timestamp} com preço {takeProfitLong} (Take Profit), saldo atualizado: {saldo:.2f}")
                    position_open = False
            elif current_position == 'short':
                stopLossShort = entry_price * stoploss_lateral_short
                takeProfitShort = entry_price * stopgain_lateral_short
                if high_price[i] >= stopLossShort:
                    saldo = saldo - (quantidade * (stopLossShort - entry_price))
                    orders.append(f"sair de transação (short) em {adjusted_timestamp} com preço {stopLossShort} (Stoploss), saldo atualizado: {saldo:.2f}")
                    position_open = False
                elif low_price[i] <= takeProfitShort:
                    saldo = saldo + (quantidade * (entry_price - takeProfitShort))
                    orders.append(f"sair de transação (short) em {adjusted_timestamp} com preço {takeProfitShort} (Take Profit), saldo atualizado: {saldo:.2f}")
                    position_open = False

    if not isLateral[i]:
        if not position_open:
            if longCondition[i]:
                entry_price = close_price[i]
                quantidade = saldo / entry_price
                orders.append(f"entrar em transação (long normal) em {adjusted_timestamp} com preço {entry_price}, Stop Loss: {stopLossLong}, Take Profit: {takeProfitLong}")
                position_open = True
                current_position = 'long'
                trade_count += 1
            elif shortCondition[i]:
                entry_price = close_price[i]
                quantidade = saldo / entry_price
                orders.append(f"entrar em transação (short normal) em {adjusted_timestamp} com preço {entry_price}, Stop Loss: {stopLossShort}, Take Profit: {takeProfitShort}")
                position_open = True
                current_position = 'short'
                trade_count += 1
        elif position_open:
            if current_position == 'long':
                if shortCondition[i]:
                    saldo = quantidade * close_price[i]
                    orders.append(f"sair de transação (long) em {adjusted_timestamp} com preço {close_price[i]} (Inversão para Short), saldo atualizado: {saldo:.2f}")
                    entry_price = close_price[i]
                    orders.append(f"entrar em transação (short normal) em {adjusted_timestamp} com preço {entry_price}, Stop Loss: {stopLossShort}, Take Profit: {takeProfitShort}")
                    current_position = 'short'
                    trade_count += 1
            elif current_position == 'short':
                if longCondition[i]:
                    saldo = saldo + (quantidade * (entry_price - close_price[i]))
                    orders.append(f"sair de transação (short) em {adjusted_timestamp} com preço {close_price[i]} (Inversão para Long), saldo atualizado: {saldo:.2f}")
                    entry_price = close_price[i]
                    quantidade = saldo / entry_price
                    orders.append(f"entrar em transação (long normal) em {adjusted_timestamp} com preço {entry_price}, Stop Loss: {stopLossLong}, Take Profit: {takeProfitLong}")
                    current_position = 'long'
                    trade_count += 1
            if current_position == 'long':
                stopLossLong = entry_price * stoploss_normal_long
                takeProfitLong = entry_price * stopgain_normal_long
                if low_price[i] <= stopLossLong:
                    saldo = quantidade * stopLossLong
                    orders.append(f"sair de transação (long) em {adjusted_timestamp} com preço {stopLossLong} (Stoploss), saldo atualizado: {saldo:.2f}")
                    position_open = False
                elif high_price[i] >= takeProfitLong:
                    saldo = quantidade * takeProfitLong
                    orders.append(f"sair de transação (long) em {adjusted_timestamp} com preço {takeProfitLong} (Take Profit), saldo atualizado: {saldo:.2f}")
                    position_open = False
            elif current_position == 'short':
                stopLossShort = entry_price * stoploss_normal_short
                takeProfitShort = entry_price * stopgain_normal_short
                if high_price[i] >= stopLossShort:
                    saldo = saldo - (quantidade * (stopLossShort - entry_price))
                    orders.append(f"sair de transação (short) em {adjusted_timestamp} com preço {stopLossShort} (Stoploss), saldo atualizado: {saldo:.2f}")
                    position_open = False
                elif low_price[i] <= takeProfitShort:
                    saldo = saldo + (quantidade * (entry_price - takeProfitShort))
                    orders.append(f"sair de transação (short) em {adjusted_timestamp} com preço {takeProfitShort} (Take Profit), saldo atualizado: {saldo:.2f}")
                    position_open = False


# Exibir as ordens geradas
for order in orders:
    print(order)

# Exibir o número total de trades
print(f"Total de trades: {trade_count}")
