import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import talib

# Carregar o CSV com nome atualizado
df = pd.read_csv('testes_iniciais/BYBIT_BTCUSDT.P_1h.csv')
df['time'] = pd.to_datetime(df['time'], unit='s')

# Atribuir os dados às variáveis
timestamp = df['time']
open_price = df['open']
high_price = df['high']
low_price = df['low']
close_price = df['close']
volume = df['Volume']

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

# Funções para cálculo manual do MACD
def macd(series, fast_period, slow_period, signal_period):
    ema_fast = talib.EMA(series, fast_period)
    ema_slow = talib.EMA(series, slow_period)
    macd_line = ema_fast - ema_slow
    signal_line = talib.EMA(macd_line, signal_period)
    macd_hist = macd_line - signal_line
    return macd_line, signal_line, macd_hist

def get_adx_manual(high, low, close, di_lookback, adx_smoothing):
    tr = []
    previous_close = close.iloc[0]
    plus_dm = []
    minus_dm = []

    for i in range(1, len(close)):
        current_plus_dm = high.iloc[i] - high.iloc[i-1]
        current_minus_dm = low.iloc[i-1] - low.iloc[i]
        plus_dm.append(max(current_plus_dm, 0) if current_plus_dm > current_minus_dm else 0)
        minus_dm.append(max(current_minus_dm, 0) if current_minus_dm > current_plus_dm else 0)
        
        tr1 = high.iloc[i] - low.iloc[i]
        tr2 = abs(high.iloc[i] - previous_close)
        tr3 = abs(low.iloc[i] - previous_close)
        true_range = max(tr1, tr2, tr3)
        tr.append(true_range)
        
        previous_close = close.iloc[i]
    
    tr.insert(0, np.nan)
    plus_dm.insert(0, np.nan)
    minus_dm.insert(0, np.nan)
    
    tr = pd.Series(tr)
    plus_dm = pd.Series(plus_dm)
    minus_dm = pd.Series(minus_dm)
    
    atr = tr.rolling(window=di_lookback).mean()
    
    plus_di = 100 * (plus_dm.ewm(alpha=1/di_lookback).mean() / atr)
    minus_di = 100 * (minus_dm.ewm(alpha=1/di_lookback).mean() / atr)
    
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
    
    adx = dx.ewm(alpha=1/adx_smoothing).mean()
    
    return plus_di, minus_di, adx

# Calcular os indicadores manuais e via TA-Lib
emaShort = talib.EMA(close_price, emaShortLength)
emaLong = talib.EMA(close_price, emaLongLength)
rsi = talib.RSI(close_price, timeperiod=rsiLength)
macdLine, signalLine, macdHist = macd(close_price, macdShort, macdLong, macdSignal)
plus_di, minus_di, adx = get_adx_manual(high_price, low_price, close_price, adxLength, adxSmoothing)

# Bollinger Bands
upperBand, middleBand, lowerBand = talib.BBANDS(close_price, timeperiod=bbLength, nbdevup=bbMultiplier, nbdevdn=bbMultiplier, matype=0)

# Função para detectar crossover (cruzamento ascendente)
def crossover(series1, series2):
    cross = (series1 > series2) & (series1.shift(1) <= series2.shift(1))
    cross_filled = cross.fillna(False)
    return cross_filled

# Função para detectar crossunder (cruzamento descendente)
def crossunder(series1, series2):
    cross = (series1 < series2) & (series1.shift(1) >= series2.shift(1))
    cross_filled = cross.fillna(False)
    return cross_filled

# Condição de mercado em tendência (baseada no ADX)
trendingMarket = adx > adxThreshold

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
    adjusted_timestamp = timestamp.iloc[i]

    # Estratégia de Mean Reversion para mercado lateral
    if isLateral.iloc[i]:
        # Reversão para Long quando o preço cruza a banda inferior para cima
        if (close_price.iloc[i] < lowerBand.iloc[i]) and crossover(close_price, lowerBand).iloc[i]:
            entry_price = close_price.iloc[i]
            quantidade = saldo / entry_price
            stopLossLong = entry_price * 0.973
            takeProfitLong = entry_price * 1.11
            orders.append(f"entrar em transação (long) em {adjusted_timestamp} com preço {entry_price}, Stop Loss: {stopLossLong}, Take Profit: {takeProfitLong}")
            position_open = True
            current_position = 'long'
            trade_count += 1

        # Reversão para Short quando o preço cruza a banda superior para baixo
        elif (close_price.iloc[i] > upperBand.iloc[i]) and crossunder(close_price, upperBand).iloc[i]:
            entry_price = close_price.iloc[i]
            quantidade = saldo / entry_price
            stopLossShort = entry_price * 1.09
            takeProfitShort = entry_price * 0.973
            orders.append(f"entrar em transação (short) em {adjusted_timestamp} com preço {entry_price}, Stop Loss: {stopLossShort}, Take Profit: {takeProfitShort}")
            position_open = True
            current_position = 'short'
            trade_count += 1

    # Se não há uma posição aberta e o mercado não está lateral
    if not position_open and not isLateral.iloc[i]:
        if longCondition.iloc[i]:
            entry_price = close_price.iloc[i]
            quantidade = saldo / entry_price
            stopLossLong = entry_price * 0.92
            takeProfitLong = entry_price * 1.32
            orders.append(f"entrar em transação (long) em {adjusted_timestamp} com preço {entry_price}, Stop Loss: {stopLossLong}, Take Profit: {takeProfitLong}")
            position_open = True
            current_position = 'long'
            trade_count += 1
        elif shortCondition.iloc[i]:
            entry_price = close_price.iloc[i]
            quantidade = saldo / entry_price
            stopLossShort = entry_price * 1.12
            takeProfitShort = entry_price * 0.77
            orders.append(f"entrar em transação (short) em {adjusted_timestamp} com preço {entry_price}, Stop Loss: {stopLossShort}, Take Profit: {takeProfitShort}")
            position_open = True
            current_position = 'short'
            trade_count += 1

    # Saída para long
    if position_open and current_position == 'long':
        if low_price.iloc[i] <= stopLossLong:
            saldo = quantidade * stopLossLong
            orders.append(f"sair de transação (long) em {adjusted_timestamp} com preço {stopLossLong} (Stoploss), saldo atualizado: {saldo:.2f}")
            position_open = False
        elif high_price.iloc[i] >= takeProfitLong:
            saldo = quantidade * takeProfitLong
            orders.append(f"sair de transação (long) em {adjusted_timestamp} com preço {takeProfitLong} (Take Profit), saldo atualizado: {saldo:.2f}")
            position_open = False
        elif shortCondition.iloc[i]:
            saldo = quantidade * close_price.iloc[i]
            orders.append(f"sair de transação (long) em {adjusted_timestamp} com preço {close_price.iloc[i]} (Inversão para Short), saldo atualizado: {saldo:.2f}")
            entry_price = close_price.iloc[i]
            quantidade = saldo / entry_price
            stopLossShort = entry_price * 1.12
            takeProfitShort = entry_price * 0.77
            orders.append(f"entrar em transação (short) em {adjusted_timestamp} com preço {entry_price}, Stop Loss: {stopLossShort}, Take Profit: {takeProfitShort}")
            current_position = 'short'
            trade_count += 1

    # Saída para short
    elif position_open and current_position == 'short':
        if high_price.iloc[i] >= stopLossShort:
            saldo = saldo - (quantidade * (stopLossShort - entry_price))
            orders.append(f"sair de transação (short) em {adjusted_timestamp} com preço {stopLossShort} (Stoploss), saldo atualizado: {saldo:.2f}")
            position_open = False
        elif low_price.iloc[i] <= takeProfitShort:
            saldo = saldo + (quantidade * (entry_price - takeProfitShort))
            orders.append(f"sair de transação (short) em {adjusted_timestamp} com preço {takeProfitShort} (Take Profit), saldo atualizado: {saldo:.2f}")
            position_open = False
        elif longCondition.iloc[i]:
            saldo = saldo + (quantidade * (entry_price - close_price.iloc[i]))
            orders.append(f"sair de transação (short) em {adjusted_timestamp} com preço {close_price.iloc[i]} (Inversão para Long), saldo atualizado: {saldo:.2f}")
            entry_price = close_price.iloc[i]
            quantidade = saldo / entry_price
            stopLossLong = entry_price * 0.92
            takeProfitLong = entry_price * 1.32
            orders.append(f"entrar em transação (long) em {adjusted_timestamp} com preço {entry_price}, Stop Loss: {stopLossLong}, Take Profit: {takeProfitLong}")
            current_position = 'long'
            trade_count += 1

# Exibir as ordens geradas
for order in orders:
    print(order)

# Exibir o número total de trades
print(f"Total de trades: {trade_count}")

# Imprimir os valores do ADX, DI+ e DI- para cada vela
print("\nValores do ADX, DI+ e DI- para cada vela:")
for i in range(len(df)):
    adjusted_timestamp = timestamp.iloc[i]
    date = adjusted_timestamp.date()
    time = adjusted_timestamp.time()
    current_adx = adx.iloc[i]
    current_plus_di = plus_di.iloc[i]
    current_minus_di = minus_di.iloc[i]
    trending = current_adx > adxThreshold
    print(f"Data: {date}, Hora: {time}, ADX: {current_adx:.2f}, Tendência: {trending}")

# Imprimir o saldo final
print(f"\nSaldo final: {saldo:.2f}")
