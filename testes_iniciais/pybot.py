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

# Função para calcular o ADX conforme o Backtrader
def get_adx_bt(high, low, close, period):
    high = np.array(high)
    low = np.array(low)
    close = np.array(close)

    tr = np.maximum.reduce([
        high[1:] - low[1:],
        np.abs(high[1:] - close[:-1]),
        np.abs(low[1:] - close[:-1])
    ])
    tr = np.insert(tr, 0, np.nan)

    plus_dm = np.where((high[1:] - high[:-1]) > (low[:-1] - low[1:]),
                       np.maximum(high[1:] - high[:-1], 0),
                       0)
    minus_dm = np.where((low[:-1] - low[1:]) > (high[1:] - high[:-1]),
                        np.maximum(low[:-1] - low[1:], 0),
                        0)
    plus_dm = np.insert(plus_dm, 0, np.nan)
    minus_dm = np.insert(minus_dm, 0, np.nan)

    atr = np.zeros_like(tr)
    plus_dm_smooth = np.zeros_like(plus_dm)
    minus_dm_smooth = np.zeros_like(minus_dm)

    # Inicialização dos valores suavizados
    atr[period - 1] = np.nansum(tr[period - 1:2 * period - 1])
    plus_dm_smooth[period - 1] = np.nansum(plus_dm[period - 1:2 * period - 1])
    minus_dm_smooth[period - 1] = np.nansum(minus_dm[period - 1:2 * period - 1])

    # Suavização de Wilder
    for i in range(period, len(tr)):
        atr[i] = atr[i - 1] - (atr[i - 1] / period) + tr[i]
        plus_dm_smooth[i] = plus_dm_smooth[i - 1] - (plus_dm_smooth[i - 1] / period) + plus_dm[i]
        minus_dm_smooth[i] = minus_dm_smooth[i - 1] - (minus_dm_smooth[i - 1] / period) + minus_dm[i]

    # Calcular DI+
    plus_di = 100 * (plus_dm_smooth / atr)
    # Calcular DI-
    minus_di = 100 * (minus_dm_smooth / atr)

    # Calcular DX
    dx = 100 * (np.abs(plus_di - minus_di) / (plus_di + minus_di))

    # Calcular ADX
    adx = np.zeros_like(dx)
    adx[:2 * period - 1] = np.nan
    adx[2 * period - 1] = np.nanmean(dx[period:2 * period])

    # Suavização do ADX
    for i in range(2 * period, len(dx)):
        adx[i] = ((adx[i - 1] * (period - 1)) + dx[i]) / period

    return adx

# Calcular os indicadores manuais e via TA-Lib
emaShort = talib.EMA(close_price, emaShortLength)
emaLong = talib.EMA(close_price, emaLongLength)
rsi = talib.RSI(close_price, timeperiod=rsiLength)
macdLine, signalLine, macdHist = macd(close_price, macdShort, macdLong, macdSignal)

# Chamar a função get_adx_bt com o período desejado
adx = get_adx_bt(high_price, low_price, close_price, adxLength)

# Bollinger Bands
upperBand, middleBand, lowerBand = talib.BBANDS(close_price, timeperiod=bbLength, nbdevup=bbMultiplier, nbdevdn=bbMultiplier, matype=0)

# Função para detectar crossover (cruzamento ascendente)
def crossover(series1, series2):
    return (series1 > series2) & (series1.shift(1) <= series2.shift(1))

# Função para detectar crossunder (cruzamento descendente)
def crossunder(series1, series2):
    return (series1 < series2) & (series1.shift(1) >= series2.shift(1))

# Converter adx para Series do pandas e alinhar com o DataFrame original
adx = pd.Series(adx, index=df.index)

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
            orders.append(f"Entrar em transação (long) em {adjusted_timestamp} com preço {entry_price}, Stop Loss: {stopLossLong}, Take Profit: {takeProfitLong}")
            position_open = True
            current_position = 'long'
            trade_count += 1

        # Reversão para Short quando o preço cruza a banda superior para baixo
        elif (close_price.iloc[i] > upperBand.iloc[i]) and crossunder(close_price, upperBand).iloc[i]:
            entry_price = close_price.iloc[i]
            quantidade = saldo / entry_price
            stopLossShort = entry_price * 1.09
            takeProfitShort = entry_price * 0.973
            orders.append(f"Entrar em transação (short) em {adjusted_timestamp} com preço {entry_price}, Stop Loss: {stopLossShort}, Take Profit: {takeProfitShort}")
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
            orders.append(f"Entrar em transação (long) em {adjusted_timestamp} com preço {entry_price}, Stop Loss: {stopLossLong}, Take Profit: {takeProfitLong}")
            position_open = True
            current_position = 'long'
            trade_count += 1
        elif shortCondition.iloc[i]:
            entry_price = close_price.iloc[i]
            quantidade = saldo / entry_price
            stopLossShort = entry_price * 1.12
            takeProfitShort = entry_price * 0.77
            orders.append(f"Entrar em transação (short) em {adjusted_timestamp} com preço {entry_price}, Stop Loss: {stopLossShort}, Take Profit: {takeProfitShort}")
            position_open = True
            current_position = 'short'
            trade_count += 1

    # Saída para long
    if position_open and current_position == 'long':
        if low_price.iloc[i] <= stopLossLong:
            saldo = quantidade * stopLossLong
            orders.append(f"Sair de transação (long) em {adjusted_timestamp} com preço {stopLossLong} (Stoploss), saldo atualizado: {saldo:.2f}")
            position_open = False
        elif high_price.iloc[i] >= takeProfitLong:
            saldo = quantidade * takeProfitLong
            orders.append(f"Sair de transação (long) em {adjusted_timestamp} com preço {takeProfitLong} (Take Profit), saldo atualizado: {saldo:.2f}")
            position_open = False
        elif shortCondition.iloc[i]:
            saldo = quantidade * close_price.iloc[i]
            orders.append(f"Sair de transação (long) em {adjusted_timestamp} com preço {close_price.iloc[i]} (Inversão para Short), saldo atualizado: {saldo:.2f}")
            entry_price = close_price.iloc[i]
            quantidade = saldo / entry_price
            stopLossShort = entry_price * 1.12
            takeProfitShort = entry_price * 0.77
            orders.append(f"Entrar em transação (short) em {adjusted_timestamp} com preço {entry_price}, Stop Loss: {stopLossShort}, Take Profit: {takeProfitShort}")
            current_position = 'short'
            trade_count += 1

    # Saída para short
    elif position_open and current_position == 'short':
        if high_price.iloc[i] >= stopLossShort:
            saldo = saldo - (quantidade * (stopLossShort - entry_price))
            orders.append(f"Sair de transação (short) em {adjusted_timestamp} com preço {stopLossShort} (Stoploss), saldo atualizado: {saldo:.2f}")
            position_open = False
        elif low_price.iloc[i] <= takeProfitShort:
            saldo = saldo + (quantidade * (entry_price - takeProfitShort))
            orders.append(f"Sair de transação (short) em {adjusted_timestamp} com preço {takeProfitShort} (Take Profit), saldo atualizado: {saldo:.2f}")
            position_open = False
        elif longCondition.iloc[i]:
            saldo = saldo + (quantidade * (entry_price - close_price.iloc[i]))
            orders.append(f"Sair de transação (short) em {adjusted_timestamp} com preço {close_price.iloc[i]} (Inversão para Long), saldo atualizado: {saldo:.2f}")
            entry_price = close_price.iloc[i]
            quantidade = saldo / entry_price
            stopLossLong = entry_price * 0.92
            takeProfitLong = entry_price * 1.32
            orders.append(f"Entrar em transação (long) em {adjusted_timestamp} com preço {entry_price}, Stop Loss: {stopLossLong}, Take Profit: {takeProfitLong}")
            current_position = 'long'
            trade_count += 1

# Exibir as ordens geradas
for order in orders:
    print(order)

# Exibir o número total de trades
print(f"Total de trades: {trade_count}")
