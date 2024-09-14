import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import talib
import backtrader as bt


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

macdLine, signalLine, macdHist = macd(close_price, macdShort, macdLong, macdSignal)

adx = bt.indicators.ADX(df, period=adxLength)
plus_di = bt.indicators.PlusDI(df, period=adxLength)
minus_di = bt.indicators.MinusDI(df, period=adxLength)

# Calcular os indicadores manuais e via TA-Lib
emaShort = talib.EMA(close_price, emaShortLength)
emaLong = talib.EMA(close_price, emaLongLength)
rsi = talib.RSI(close_price, timeperiod=rsiLength)

# Usar Backtrader apenas para calcular ADX e indicadores direcionais
class ADXIndicator(bt.Indicator):
    lines = ('adx', 'plus_di', 'minus_di',)

    def __init__(self):
        self.adx = bt.indicators.ADX(self.data, period=adxLength)
        self.plus_di = bt.indicators.PlusDI(self.data, period=adxLength)
        self.minus_di = bt.indicators.MinusDI(self.data, period=adxLength)

# Alimentar o DataFrame no Backtrader
data_feed = bt.feeds.PandasData(
    dataname=df,
    datetime='time',
    open='open',
    high='high',
    low='low',
    close='close',
    volume='Volume'
)

# Configurando a estratégia do Backtrader apenas para calcular o ADX e os DI's
class ADXStrategy(bt.Strategy):
    def __init__(self):
        self.adx_indicator = ADXIndicator(self.data)

    def next(self):
        # Exibir os valores calculados do ADX e DI+ e DI- para cada vela
        print(f"Data: {self.data.datetime.date(0)} Time: {self.data.datetime.time(0)}")
        print(f"ADX: {self.adx_indicator.adx[0]}")
        print(f"DI+: {self.adx_indicator.plus_di[0]}, DI-: {self.adx_indicator.minus_di[0]}")
        print("-" * 50)


# Bollinger Bands (mantendo o cálculo via TA-Lib)
upperBand, middleBand, lowerBand = talib.BBANDS(close_price, timeperiod=bbLength, nbdevup=bbMultiplier, nbdevdn=bbMultiplier, matype=0)

# Função para detectar crossover (cruzamento ascendente)
def crossover(series1, series2):
    return (series1 > series2) & (series1.shift(1) <= series2.shift(1))

# Função para detectar crossunder (cruzamento descendente)
def crossunder(series1, series2):
    return (series1 < series2) & (series1.shift(1) >= series2.shift(1))

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


class ADXIndicator(bt.Indicator):
    lines = ('adx', 'plus_di', 'minus_di',)

    def __init__(self):
        self.adx = bt.indicators.ADX(self.data, period=adxLength)
        self.plus_di = bt.indicators.PlusDI(self.data, period=adxLength)
        self.minus_di = bt.indicators.MinusDI(self.data, period=adxLength)

# Alimentar o DataFrame no Backtrader
data_feed = bt.feeds.PandasData(
    dataname=df,
    datetime='time',
    open='open',
    high='high',
    low='low',
    close='close',
    volume='Volume'
)

# Configurando a estratégia do Backtrader apenas para calcular o ADX e os DI's
class ADXStrategy(bt.Strategy):
    def __init__(self):
        self.adx_indicator = ADXIndicator(self.data)

    def next(self):
        # Exibir os valores calculados do ADX e DI+ e DI- para cada vela
        print(f"Data: {self.data.datetime.date(0)} Time: {self.data.datetime.time(0)}")
        print(f"ADX: {self.adx_indicator.adx[0]}")
        print(f"DI+: {self.adx_indicator.plus_di[0]}, DI-: {self.adx_indicator.minus_di[0]}")
        print("-" * 50)

# Configurar o cerebro para rodar a estratégia do Backtrader
cerebro = bt.Cerebro()
cerebro.adddata(data_feed)
cerebro.addstrategy(ADXStrategy)
cerebro.broker.set_cash(1000000)

# Rodar a estratégia
cerebro.run()
# Loop para verificar e executar as ordens
for i in range(len(df)):
    adjusted_timestamp = timestamp[i]

    # Exibir os valores dos indicadores manuais para cada vela
    print(f"Vela: {adjusted_timestamp}")
    print(f"EMA Curta: {emaShort[i]}, EMA Longa: {emaLong[i]}")
    print(f"RSI: {rsi[i]}")
    print(f"MACD Line: {macdLine[i]}, Signal Line: {signalLine[i]}, MACD Hist: {macdHist[i]}")
    print(f"ADX: {adx[i]}")
    print(f"Bollinger Upper: {upperBand[i]}, Bollinger Lower: {lowerBand[i]}")
    print("-" * 50)

    # Estratégia de Mean Reversion para mercado lateral
    if isLateral[i]:
        # Reversão para Long quando o preço cruza a banda inferior para cima
        if (close_price[i] < lowerBand[i]) and crossover(close_price, lowerBand)[i]:
            entry_price = close_price[i]
            quantidade = saldo / entry_price
            stopLossLong = entry_price * 0.973
            takeProfitLong = entry_price * 1.11
            orders.append(f"entrar em transação (long) em {adjusted_timestamp} com preço {entry_price}, Stop Loss: {stopLossLong}, Take Profit: {takeProfitLong}")
            position_open = True
            current_position = 'long'
            trade_count += 1

        # Reversão para Short quando o preço cruza a banda superior para baixo
        elif (close_price[i] > upperBand[i]) and crossunder(close_price, upperBand)[i]:
            entry_price = close_price[i]
            quantidade = saldo / entry_price
            stopLossShort = entry_price * 1.09
            takeProfitShort = entry_price * 0.973
            orders.append(f"entrar em transação (short) em {adjusted_timestamp} com preço {entry_price}, Stop Loss: {stopLossShort}, Take Profit: {takeProfitShort}")
            position_open = True
            current_position = 'short'
            trade_count += 1

    # Se não há uma posição aberta e o mercado não está lateral
    if not position_open and not isLateral[i]:
        if longCondition[i]:
            entry_price = close_price[i]
            quantidade = saldo / entry_price
            stopLossLong = entry_price * 0.92
            takeProfitLong = entry_price * 1.32
            orders.append(f"entrar em transação (long) em {adjusted_timestamp} com preço {entry_price}, Stop Loss: {stopLossLong}, Take Profit: {takeProfitLong}")
            position_open = True
            current_position = 'long'
            trade_count += 1
        elif shortCondition[i]:
            entry_price = close_price[i]
            quantidade = saldo / entry_price
            stopLossShort = entry_price * 1.12
            takeProfitShort = entry_price * 0.77
            orders.append(f"entrar em transação (short) em {adjusted_timestamp} com preço {entry_price}, Stop Loss: {stopLossShort}, Take Profit: {takeProfitShort}")
            position_open = True
            current_position = 'short'
            trade_count += 1

    # Saída para long
    if position_open and current_position == 'long':
        if low_price[i] <= stopLossLong:
            saldo = quantidade * stopLossLong
            orders.append(f"sair de transação (long) em {adjusted_timestamp} com preço {stopLossLong} (Stoploss), saldo atualizado: {saldo:.2f}")
            position_open = False
        elif high_price[i] >= takeProfitLong:
            saldo = quantidade * takeProfitLong
            orders.append(f"sair de transação (long) em {adjusted_timestamp} com preço {takeProfitLong} (Take Profit), saldo atualizado: {saldo:.2f}")
            position_open = False
        elif shortCondition[i]:
            saldo = quantidade * close_price[i]
            orders.append(f"sair de transação (long) em {adjusted_timestamp} com preço {close_price[i]} (Inversão para Short), saldo atualizado: {saldo:.2f}")
            entry_price = close_price[i]
            quantidade = saldo / entry_price
            stopLossShort = entry_price * 1.12
            takeProfitShort = entry_price * 0.77
            orders.append(f"entrar em transação (short) em {adjusted_timestamp} com preço {entry_price}, Stop Loss: {stopLossShort}, Take Profit: {takeProfitShort}")
            current_position = 'short'
            trade_count += 1

    # Saída para short
    elif position_open and current_position == 'short':
        if high_price[i] >= stopLossShort:
            saldo = saldo - (quantidade * (stopLossShort - entry_price))
            orders.append(f"sair de transação (short) em {adjusted_timestamp} com preço {stopLossShort} (Stoploss), saldo atualizado: {saldo:.2f}")
            position_open = False
        elif low_price[i] <= takeProfitShort:
            saldo = saldo + (quantidade * (entry_price - takeProfitShort))
            orders.append(f"sair de transação (short) em {adjusted_timestamp} com preço {takeProfitShort} (Take Profit), saldo atualizado: {saldo:.2f}")
            position_open = False
        elif longCondition[i]:
            saldo = saldo + (quantidade * (entry_price - close_price[i]))
            orders.append(f"sair de transação (short) em {adjusted_timestamp} com preço {close_price[i]} (Inversão para Long), saldo atualizado: {saldo:.2f}")
            entry_price = close_price[i]
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
