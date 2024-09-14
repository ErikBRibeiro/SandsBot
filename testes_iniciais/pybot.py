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
adxThreshold = 12  # Nível de ADX para indicar tendência
bbLength = 14  # Período do Bollinger Bands
bbMultiplier = 1.7  # Multiplicador do Bollinger Bands
lateralThreshold = 0.005  # Limite de Lateralização

# Funções para cálculo manual do MACD com talib
def macd(series, fast_period, slow_period, signal_period):
    ema_fast = talib.EMA(series, fast_period)
    ema_slow = talib.EMA(series, slow_period)
    macd_line = ema_fast - ema_slow
    signal_line = talib.EMA(macd_line, signal_period)
    macd_hist = macd_line - signal_line
    return macd_line, signal_line, macd_hist

# Cálculo do MACD com talib
macdLine, signalLine, macdHist = macd(close_price, macdShort, macdLong, macdSignal)

# Cálculo de outros indicadores com talib
emaShort = talib.EMA(close_price, emaShortLength)
emaLong = talib.EMA(close_price, emaLongLength)
rsi = talib.RSI(close_price, timeperiod=rsiLength)
upperBand, middleBand, lowerBand = talib.BBANDS(close_price, timeperiod=bbLength, nbdevup=bbMultiplier, nbdevdn=bbMultiplier, matype=0)

# Funções para detectar crossover e crossunder
def crossover(series1, series2):
    return (series1 > series2) & (series1.shift(1) <= series2.shift(1))

def crossunder(series1, series2):
    return (series1 < series2) & (series1.shift(1) >= series2.shift(1))

# Backtrader - usar ADX e DI's
class ADXIndicator(bt.Indicator):
    lines = ('adx', 'plus_di', 'minus_di',)

    def __init__(self):
        self.adx = bt.indicators.ADX(self.data, period=adxLength)
        self.plus_di = bt.indicators.PlusDI(self.data, period=adxLength)
        self.minus_di = bt.indicators.MinusDI(self.data, period=adxLength)

# Configurando a estratégia do Backtrader apenas para calcular o ADX e os DI's
class ADXStrategy(bt.Strategy):
    def __init__(self):
        self.adx_indicator = ADXIndicator(self.data)

    def next(self):
        # Aqui você pode acessar os valores de ADX, DI+ e DI- e armazená-los para uso posterior
        self.adx_value = self.adx_indicator.adx[0]
        self.plus_di_value = self.adx_indicator.plus_di[0]
        self.minus_di_value = self.adx_indicator.minus_di[0]

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

# Configurar o cerebro para rodar a estratégia do Backtrader
cerebro = bt.Cerebro()
cerebro.adddata(data_feed)
cerebro.addstrategy(ADXStrategy)
cerebro.broker.set_cash(1000000)

# Rodar a estratégia do Backtrader para calcular ADX e DI's
strategy_instance = cerebro.run()[0]

# Loop para verificar e executar as ordens
orders = []
trade_count = 0
saldo = 1_000_000  # Saldo inicial de 1.000.000
position_open = False  # Indica se estamos em uma transação
current_position = None  # 'long' ou 'short'
quantidade = 0  # Quantidade de BTC comprada/vendida na transação

# Revisão na lógica de tendência para evitar inversões incorretas
for i in range(len(df)):
    adjusted_timestamp = timestamp[i]

    # Pegando valores calculados do ADX e DI pelo Backtrader
    adx_value = strategy_instance.adx_indicator.adx[i]
    plus_di_value = strategy_instance.adx_indicator.plus_di[i]
    minus_di_value = strategy_instance.adx_indicator.minus_di[i]

    # Condições de mercado em tendência (baseadas no ADX)
    trendingMarket = adx_value > adxThreshold

    # Condições de lateralização com Bollinger Bands
    bandWidth = (upperBand[i] - lowerBand[i]) / middleBand[i]
    isLateral = bandWidth < lateralThreshold

    # Condição Long
    longCondition = (crossover(emaShort, emaLong)[i]) & (rsi[i] < 60) & (macdHist[i] > 0.5) & trendingMarket

    # Condição Short
    shortCondition = (crossunder(emaShort, emaLong)[i]) & (rsi[i] > 40) & (macdHist[i] < -0.5) & trendingMarket

    # Exibir os valores dos indicadores manuais para cada vela
    print(f"Vela: {adjusted_timestamp}")
    print(f"EMA Curta: {emaShort[i]}, EMA Longa: {emaLong[i]}")
    print(f"RSI: {rsi[i]}")
    print(f"MACD Line: {macdLine[i]}, Signal Line: {signalLine[i]}, MACD Hist: {macdHist[i]}")
    print(f"ADX: {adx_value}, DI+: {plus_di_value}, DI-: {minus_di_value}")
    print(f"Bollinger Upper: {upperBand[i]}, Bollinger Lower: {lowerBand[i]}")
    print("-" * 50)

    # Estratégia de Mean Reversion para mercado lateral
    if isLateral:
        if (close_price[i] < lowerBand[i]) and crossover(close_price, lowerBand)[i]:
            entry_price = close_price[i]
            quantidade = saldo / entry_price
            stopLossLong = entry_price * 0.973
            takeProfitLong = entry_price * 1.11
            orders.append(f"entrar em transação (long) em {adjusted_timestamp} com preço {entry_price}, Stop Loss: {stopLossLong}, Take Profit: {takeProfitLong}")
            position_open = True
            current_position = 'long'
            trade_count += 1
        elif (close_price[i] > upperBand[i]) and crossunder(close_price, upperBand)[i]:
            entry_price = close_price[i]
            quantidade = saldo / entry_price
            stopLossShort = entry_price * 1.09
            takeProfitShort = entry_price * 0.973
            orders.append(f"entrar em transação (short) em {adjusted_timestamp} com preço {entry_price}, Stop Loss: {stopLossShort}, Take Profit: {takeProfitShort}")
            position_open = True
            current_position = 'short'
            trade_count += 1

    # Condições de mercado em tendência (não lateral)
    elif not isLateral:
         if not position_open and longCondition:
            entry_price = close_price[i]
            quantidade = saldo / entry_price
            stopLossLong = entry_price * 0.92
            takeProfitLong = entry_price * 1.32
            orders.append(f"entrar em transação (long) em {adjusted_timestamp} com preço {entry_price}, Stop Loss: {stopLossLong}, Take Profit: {takeProfitLong}")
            position_open = True
            current_position = 'long'
            trade_count += 1
        elif not position_open and shortCondition:
            entry_price = close_price[i]
            quantidade = saldo / entry_price
            stopLossShort = entry_price * 1.12
            takeProfitShort = entry_price * 0.77
            orders.append(f"entrar em transação (short) em {adjusted_timestamp} com preço {entry_price}, Stop Loss: {stopLossShort}, Take Profit: {takeProfitShort}")
            position_open = True
            current_position = 'short'
            trade_count += 1

# Exibir as ordens geradas
for order in orders:
    print(order)

# Exibir o número total de trades
print(f"Total de trades: {trade_count}")
