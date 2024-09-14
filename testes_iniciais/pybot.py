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
bbLength = 14  # Período do Bollinger Bands
bbMultiplier = 1.7  # Multiplicador do Bollinger Bands
lateralThreshold = 0.005  # Limite de Lateralização
adx_threshold_value = 25  # Valor padrão para ADX threshold

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
    params = (('period', adxLength),)

    def __init__(self):
        adx = bt.indicators.ADX(self.data, period=self.p.period)
        self.lines.adx = adx.adx
        self.lines.plus_di = bt.indicators.PlusDI(self.data, period=self.p.period)
        self.lines.minus_di = bt.indicators.MinusDI(self.data, period=self.p.period)

# Configurando a estratégia do Backtrader para usar ADX e DI's
class ADXStrategy(bt.Strategy):
    def __init__(self):
        self.adx_indicator = ADXIndicator(self.data)
        self.order = None  # Variável para armazenar ordens em aberto

    def log(self, txt, dt=None):
        ''' Função de logging para exibir mensagens no console '''
        dt = dt or self.data.datetime.date(0)
        print(f'{dt}: {txt}')

    def notify_order(self, order):
        ''' Notificação de execução de ordens '''
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'COMPRA EXECUTADA, Preço: {order.executed.price:.2f}, Custo: {order.executed.value:.2f}, Comissão: {order.executed.comm:.2f}')
            elif order.issell():
                self.log(f'VENDA EXECUTADA, Preço: {order.executed.price:.2f}, Custo: {order.executed.value:.2f}, Comissão: {order.executed.comm:.2f}')
            self.bar_executed = len(self)
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Ordem Cancelada/Rejeitada')

        self.order = None

    def notify_trade(self, trade):
        ''' Notificação de trade '''
        if not trade.isclosed:
            return
        self.log(f'LUCRO/PERDA REALIZADO, Lucro: {trade.pnl:.2f}, Lucro Líquido: {trade.pnlcomm:.2f}')

    def next(self):
        adx_value = self.adx_indicator.adx[0]
        plus_di_value = self.adx_indicator.plus_di[0]
        minus_di_value = self.adx_indicator.minus_di[0]

        # Lógica de entrada e saída de trades
        if adx_value > adx_threshold_value and plus_di_value > minus_di_value:
            if not self.position:  # Sem posição aberta
                self.order = self.buy()
                self.log(f'COMPRA gerada a {self.data.close[0]}')
        elif adx_value > adx_threshold_value and minus_di_value > plus_di_value:
            if self.position:  # Com posição aberta
                self.order = self.sell()
                self.log(f'VENDA gerada a {self.data.close[0]}')

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
cerebro.addsizer(bt.sizers.FixedSize, stake=1)  # Define o tamanho da posição
cerebro.broker.setcommission(commission=0.001)  # Configura a comissão

# Rodar a estratégia do Backtrader para calcular ADX e DI's
print('Valor inicial da carteira: %.2f' % cerebro.broker.getvalue())
cerebro.run()
print('Valor final da carteira: %.2f' % cerebro.broker.getvalue())
