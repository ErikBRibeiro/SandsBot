import pandas as pd
import talib
import numpy as np
import backtrader as bt
from datetime import datetime

# Carregar o CSV com nome atualizado
df = pd.read_csv('testes_iniciais/BYBIT_BTCUSDT.P_1h.csv')
df['time'] = pd.to_datetime(df['time'], unit='s')

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

# Definir a estratégia
class MyStrategy(bt.Strategy):
    params = (
        ('ema_short_length', 11),
        ('ema_long_length', 55),
        ('rsi_length', 22),
        ('macd_short', 15),
        ('macd_long', 34),
        ('macd_signal', 11),
        ('adx_length', 16),
        ('adx_threshold', 12),
        ('bb_length', 14),
        ('bb_multiplier', 1.7),
        ('lateral_threshold', 0.005),
    )

    def __init__(self):
        # Calcular os indicadores usando TA-Lib e numpy arrays
        close_prices = np.array(self.data.close.get(size=len(self.data)))

        self.ema_short = bt.indicators.EMA(self.data.close, period=self.params.ema_short_length)
        self.ema_long = bt.indicators.EMA(self.data.close, period=self.params.ema_long_length)
        self.rsi = bt.indicators.RSI(self.data.close, period=self.params.rsi_length)
        self.macd = bt.indicators.MACD(self.data.close, 
                                       period_me1=self.params.macd_short, 
                                       period_me2=self.params.macd_long, 
                                       period_signal=self.params.macd_signal)

        # ADX e DI+ DI- usando Backtrader
        self.adx = bt.indicators.ADX(self.data, period=self.params.adx_length)
        self.plus_di = bt.indicators.PlusDI(self.data, period=self.params.adx_length)
        self.minus_di = bt.indicators.MinusDI(self.data, period=self.params.adx_length)

        # Bollinger Bands usando TA-Lib com numpy array
        self.bbands_upper, self.bbands_middle, self.bbands_lower = talib.BBANDS(
            close_prices,
            timeperiod=self.params.bb_length, 
            nbdevup=self.params.bb_multiplier, 
            nbdevdn=self.params.bb_multiplier, 
            matype=0
        )

        self.position_open = False

    def next(self):
        # Condição para mercado em lateralização
        band_width = (self.bbands_upper[-1] - self.bbands_lower[-1]) / self.bbands_middle[-1]
        is_lateral = band_width < self.params.lateral_threshold

        # Valores de stoploss e takeprofit baseados no estado do mercado
        if is_lateral:
            stop_loss_long = self.data.close[0] * 0.973
            take_profit_long = self.data.close[0] * 1.11
            stop_loss_short = self.data.close[0] * 1.09
            take_profit_short = self.data.close[0] * 0.973
        else:
            stop_loss_long = self.data.close[0] * 0.92
            take_profit_long = self.data.close[0] * 1.32
            stop_loss_short = self.data.close[0] * 1.12
            take_profit_short = self.data.close[0] * 0.77

        # Definir as condições para entrar em long e short
        if not self.position_open:
            if self.adx > self.params.adx_threshold and self.plus_di > self.minus_di and self.macd.macd > self.macd.signal and self.rsi < 60:
                # Condição de compra (long)
                self.buy()
                print(f"Comprar Long em {self.data.datetime.date(0)} {self.data.datetime.time(0)} com preço {self.data.close[0]}, Stop Loss: {stop_loss_long}, Take Profit: {take_profit_long}")
                self.position_open = True

            elif self.adx > self.params.adx_threshold and self.minus_di > self.plus_di and self.macd.macd < self.macd.signal and self.rsi > 40:
                # Condição de venda (short)
                self.sell()
                print(f"Vender Short em {self.data.datetime.date(0)} {self.data.datetime.time(0)} com preço {self.data.close[0]}, Stop Loss: {stop_loss_short}, Take Profit: {take_profit_short}")
                self.position_open = True

        # Saída de uma posição long
        elif self.position and self.position.size > 0:
            if self.data.low[0] < stop_loss_long:  # Stoploss
                self.close()
                print(f"Sair de Long (Stoploss) em {self.data.datetime.date(0)} {self.data.datetime.time(0)} com preço {self.data.close[0]}")
                self.position_open = False
            elif self.data.high[0] > take_profit_long:  # Takeprofit
                self.close()
                print(f"Sair de Long (Takeprofit) em {self.data.datetime.date(0)} {self.data.datetime.time(0)} com preço {self.data.close[0]}")
                self.position_open = False

        # Saída de uma posição short
        elif self.position and self.position.size < 0:
            if self.data.high[0] > stop_loss_short:  # Stoploss
                self.close()
                print(f"Sair de Short (Stoploss) em {self.data.datetime.date(0)} {self.data.datetime.time(0)} com preço {self.data.close[0]}")
                self.position_open = False
            elif self.data.low[0] < take_profit_short:  # Takeprofit
                self.close()
                print(f"Sair de Short (Takeprofit) em {self.data.datetime.date(0)} {self.data.datetime.time(0)} com preço {self.data.close[0]}")
                self.position_open = False

# Configurar o cerebro para rodar a estratégia
cerebro = bt.Cerebro()
cerebro.adddata(data_feed)
cerebro.addstrategy(MyStrategy)
cerebro.broker.set_cash(1000000)

# Rodar a estratégia
cerebro.run()

# Exibir o saldo final
print(f"Saldo Final: {cerebro.broker.getvalue()}")
