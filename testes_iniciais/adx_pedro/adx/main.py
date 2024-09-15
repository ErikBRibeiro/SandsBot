from datetime import datetime
import backtrader as bt


lista = []

class ADXStrategy(bt.Strategy):
    adx_period = 16
    adx_threshold = 12
    adx_smooth = 13

    def __init__(self):
        self.adx = bt.indicators.ADX(self.data, period=self.adx_period)
        self.di_plus = bt.indicators.PlusDI(self.data, period=self.adx_period)
        self.di_minus = bt.indicators.MinusDI(self.data, period=self.adx_period)

    def next(self):
        # Print the ADX, DI+ and DI- values for each candle, including the date and time
        print(f"Date: {self.data.datetime.date(0)}, Time: {self.data.datetime.time(0)}, ADX: {self.adx[0]:.2f}, DI+: {self.di_plus[0]:.2f}, DI-: {self.di_minus[0]:.2f}")

        if self.adx > self.adx_threshold:
            trending = True
            return trending

if __name__ == '__main__':
    cerebro = bt.Cerebro()

    data = bt.feeds.GenericCSVData(
        dataname="testes_iniciais/adx_pedro/adx/bybit.csv",
        fromdate=datetime(2020, 5, 25),
        todate=datetime(2024, 9, 11),
        nullvalue=0.0,
        dtformat=1,
        datetime=0,
        time=-1,
        open=1,
        high=2,
        low=3,
        close=4,
        volume=-1,
        openinterest=-1,
    )

    cerebro.adddata(data)
    cerebro.addstrategy(ADXStrategy)

    cerebro.addanalyzer(bt.analyzers.Transactions, _name='transactions')

    strat = cerebro.run()[0]
    print(f"Transactions Generated: {len(strat.analyzers.transactions.get_analysis())}")
