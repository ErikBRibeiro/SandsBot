from datetime import datetime
import backtrader as bt

class ADXStrategy(bt.Strategy):
    adx_period = 14
    adx_threshold = 20
    adx_threshold_sell = 20
    adx_smooth = 13

    def __init__(self):
        self.adx = bt.indicators.ADX(self.data, period=self.adx_period)
        self.di_plus = bt.indicators.PlusDI(self.data, period=self.adx_period)
        self.di_minus = bt.indicators.MinusDI(self.data, period=self.adx_period)

    def next(self):
        if self.adx > self.adx_threshold and self.di_plus > self.di_minus:
            self.buy()
        elif self.adx > self.adx_threshold_sell and self.di_minus > self.di_plus:
            self.sell()

if __name__ == '__main__':
    cerebro = bt.Cerebro()

    data = bt.feeds.GenericCSVData(
        dataname="testes_iniciais/adx_pedro/adx/bybit.csv",
        fromdate=datetime(2024, 1, 1),
        todate=datetime(2024, 9, 13),
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
    cerebro.broker.setcash(1000000)
    cerebro.addanalyzer(bt.analyzers.Transactions, _name='transactions')

    strat = cerebro.run()[0]
    print(f"Transactions Generated: {len(strat.analyzers.transactions.get_analysis())}")
