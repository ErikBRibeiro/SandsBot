from collections import defaultdict

import utils
import setups.stopgain as StopGain
import setups.stoploss as StopLoss

class EvaluatedStrategy:
    def __init__(self, identifier, starting_balance, trading_tax,
                 historical_data, short_period, long_period, stop_candles,
                 ratio):
        self.identifier = identifier
        self.balance = starting_balance
        self.trading_tax = trading_tax
        self.stop_candles = stop_candles
        self.ratio = ratio

        self.is_holding = False
        self.monthly_results = defaultdict(lambda: defaultdict(
            lambda: {
                'open_trades': 0,
                'lucro': 0,
                'successful_trades': 0,
                'failed_trades': 0,
                'perda_percentual_total': 0,
                'saldo_inicial': None,
                'saldo_final': starting_balance,
                'max_drawdown': 0
            }
        ))

        self.short_ema_column = f"ema_{short_period}"
        self.long_ema_column = f"ema_{long_period}"

        if self.short_ema_column not in historical_data:
            historical_data[self.short_ema_column] = historical_data['close'].ewm(
                span=short_period, adjust=False
                ).mean()

        if self.long_ema_column not in historical_data:
            historical_data[self.long_ema_column] = historical_data['close'].ewm(
                span=long_period, adjust=False
                ).mean()

        self.max_balance = starting_balance
        self.min_balance_since_max = starting_balance
        self.max_drawdown = 0

        self.trades = []
        self.losses = []
        self.gains = []

        self.current_trade = None
        self.stop_gain = None
        self.stop_loss = None
        self.buy_price = None

    def trade(self, idx, candle, previous_candle, historical_data):
        open_time = candle.open_time
        current_month_results = self.monthly_results[open_time.year][open_time.month]

        if current_month_results['saldo_inicial'] is None:
            current_month_results['saldo_inicial'] = self.balance

        if not self.is_holding:
            # same logic as buy_double_ema_breakout but optimized
            if not candle.high > previous_candle.high:
                return

            if not (previous_candle.close
                    > getattr(previous_candle, self.long_ema_column)):
                return

            if not (previous_candle.close
                    > getattr(previous_candle, self.short_ema_column)):
                return

            current_month_results['open_trades'] += 1
            self.buy_price = previous_candle.high
            self.stop_loss = StopLoss.set_sell_stoploss_min_candles(
                historical_data.iloc[idx - (self.stop_candles + 1):idx],
                self.stop_candles)
            if self.trading_tax != 0:
                self.balance -= self.balance * self.trading_tax / 100
            current_month_results['saldo_final'] = self.balance
            self.stop_gain = StopGain.set_sell_stopgain_ratio(self.buy_price, self.stop_loss, self.ratio)
            self.is_holding = True

            self.current_trade = {
                'open_time': open_time,
                'buy_price': self.buy_price,
                'stoploss': self.stop_loss,
                'stopgain': self.stop_gain,
                'close_price': 0,
                'close_time': 0,
                'outcome': 0,
                'result': ''
            }
            return

        if StopLoss.sell_stoploss(candle.low, self.stop_loss):
            loss_percentage = utils.calculate_loss_percentage(self.buy_price, self.stop_loss)
            current_month_results['failed_trades'] += 1
            current_month_results['perda_percentual_total'] += loss_percentage + self.trading_tax
            self.balance -= self.balance * (loss_percentage + self.trading_tax) / 100
            current_month_results['saldo_final'] = self.balance
            self.is_holding = False

            self.current_trade['close_price'] = self.stop_loss
            self.current_trade['close_time'] = open_time
            self.current_trade['outcome'] = loss_percentage
            self.current_trade['result'] = 'StopLoss'
            self.trades.append(self.current_trade)
            self.losses.append(-(loss_percentage + self.trading_tax))

            if self.balance < self.min_balance_since_max:
                self.min_balance_since_max = self.balance
            drawdown = (self.max_balance - self.min_balance_since_max) / self.max_balance * 100

            if drawdown > self.max_drawdown:
                self.max_drawdown = drawdown

            current_month_results['max_drawdown'] = self.max_drawdown

        elif StopGain.sell_stopgain(candle.high, self.stop_gain):
            profit = utils.calculate_gain_percentage(self.buy_price, self.stop_gain)
            current_month_results['lucro'] += profit - self.trading_tax
            current_month_results['successful_trades'] += 1
            self.balance += self.balance * ((profit - self.trading_tax) / 100)
            current_month_results['saldo_final'] = self.balance
            self.is_holding = False

            self.current_trade['close_price'] = self.stop_gain
            self.current_trade['close_time'] = open_time
            self.current_trade['outcome'] = profit
            self.current_trade['result'] = 'StopGain'
            self.trades.append(self.current_trade)

            self.gains.append(profit - self.trading_tax)

            if self.balance > self.max_balance:
                self.max_balance = self.balance
                self.min_balance_since_max = self.balance

    def current_balance(self):
        return self.balance

    def metrics(self):
        return {
            "balance": self.balance,
            "trades": self.trades,
            "monthly_results": self.monthly_results,
            "max_drawdown": self.max_drawdown,
            "gains": self.gains,
            "losses": self.losses
        }
