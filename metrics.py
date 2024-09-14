from prometheus_client import CollectorRegistry, Gauge, Counter, Histogram, Summary, start_http_server

registry = CollectorRegistry()

class Metrics:
    def __init__(self, currency):
        self.current_price_metric = Gauge('trade_bot_current_price', 'Current price of the asset', ['currency'], registry=registry)
        self.current_trade_volume_metric = Gauge('trade_bot_current_trade_volume', 'Current volume of the asset traded', ['currency'], registry=registry)
        self.total_trades_count_metric = Gauge('trade_bot_total_trades_count', 'Total count of trades', ['currency'], registry=registry)
        self.current_accumulated_profit_metric = Gauge('trade_bot_current_accumulated_profit', 'Current accumulated profit', ['currency'], registry=registry)
        self.current_accumulated_loss_metric = Gauge('trade_bot_current_accumulated_loss', 'Current accumulated loss', ['currency'], registry=registry)
        self.average_buy_price_metric = Gauge('trade_bot_average_buy_price', 'Average buy price', ['currency'], registry=registry)
        self.average_sell_price_metric = Gauge('trade_bot_average_sell_price', 'Average sell price', ['currency'], registry=registry)
        self.current_success_rate_metric = Gauge('trade_bot_current_success_rate', 'Current success rate of trades', ['currency'], registry=registry)
        self.average_trade_time_metric = Gauge('trade_bot_average_trade_time', 'Average time per trade', ['currency'], registry=registry)
        self.price_standard_deviation_metric = Gauge('trade_bot_price_standard_deviation', 'Standard deviation of closing prices', ['currency'], registry=registry)
        self.current_high_price_metric = Gauge('trade_bot_current_high_price', 'Current high price of the asset', ['currency'], registry=registry)
        self.current_low_price_metric = Gauge('trade_bot_current_low_price', 'Current low price of the asset', ['currency'], registry=registry)
        self.current_volume_metric = Gauge('trade_bot_current_volume', 'Current trading volume', ['currency'], registry=registry)
        self.buy_attempts_metric = Counter('trade_bot_buy_attempts', 'Number of buy attempts', ['currency'], registry=registry)
        self.successful_buys_metric = Counter('trade_bot_successful_buys', 'Number of successful buys', ['currency'], registry=registry)
        self.current_stoploss_metric = Gauge('trade_bot_current_stoploss', 'Current stoploss value', ['currency'], registry=registry)
        self.current_stopgain_metric = Gauge('trade_bot_current_stopgain', 'Current stopgain value', ['currency'], registry=registry)
        self.last_buy_price_metric = Gauge('trade_bot_last_buy_price', 'Last buy price', ['currency'], registry=registry)
        self.buy_duration_metric = Histogram('trade_bot_buy_duration_seconds', 'Duration of buy transactions in seconds', ['currency'], registry=registry)
        self.buy_price_spread_metric = Gauge('trade_bot_buy_price_spread', 'Spread between buy prices', ['currency'], registry=registry)
        self.potential_loss_metric = Gauge('trade_bot_potential_loss', 'Potential loss of current buy', ['currency'], registry=registry)
        self.potential_gain_metric = Gauge('trade_bot_potential_gain', 'Potential gain of current buy', ['currency'], registry=registry)
        self.successful_sells_metric = Counter('trade_bot_successful_sells', 'Number of successful sells', ['currency'], registry=registry)
        self.last_sell_price_metric = Gauge('trade_bot_last_sell_price', 'Last sell price', ['currency'], registry=registry)
        self.sell_duration_metric = Histogram('trade_bot_sell_duration_seconds', 'Duration of sell transactions in seconds', ['currency'], registry=registry)
        self.transaction_outcome_metric = Summary('trade_bot_transaction_outcome', 'Transaction outcomes (gain/loss)', ['currency'], registry=registry)
        self.sell_price_spread_metric = Gauge('trade_bot_sell_price_spread', 'Spread between sell prices', ['currency'], registry=registry)
        self.total_trades_metric = Counter('trade_bot_total_trades', 'Total number of trades', ['currency'], registry=registry)
        self.total_loss_metric = Gauge('trade_bot_total_loss', 'Total loss accumulated', ['currency'], registry=registry)
        self.trade_volume_metric = Gauge('trade_bot_trade_volume', 'Total volume of trades', ['currency'], registry=registry)
        self.success_rate_metric = Gauge('trade_bot_success_rate', 'Success rate of trades', ['currency'], registry=registry)
        self.total_profit_metric = Gauge('trade_bot_total_profit', 'Total profit accumulated', ['currency'], registry=registry)
        self.average_trade_duration_metric = Gauge('trade_bot_average_trade_duration', 'Average duration of trades', ['currency'], registry=registry)
        self.profit_factor_metric = Gauge('trade_bot_profit_factor', 'Profit factor (total profit / total loss)', ['currency'], registry=registry)
        self.server_status_metric = Gauge('trade_bot_server_status', 'Status of the trade bot server (1 = up, 0 = down)', registry=registry)
        self.server_down_metric_duration = Histogram('trade_bot_down_duration', 'Offline duration', registry=registry)
        self.loop_counter_metric = Counter('trade_bot_loop_executions', 'Number of loop executions', ['currency'], registry=registry)

        self.total_trade_duration = 0
        self.buy_prices = []
        self.sell_prices = []

    def update_metrics_on_sell(self, ticker, symbol):
        self.last_sell_price_metric.labels(symbol).set(ticker)
        self.successful_sells_metric.labels(symbol).inc()
        self.sell_price_spread_metric.labels(symbol).set(max(self.sell_prices) - min(self.sell_prices) if self.sell_prices else 0)

    def update_metrics_on_buy(self, symbol, current_price, stoploss, stopgain, potential_loss, potential_gain):
        self.current_stoploss_metric.labels(symbol).set(stoploss)
        self.current_stopgain_metric.labels(symbol).set(stopgain)
        self.last_buy_price_metric.labels(symbol).set(current_price)
        self.buy_attempts_metric.labels(symbol).inc()
        self.successful_buys_metric.labels(symbol).inc()
        self.buy_price_spread_metric.labels(symbol).set(max(self.buy_prices) - min(self.buy_prices) if self.buy_prices else 0)
        self.potential_loss_metric.labels(symbol).set(potential_loss)
        self.potential_gain_metric.labels(symbol).set(potential_gain)

def start_prometheus_server(port=8000):
    start_http_server(port)
