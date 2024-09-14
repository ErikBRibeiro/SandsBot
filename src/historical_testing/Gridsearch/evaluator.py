class StrategyEvaluator:
    def __init__(self, historical_data, strategies, performance_function, start_cursor):
        self.historical_data = historical_data

        # list of classes that operate on sequential candles and historical data
        # as well as providing methods to report their trading metrics
        self.strategies = strategies

        # function that receives a strategy with a history of trades, and
        # returns an integer representing its performance in comparison to
        # other strategies
        self.performance_function = performance_function

        self.candles = historical_data.itertuples()
        self.previous_candle = None
        self.start_cursor = start_cursor
        self.cursor = 0

    def is_done_evaluating(self):
        return (self.cursor >= len(self.historical_data) - 1
                or len(self.strategies) == 0)

    def current_progress(self):
        return self.cursor / len(self.historical_data)

    def performance_of(self, strategy):
        return self.performance_function(strategy)

    def evaluate_next_candle(self):
        if self.is_done_evaluating():
            return
        self.cursor += 1
        candle = next(self.candles)

        if self.start_cursor > self.cursor:
            self.previous_candle = candle
            return

        for strategy in self.strategies:
            strategy.trade(self.cursor, candle, self.previous_candle, self.historical_data)
        self.previous_candle = candle

    def results(self):
        best_strategy = None

        for strategy in self.strategies:
            if (best_strategy is None
                or self.performance_of(strategy) >
                   self.performance_of(best_strategy)):
                best_strategy = strategy

        if best_strategy is None:
            return None

        best_strategy_summary = {
            "identifier": best_strategy.identifier,
            "performance": self.performance_of(best_strategy),
            "metrics": best_strategy.metrics()
        }

        return best_strategy_summary
