import talib
from src.parameters import ativo, short_period, long_period, adx_length, adx_smoothing, rsi_length, macd_short, macd_long, macd_signal, bb_length, bb_multiplier, adx_threshold

from src.setups.stopgain import long_stopgain, short_stopgain
from src.setups.stoploss import long_stoploss, short_stoploss

class TradingStrategy:
    def __init__(self, data_interface, metrics, ativo, timeframe, setup):
        self.data_interface = data_interface
        self.metrics = metrics
        self.ativo = ativo
        self.timeframe = timeframe
        self.setup = setup
        self.position_open = False
        self.current_position = None

    def apply_indicators(self, data, bb_length, bb_multiplier):
        # Aplicar indicadores técnicos nos dados
        close_price = data['close']
        high_price = data['high']
        low_price = data['low']
        
        # Cálculo do ADX e Bollinger Bands
        adx = talib.ADX(high_price, low_price, close_price, timeperiod=adx_length)
        upper_band, middle_band, lower_band = talib.BBANDS(close_price, timeperiod=bb_length, nbdevup=bb_multiplier, nbdevdn=bb_multiplier, matype=0)
        print(upper_band)
        print(middle_band)
        print(lower_band)
        data['adx'] = adx
        data['upper_band'] = upper_band
        data['middle_band'] = middle_band        
        data['lower_band'] = lower_band
        return data

    def check_lateral_market(self, data):
        print(data[['upper_band', 'lower_band', 'middle_band']].tail())  # Verifica os últimos valores
        band_width = (data['upper_band'] - data['lower_band']) / data['middle_band']
        print(band_width)
        return band_width < 0.005


    def execute_trade(self, side, entry_price, stop_loss, take_profit, qty):
        self.data_interface.create_order(self.ativo, side, qty, stop_loss, take_profit)
        self.metrics.trade_counter_metric.labels(self.ativo).inc()

    def buy_logic(self, trade_history, current_time):
        data = self.data_interface.get_historical_data(self.ativo, self.timeframe)
        data = self.apply_indicators(data, bb_length, bb_multiplier)
        current_price = self.data_interface.get_current_price(self.ativo)

        if self.check_lateral_market(data):
            stopLossLong = current_price * 0.973
            takeProfitLong = current_price * 1.11
        else:
            stopLossLong = current_price * 0.92
            takeProfitLong = current_price * 1.32

        qty = self.data_interface.get_lot_size(self.ativo, data_interface=self.data_interface)
        self.execute_trade("long", current_price, stopLossLong, takeProfitLong, qty)
        self.current_position = 'long'
        self.position_open = True
        return True, trade_history

    def sell_logic(self, trade_history, current_time):
        data = self.data_interface.get_historical_data(self.ativo, self.timeframe)
        data = self.apply_indicators(data, bb_length, bb_multiplier)
        current_price = self.data_interface.get_current_price(self.ativo)

        if self.check_lateral_market(data):
            stopLossShort = current_price * 1.09
            takeProfitShort = current_price * 0.973
        else:
            stopLossShort = current_price * 1.12
            takeProfitShort = current_price * 0.77

        qty = self.data_interface.get_lot_size(self.ativo, data_interface=self.data_interface)
        self.execute_trade("short", current_price, stopLossShort, takeProfitShort, qty)
        self.current_position = 'short'
        self.position_open = True
        return False, trade_history
