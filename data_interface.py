import time
import pandas as pd
from pybit.unified_trading import HTTP
from requests.exceptions import ConnectionError, Timeout
from src.utils import logger, safe_float_conversion

class LiveData:
    def __init__(self, api_key, api_secret, futures=False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.futures = futures
        if self.futures:
            self.client = HTTP(api_key=api_key, api_secret=api_secret)
        else:
            self.client = HTTP(api_key=api_key, api_secret=api_secret)
        self.current_price = None

    def check_rate_limit(self, headers):
        limit_status = int(headers.get("X-Bapi-Limit-Status", -1))
        limit_reset_timestamp = int(headers.get("X-Bapi-Limit-Reset-Timestamp", time.time()))

        if limit_status <= 2:
            sleep_time = max(0, limit_reset_timestamp - time.time())
            logger.warning(f"Rate limit exceeded. Sleeping for {sleep_time} seconds.")
            time.sleep(sleep_time + 1)

    def get_historical_data(self, symbol, interval, limit=150):
        try:
            response = self.client.get_kline(symbol=symbol, interval=interval, limit=limit, category='linear')
            
            if response is None or 'result' not in response:
                logger.error("Resposta inválida ao obter dados históricos.")
                return None

            data = pd.DataFrame(response['result'], columns=['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time'])

            data['close'] = data['close'].apply(safe_float_conversion)
            data['low'] = data['low'].apply(safe_float_conversion)
            data['high'] = data['high'].apply(safe_float_conversion)
            data['volume'] = data['volume'].apply(safe_float_conversion)

            return data

        except Exception as e:
            logger.error(f"Erro inesperado ao obter dados históricos: {e}")
            return None

    def get_current_price(self, symbol):
        try:
            response = self.client.get_tickers(symbol=symbol, category='linear')

            if response is None or 'result' not in response or len(response['result']['list']) == 0:
                logger.error(f"Erro ao obter preço: resposta inválida ou vazia.")
                return 0  # Retorna 0 em caso de erro

            if 'lastPrice' not in response['result']['list'][0]:
                logger.error(f"Erro ao obter preço: campo 'lastPrice' não encontrado.")
                return 0  # Retorna 0 se o campo não for encontrado

            ticker = float(response['result']['list'][0]['lastPrice'])
            return ticker
        except Exception as e:
            logger.error(f"Erro inesperado ao obter preço atual: {e}")
            return 0

    def get_current_balance(self, asset):
        try:
            response = self.client.get_wallet_balance(accountType="UNIFIED", coin=asset)

            if response is None or 'result' not in response:
                logger.error(f"Erro ao obter saldo: resposta inválida ou vazia.")
                return 0.0

            return float(response['result']['list'][0]['totalEquity'])
        except Exception as e:
            logger.error(f"Erro inesperado ao obter saldo: {e}")
            return 0.0

    def create_order(self, symbol, side, quantity, stop_loss=None, take_profit=None):
        try:
            if self.futures:
                response = self.client.place_order(
                    category='linear', 
                    symbol=symbol, 
                    isLeverage=1, 
                    side=side.capitalize(), 
                    orderType="Market", 
                    qty=quantity, 
                    stopLossPrice=stop_loss, 
                    takeProfitPrice=take_profit
                )
            else:
                response = self.client.place_order(
                    category='linear', 
                    symbol=symbol, 
                    isLeverage=1, 
                    side=side.capitalize(), 
                    orderType="Market", 
                    qty=quantity, 
                    stopLossPrice=stop_loss, 
                    takeProfitPrice=take_profit
                )
            
            if response is None or 'result' not in response:
                logger.error(f"Erro ao criar ordem: resposta inválida ou vazia.")
                return None

            return response
        except Exception as e:
            logger.error(f"Erro inesperado ao criar ordem: {e}")
            return None

    def close_order(self, symbol):
        try:
            response = self.client.place_order(category='linear', symbol=symbol, isLeverage=1, side='Sell', orderType="Market", qty=0, reduceOnly=True, closeOnTrigger=True)

            if response is None or 'result' not in response:
                logger.error(f"Erro ao fechar ordem: resposta inválida ou vazia.")
                return None

            return response
        except Exception as e:
            logger.error(f"Erro inesperado ao fechar ordem: {e}")
            return None

    def update_price_continuously(self, symbol, frequency_per_second=1):
        interval = 1 / frequency_per_second
        while True:
            try:
                self.current_price = self.get_current_price(symbol)
            except Exception as e:
                logger.error(f"Erro ao atualizar o preço continuamente: {e}")
            time.sleep(interval)
