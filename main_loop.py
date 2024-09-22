import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import talib
import os
import sys
import time
from dotenv import load_dotenv, find_dotenv
from pybit.unified_trading import HTTP
import logging
import traceback
import requests
from requests.exceptions import RequestException, HTTPError, ConnectionError, Timeout
import math

# Load API key and secret from .env file
load_dotenv(find_dotenv())
API_KEY = os.getenv('BYBIT_API_KEY')
API_SECRET = os.getenv('BYBIT_API_SECRET')

if not API_KEY or not API_SECRET:
    logging.error("API_KEY and/or API_SECRET not found. Please check your .env file.")
    sys.exit(1)

# Initialize Bybit client using the HTTP class from unified_trading
session = HTTP(
    testnet=False,  # Set to True if you want to use the testnet
    api_key=API_KEY,
    api_secret=API_SECRET
)

symbol = 'BTCUSDT'

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bot_log.log"),
        logging.StreamHandler()
    ]
)

# Check if trade_history.csv exists, if not create it with headers
trade_history_file = '/app/data/trade_history.csv'
if not os.path.isfile(trade_history_file):
    columns = ['trade_id', 'timestamp', 'symbol', 'buy_price', 'sell_price', 'quantity',
               'stop_loss', 'stop_gain', 'potential_loss', 'potential_gain', 'timeframe',
               'setup', 'outcome', 'commission', 'old_balance', 'new_balance',
               'secondary_stop_loss', 'secondary_stop_gain', 'sell_time']
    df_trade_history = pd.DataFrame(columns=columns)
    df_trade_history.to_csv(trade_history_file, index=False)

# Function to fetch historical kline data
def get_historical_klines(symbol, interval, limit):
    try:
        now = datetime.utcnow()
        from_time = now - timedelta(minutes=int(interval) * limit)
        from_time_ms = int(from_time.timestamp() * 1000)
        kline = session.get_kline(
            category='linear',
            symbol=symbol,
            interval=str(interval),
            start=str(from_time_ms),
            limit=limit
        )
        if kline['retMsg'] != 'OK':
            logging.error(f"Error fetching kline data: {kline['retMsg']}")
            return None
        kline_data = kline['result']['list']
        df = pd.DataFrame(kline_data, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'
        ])
        # Ensure 'timestamp' is numeric before converting to datetime
        df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)
        df['turnover'] = df['turnover'].astype(float)
        return df
    except Exception as e:
        logging.error(f"Exception in get_historical_klines: {e}")
        return None

# Function to calculate indicators
def calculate_indicators(df):
    try:
        close_price = df['close'].values
        high_price = df['high'].values
        low_price = df['low'].values

        # Parameters
        emaShortLength = 11
        emaLongLength = 55
        rsiLength = 22
        macdShort = 15
        macdLong = 34
        macdSignal = 11
        adxLength = 16
        adxSmoothing = 13
        adxThreshold = 12
        bbLength = 14
        bbMultiplier = 1.7
        lateralThreshold = 0.005

        # Calculating indicators using TA-Lib
        emaShort = talib.EMA(close_price, emaShortLength)
        emaLong = talib.EMA(close_price, emaLongLength)
        rsi = talib.RSI(close_price, timeperiod=rsiLength)
        macdLine, signalLine, macdHist = talib.MACD(
            close_price, fastperiod=macdShort, slowperiod=macdLong, signalperiod=macdSignal
        )
        upperBand, middleBand, lowerBand = talib.BBANDS(
            close_price, timeperiod=bbLength, nbdevup=bbMultiplier, nbdevdn=bbMultiplier
        )
        adx = talib.ADX(high_price, low_price, close_price, timeperiod=adxSmoothing)
        bandWidth = (upperBand - lowerBand) / middleBand
        isLateral = bandWidth < lateralThreshold

        df['emaShort'] = emaShort
        df['emaLong'] = emaLong
        df['rsi'] = rsi
        df['macdHist'] = macdHist
        df['adx'] = adx
        df['upperBand'] = upperBand
        df['middleBand'] = middleBand
        df['lowerBand'] = lowerBand
        df['bandWidth'] = bandWidth
        df['isLateral'] = isLateral

        return df
    except Exception as e:
        logging.error(f"Exception in calculate_indicators: {e}")
        return None

# Helper functions for crossover logic
def crossover(series1, series2):
    try:
        cross = (series1 > series2) & (series1.shift(1) <= series2.shift(1))
        return cross
    except Exception as e:
        logging.error(f"Exception in crossover function: {e}")
        return pd.Series([False])

def crossunder(series1, series2):
    try:
        cross = (series1 < series2) & (series1.shift(1) >= series2.shift(1))
        return cross
    except Exception as e:
        logging.error(f"Exception in crossunder function: {e}")
        return pd.Series([False])

# Função para obter a posição atual
def get_current_position(retries=3, backoff_factor=5):
    attempt = 0
    while attempt < retries:
        try:
            # Utilizando o método correto para obter posições
            positions = session.get_positions(
                category='linear',  # linear para contratos perpétuos
                symbol=symbol       # par de símbolos como BTCUSDT
            )
            
            # Verificar se a resposta foi bem-sucedida
            if positions['retMsg'] != 'OK':
                return None, None

            positions_data = positions['result']['list']

            # Verificar se há uma posição aberta
            for pos in positions_data:
                size = float(pos['size'])
                if size != 0:
                    side = (pos['side']).lower()
                    entry_price = float(pos['avgPrice'])
                    return side, {'entry_price': entry_price, 'size': size, 'side': side}
                    
            # Se não houver posição aberta, retornar False
            return False, None
        
        except Exception as e:
            logging.error(f"Erro inesperado no get_current_position: {e}")
            logging.error(traceback.format_exc())
            attempt += 1
            time.sleep(backoff_factor * attempt)

    logging.error("Falha ao obter posição atual após várias tentativas.")
    return False, None



# Function to fetch the latest price
def get_latest_price():
    try:
        ticker = session.get_tickers(
            category='linear',
            symbol=symbol
        )
        if ticker['retMsg'] != 'OK':
            logging.error(f"Error fetching latest price: {ticker['retMsg']}")
            return None
        price = float(ticker['result']['list'][0]['lastPrice'])
        return price
    except Exception as e:
        logging.error(f"Exception in get_latest_price: {e}")
        return None

# Function to get account balance
def get_account_balance():
    try:
        balance_info = session.get_wallet_balance(accountType='UNIFIED')
        if balance_info['retMsg'] != 'OK':
            logging.error(f"Error fetching account balance: {balance_info['retMsg']}")
            return None
        
        # Acessando o totalEquity corretamente na lista dentro de result
        total_equity = float(balance_info['result']['list'][0]['totalEquity'])
        return total_equity
    except Exception as e:
        logging.error(f"Exception in get_account_balance: {e}")
        return None

# Functions to log trade entries and exits
def log_trade_entry(trade_data):
    try:
        if os.path.isfile(trade_history_file):
            df_trade_history = pd.read_csv(trade_history_file)
        else:
            columns = ['trade_id', 'timestamp', 'symbol', 'buy_price', 'sell_price', 'quantity',
                       'stop_loss', 'stop_gain', 'potential_loss', 'potential_gain', 'timeframe',
                       'setup', 'outcome', 'commission', 'old_balance', 'new_balance',
                       'secondary_stop_loss', 'secondary_stop_gain', 'sell_time', 'type', 'entry_lateral', 'exit_lateral']
            df_trade_history = pd.DataFrame(columns=columns)
        df_trade_history = df_trade_history.append(trade_data, ignore_index=True)
        df_trade_history.to_csv(trade_history_file, index=False)
    except Exception as e:
        logging.error(f"Exception in log_trade_entry: {e}")

def log_trade_update(trade_id, symbol, update_data):
    try:
        if not os.path.isfile(trade_history_file):
            logging.error("Trade history file does not exist.")
            return
        df_trade_history = pd.read_csv(trade_history_file)
        mask = (df_trade_history['trade_id'] == trade_id) & (df_trade_history['symbol'] == symbol)
        if mask.any():
            for key, value in update_data.items():
                df_trade_history.loc[mask, key] = value
            df_trade_history.to_csv(trade_history_file, index=False)
        else:
            logging.error("Trade not found in trade history for update.")
    except Exception as e:
        logging.error(f"Exception in log_trade_update: {e}")

def log_trade_exit(trade_id, symbol, update_data, exit_lateral):
    try:
        if not os.path.isfile(trade_history_file):
            logging.error("Trade history file does not exist.")
            return
        df_trade_history = pd.read_csv(trade_history_file)
        mask = (df_trade_history['trade_id'] == trade_id) & (df_trade_history['symbol'] == symbol)
        if mask.any():
            update_data['exit_lateral'] = exit_lateral  # Adiciona 'exit_lateral'
            df_trade_history.loc[mask, update_data.keys()] = update_data.values()
            df_trade_history.to_csv(trade_history_file, index=False)
        else:
            logging.error("Trade not found in trade history for update.")
    except Exception as e:
        logging.error(f"Exception in log_trade_exit: {e}")

def calculate_qty(total_equity, latest_price, leverage=1):
    try:
        # Calcular o número de contratos
        qty = total_equity / latest_price
        factor = 100
        # Ajustar para a precisão permitida pela Bybit (exemplo: 2 casas decimais)
        qty = math.floor(qty * factor) / factor
        
        return qty
    except Exception as e:
        logging.error(f"Exception in calculate_qty: {e}")
        return None
    
out_of_trade_logged = False
in_trade_logged = False

# Função para logar o status de entrada na posição
def log_entry(side, entry_price, size, stop_gain, stop_loss):
    logging.info(f"Entrou em posição {side} com tamanho {size} BTC a {entry_price}.")
    logging.info(f"Stopgain definido em {stop_gain}, Stoploss definido em {stop_loss}.")

# Função para logar o status de saída da posição
def log_exit(side, exit_price, size, outcome):
    logging.info(f"Saindo da posição {side} com tamanho {size} BTC a {exit_price}.")
    logging.info(f"Resultado da posição: {outcome}")

# Trading parameters
stopgain_lateral_long = 1.11
stoploss_lateral_long = 0.973
stopgain_lateral_short = 0.973
stoploss_lateral_short = 1.09
stopgain_normal_long = 1.32
stoploss_normal_long = 0.92
stopgain_normal_short = 0.77
stoploss_normal_short = 1.12

trade_count = 0  # Counter for the number of trades

# Initialize variables
last_candle_time = None  # To keep track of when to update indicators
last_log_time = None     # To keep track of logging every 10 minutes
previous_isLateral = None  # To detect transition into lateral market

# Variables to track current trade
current_trade_id = None
current_position_side = None
entry_price = None
current_secondary_stop_loss = None
current_secondary_stop_gain = None
previous_commission = 0  # To store commission from entry

# Main trading loop
while True:
    try:
        current_time = datetime.utcnow()
        # Check if it's time to update the indicators (every hour)
        if last_candle_time is None or (current_time - last_candle_time).seconds >= 3600:
            # Fetch the latest 1-hour kline data
            df = get_historical_klines(symbol, interval=60, limit=200)
            if df is None or df.empty:
                logging.error("Failed to fetch historical klines or received empty data.")
                time.sleep(10)
                continue
            df = df.sort_values('timestamp')

            # Calculate indicators
            df = calculate_indicators(df)
            if df is None:
                logging.error("Failed to calculate indicators.")
                time.sleep(10)
                continue

            # Get the latest data point
            last_row = df.iloc[-1]
            adjusted_timestamp = last_row['timestamp']
            emaShort = df['emaShort']
            emaLong = df['emaLong']
            rsi = df['rsi']
            macdHist = df['macdHist']
            adx = df['adx']
            isLateral = df['isLateral']
            upperBand = df['upperBand']
            lowerBand = df['lowerBand']
            bandWidth = df['bandWidth']

            # Determine trending market
            trendingMarket = adx.iloc[-1] >= 12  # adxThreshold

            # Detect transition into lateral market
            if previous_isLateral is not None and isLateral.iloc[-1] != previous_isLateral:
                if isLateral.iloc[-1]:
                    # Entered lateral market
                    logging.info("Entered lateral market. Stopgain and Stoploss levels adjusted.")
                    logging.info(f"Stopgain and Stoploss levels for lateral market - Long: Stopgain {stopgain_lateral_long}, Stoploss {stoploss_lateral_long}; Short: Stopgain {stopgain_lateral_short}, Stoploss {stoploss_lateral_short}")
                else:
                    # Exited lateral market
                    logging.info("Exited lateral market. Stopgain and Stoploss levels adjusted.")
                    logging.info(f"Stopgain and Stoploss levels for trending market - Long: Stopgain {stopgain_normal_long}, Stoploss {stoploss_normal_long}; Short: Stopgain {stopgain_normal_short}, Stoploss {stoploss_normal_short}")
                # Update secondary stop_loss and stop_gain if there is an open position
                if current_trade_id is not None:
                    if isLateral.iloc[-1]:
                        # Now in lateral market
                        if current_position_side == 'buy':
                            current_secondary_stop_loss = entry_price * stoploss_lateral_long
                            current_secondary_stop_gain = entry_price * stopgain_lateral_long
                        elif current_position_side == 'sell':
                            current_secondary_stop_loss = entry_price * stoploss_lateral_short
                            current_secondary_stop_gain = entry_price * stopgain_lateral_short
                    else:
                        # Now in trending market
                        if current_position_side == 'buy':
                            current_secondary_stop_loss = entry_price * stoploss_normal_long
                            current_secondary_stop_gain = entry_price * stopgain_normal_long
                        elif current_position_side == 'sell':
                            current_secondary_stop_loss = entry_price * stoploss_normal_short
                            current_secondary_stop_gain = entry_price * stopgain_normal_short
                    # Update the CSV
                    update_data = {
                        'secondary_stop_loss': current_secondary_stop_loss,
                        'secondary_stop_gain': current_secondary_stop_gain
                    }
                    log_trade_update(current_trade_id, symbol, update_data)

            previous_isLateral = isLateral.iloc[-1]

            # Update last_candle_time
            last_candle_time = current_time

            logging.info(f"Indicators updated at {current_time}")

        # Log bot status every 10 minutes
        if last_log_time is None or (current_time - last_log_time).total_seconds() >= 600:
            current_position, position_info = get_current_position()
            if current_position is None:
                logging.info("Bot status: Unable to fetch current position.")
            elif not current_position:
                logging.info("Bot status: Out of trade.")
            else:
                side = position_info['side']
                entry_price = position_info['entry_price']
                if isLateral.iloc[-1]:
                    if side == 'buy':
                        stop_loss = entry_price * stoploss_lateral_long
                        take_profit = entry_price * stopgain_lateral_long
                    else:
                        stop_loss = entry_price * stoploss_lateral_short
                        take_profit = entry_price * stopgain_lateral_short
                else:
                    if side == 'buy':
                        stop_loss = entry_price * stoploss_normal_long
                        take_profit = entry_price * stopgain_normal_long
                    else:
                        stop_loss = entry_price * stoploss_normal_short
                        take_profit = entry_price * stopgain_normal_short
                logging.info(f"Bot status: In a {side.lower()} position.")
                logging.info(f"Current Stoploss: {stop_loss:.2f}, Take Profit: {take_profit:.2f}")
                pd.set_option('display.max_columns', None)
                logging.info(f'DF INDICADORES /n{df}')
            last_log_time = current_time

        # Fetch the latest price every second
        latest_price = get_latest_price()
        if latest_price is None:
            logging.error("Failed to fetch latest price.")
            time.sleep(5)
            continue

        # Implement real-time entry and exit logic based on latest_price and indicators
        # Long and Short conditions
        longCondition = (
            crossover(emaShort, emaLong).iloc[-1]
            and (rsi.iloc[-1] < 60)
            and (macdHist.iloc[-1] > 0.5)
            and trendingMarket
        )
        shortCondition = (
            crossunder(emaShort, emaLong).iloc[-1]
            and (rsi.iloc[-1] > 40)
            and (macdHist.iloc[-1] < -0.5)
            and trendingMarket
        )

        # Get current position
        current_position, position_info = get_current_position()
        if current_position is None:
            logging.info("Failed to fetch current position.")
            time.sleep(5)
            continue

        # Implement trading logic
        if not current_position:

            if isLateral.iloc[-1]:
                # Mean Reversion Strategy in Lateral Market
                if (latest_price < lowerBand.iloc[-1]) and longCondition:
                    # Open long position
                    total_equity = get_account_balance()
                    qty = calculate_qty(total_equity, latest_price)  # Define your position size
                    
                    try:
                        order = session.place_order(
                            category='linear',
                            symbol=symbol,
                            side='Buy',
                            orderType='Market',
                            qty=str(0.01),
                            timeInForce='GTC',
                            reduceOnly=False,
                            closeOnTrigger=False
                        )
                        if order['retMsg'] != 'OK':
                            logging.error(f"Error placing buy order: {order['retMsg']}")
                            time.sleep(5)
                            continue
                    except Exception as e:
                        logging.error(f"Exception placing buy order: {e}")
                        time.sleep(5)
                        continue
                    
                    trade_id = datetime.utcnow().isoformat()
                    current_trade_id = trade_id
                    current_position_side = 'buy'
                    old_balance = get_account_balance()
                    if old_balance is None:
                        logging.error("Failed to fetch account balance.")
                        old_balance = 0  # Set to zero to avoid calculation errors
                    entry_price = latest_price
                    stop_loss = entry_price * stoploss_lateral_long
                    stop_gain = entry_price * stopgain_lateral_long
                    secondary_stop_loss = stop_loss
                    secondary_stop_gain = stop_gain
                    current_secondary_stop_loss = secondary_stop_loss
                    current_secondary_stop_gain = secondary_stop_gain
                    commission_rate = 0.0006  # 0.03%
                    commission = entry_price * qty * commission_rate
                    previous_commission = commission
                    potential_loss = ((entry_price - stop_loss) * qty) / old_balance * 100 if old_balance > 0 else 0
                    potential_gain = ((stop_gain - entry_price) * qty) / old_balance * 100 if old_balance > 0 else 0
                    trade_data = {
                        'trade_id': trade_id,
                        'timestamp': trade_id,
                        'symbol': symbol,
                        'buy_price': entry_price,
                        'sell_price': '',
                        'quantity': qty,
                        'stop_loss': stop_loss,
                        'stop_gain': stop_gain,
                        'potential_loss': potential_loss,
                        'potential_gain': potential_gain,
                        'commission': commission,
                        'old_balance': old_balance,
                        'new_balance': '',
                        'timeframe': '1h',
                        'setup': 'GPTAN',
                        'outcome': '',
                        'secondary_stop_loss': secondary_stop_loss,
                        'secondary_stop_gain': secondary_stop_gain,
                        'sell_time': '',
                        'type': 'long',  # Define como 'long'
                        'entry_lateral': 1 if isLateral.iloc[-1] else 0,  # 1 se lateral, senão 0
                        'exit_lateral': ''  # Inicialmente vazio
                    }

                    log_trade_entry(trade_data)
                    logging.info(f"Entered long position at {trade_id}, price: {entry_price}")
                    logging.info(f"Stoploss set at {stop_loss:.2f}, Take Profit set at {stop_gain:.2f}")
                    trade_count += 1
                elif (latest_price > upperBand.iloc[-1]) and shortCondition:
                    # Open short position
                    total_equity = get_account_balance()
                    qty = calculate_qty(total_equity, latest_price) # Define your position size
                    
                    try:
                        order = session.place_order(
                            category='linear',
                            symbol=symbol,
                            side='Sell',
                            orderType='Market',
                            qty=str(qty),
                            timeInForce='GTC',
                            reduceOnly=False,
                            closeOnTrigger=False
                        )
                        if order['retMsg'] != 'OK':
                            logging.error(f"Error placing sell order: {order['retMsg']}")
                            time.sleep(5)
                            continue
                    except Exception as e:
                        logging.error(f"Exception placing sell order: {e}")
                        time.sleep(5)
                        continue
                    
                    trade_id = datetime.utcnow().isoformat()
                    current_trade_id = trade_id
                    current_position_side = 'sell'
                    old_balance = get_account_balance()
                    if old_balance is None:
                        logging.error("Failed to fetch account balance.")
                        old_balance = 0  # Set to zero to avoid calculation errors
                    entry_price = latest_price
                    stop_loss = entry_price * stoploss_lateral_short
                    stop_gain = entry_price * stopgain_lateral_short
                    secondary_stop_loss = stop_loss
                    secondary_stop_gain = stop_gain
                    current_secondary_stop_loss = secondary_stop_loss
                    current_secondary_stop_gain = secondary_stop_gain
                    commission_rate = 0.0006  # 0.03%
                    commission = entry_price * qty * commission_rate
                    previous_commission = commission
                    potential_loss = ((stop_loss - entry_price) * qty) / old_balance * 100 if old_balance > 0 else 0
                    potential_gain = ((entry_price - stop_gain) * qty) / old_balance * 100 if old_balance > 0 else 0
                    trade_data = {
                        'trade_id': trade_id,
                        'timestamp': trade_id,
                        'symbol': symbol,
                        'buy_price': entry_price,
                        'sell_price': '',
                        'quantity': qty,
                        'stop_loss': stop_loss,
                        'stop_gain': stop_gain,
                        'potential_loss': potential_loss,
                        'potential_gain': potential_gain,
                        'commission': commission,
                        'old_balance': old_balance,
                        'new_balance': '',
                        'timeframe': '1h',
                        'setup': 'GPTAN',
                        'outcome': '',
                        'secondary_stop_loss': secondary_stop_loss,
                        'secondary_stop_gain': secondary_stop_gain,
                        'sell_time': '',
                        'type': 'short',  # Define como 'short'
                        'entry_lateral': 1 if isLateral.iloc[-1] else 0,  # 1 se lateral, senão 0
                        'exit_lateral': ''  # Inicialmente vazio
                    }

            else:
                # Trend Following Strategy in Trending Market
                if longCondition:
                    # Open long position
                    total_equity = get_account_balance()
                    qty = calculate_qty(total_equity, latest_price)  # Define your position size
                    
                    try:
                        order = session.place_order(
                            category='linear',
                            symbol=symbol,
                            side='Buy',
                            orderType='Market',
                            qty=str(qty),
                            timeInForce='GTC',
                            reduceOnly=False,
                            closeOnTrigger=False
                        )
                        if order['retMsg'] != 'OK':
                            logging.error(f"Error placing buy order: {order['retMsg']}")
                            time.sleep(5)
                            continue
                    except Exception as e:
                        logging.error(f"Exception placing buy order: {e}")
                        time.sleep(5)
                        continue
                    
                    trade_id = datetime.utcnow().isoformat()
                    current_trade_id = trade_id
                    current_position_side = 'buy'
                    old_balance = get_account_balance()
                    if old_balance is None:
                        logging.error("Failed to fetch account balance.")
                        old_balance = 0  # Set to zero to avoid calculation errors
                    entry_price = latest_price
                    stop_loss = entry_price * stoploss_normal_long
                    stop_gain = entry_price * stopgain_normal_long
                    secondary_stop_loss = stop_loss
                    secondary_stop_gain = stop_gain
                    current_secondary_stop_loss = secondary_stop_loss
                    current_secondary_stop_gain = secondary_stop_gain
                    commission_rate = 0.0003  # 0.03%
                    commission = entry_price * qty * commission_rate
                    previous_commission = commission
                    potential_loss = ((entry_price - stop_loss) * qty) / old_balance * 100 if old_balance > 0 else 0
                    potential_gain = ((stop_gain - entry_price) * qty) / old_balance * 100 if old_balance > 0 else 0
                    trade_data = {
                        'trade_id': trade_id,
                        'timestamp': trade_id,
                        'symbol': symbol,
                        'buy_price': entry_price,
                        'sell_price': '',
                        'quantity': qty,
                        'stop_loss': stop_loss,
                        'stop_gain': stop_gain,
                        'potential_loss': potential_loss,
                        'potential_gain': potential_gain,
                        'commission': commission,
                        'old_balance': old_balance,
                        'new_balance': '',
                        'timeframe': '1h',
                        'setup': 'GPTAN',
                        'outcome': '',
                        'secondary_stop_loss': secondary_stop_loss,
                        'secondary_stop_gain': secondary_stop_gain,
                        'sell_time': '',
                        'type': 'long',  # Define como 'long'
                        'entry_lateral': 1 if isLateral.iloc[-1] else 0,  # 1 se lateral, senão 0
                        'exit_lateral': ''  # Inicialmente vazio
                    }
                    log_trade_entry(trade_data)
                    logging.info(f"Entered long position at {trade_id}, price: {entry_price}")
                    logging.info(f"Stoploss set at {stop_loss:.2f}, Take Profit set at {stop_gain:.2f}")
                    trade_count += 1
                elif shortCondition:
                    # Open short position
                    total_equity = get_account_balance()
                    qty = calculate_qty(total_equity, latest_price)  # Define your position size
                    
                    try:
                        order = session.place_order(
                            category='linear',
                            symbol=symbol,
                            side='Sell',
                            orderType='Market',
                            qty=str(qty),
                            timeInForce='GTC',
                            reduceOnly=False,
                            closeOnTrigger=False
                        )
                        if order['retMsg'] != 'OK':
                            logging.error(f"Error placing sell order: {order['retMsg']}")
                            time.sleep(5)
                            continue
                    except Exception as e:
                        logging.error(f"Exception placing sell order: {e}")
                        time.sleep(5)
                        continue
                    
                    trade_id = datetime.utcnow().isoformat()
                    current_trade_id = trade_id
                    current_position_side = 'sell'
                    old_balance = get_account_balance()
                    if old_balance is None:
                        logging.error("Failed to fetch account balance.")
                        old_balance = 0  # Set to zero to avoid calculation errors
                    entry_price = latest_price
                    stop_loss = entry_price * stoploss_normal_short
                    stop_gain = entry_price * stopgain_normal_short
                    secondary_stop_loss = stop_loss
                    secondary_stop_gain = stop_gain
                    current_secondary_stop_loss = secondary_stop_loss
                    current_secondary_stop_gain = secondary_stop_gain
                    commission_rate = 0.0003  # 0.03%
                    commission = entry_price * qty * commission_rate
                    previous_commission = commission
                    potential_loss = ((stop_loss - entry_price) * qty) / old_balance * 100 if old_balance > 0 else 0
                    potential_gain = ((entry_price - stop_gain) * qty) / old_balance * 100 if old_balance > 0 else 0
                    trade_data = {
                        'trade_id': trade_id,
                        'timestamp': trade_id,
                        'symbol': symbol,
                        'buy_price': entry_price,
                        'sell_price': '',
                        'quantity': qty,
                        'stop_loss': stop_loss,
                        'stop_gain': stop_gain,
                        'potential_loss': potential_loss,
                        'potential_gain': potential_gain,
                        'commission': commission,
                        'old_balance': old_balance,
                        'new_balance': '',
                        'timeframe': '1h',
                        'setup': 'GPTAN',
                        'outcome': '',
                        'secondary_stop_loss': secondary_stop_loss,
                        'secondary_stop_gain': secondary_stop_gain,
                        'sell_time': '',
                        'type': 'short',  # Define como 'short'
                        'entry_lateral': 1 if isLateral.iloc[-1] else 0,  # 1 se lateral, senão 0
                        'exit_lateral': ''  # Inicialmente vazio
                    }

                    log_trade_entry(trade_data)
                    logging.info(f"Entered short position at {trade_id}, price: {entry_price}")
                    logging.info(f"Stoploss set at {stop_loss:.2f}, Take Profit set at {stop_gain:.2f}")
                    trade_count += 1
        else:
            # Manage open position
            side = position_info['side']
            entry_price = position_info['entry_price']
            size = position_info['size']
            commission_rate = 0.0006  # 0.03%
            if isLateral.iloc[-1]:
                # Lateral market exit conditions
                if side == 'buy':
                    # Long position
                    stop_loss = entry_price * stoploss_lateral_long
                    take_profit = entry_price * stopgain_lateral_long
                    if latest_price <= stop_loss or shortCondition:
                        # Close position at stop loss or reversal
                        
                        try:
                            order = session.place_order(
                                category='linear',
                                symbol=symbol,
                                side='Sell',
                                orderType='Market',
                                qty=str(size),
                                timeInForce='GTC',
                                reduceOnly=True,
                                closeOnTrigger=False
                            )
                            if order['retMsg'] != 'OK':
                                logging.error(f"Error closing long position: {order['retMsg']}")
                                time.sleep(5)
                                continue
                        except Exception as e:
                            logging.error(f"Exception closing long position: {e}")
                            time.sleep(5)
                            continue
                        
                        sell_price = latest_price
                        new_balance = get_account_balance()
                        if new_balance is None:
                            logging.error("Failed to fetch account balance.")
                            new_balance = 0
                        sell_time = datetime.utcnow().isoformat()
                        commission = sell_price * size * commission_rate
                        total_commission = previous_commission + commission
                        outcome = (sell_price - entry_price) * size - total_commission
                        # Ao fechar uma posição short por stop loss ou reversão:
                        exit_lateral = 1 if isLateral.iloc[-1] else 0  # Define exit_lateral
                        update_data = {
                            'sell_price': sell_price,
                            'new_balance': new_balance,
                            'outcome': outcome,
                            'commission': total_commission,
                            'sell_time': sell_time,
                            'secondary_stop_loss': current_secondary_stop_loss,
                            'secondary_stop_gain': current_secondary_stop_gain
                        }
                        log_trade_exit(current_trade_id, symbol, update_data, exit_lateral)
                        logging.info(f"Exited long position at {sell_time}, price: {sell_price}")
                        # Reset tracking variables
                        current_trade_id = None
                        current_position_side = None
                        entry_price = None
                        current_secondary_stop_loss = None
                        current_secondary_stop_gain = None
                        previous_commission = 0
                    elif latest_price >= take_profit:
                        # Close position at take profit
                        
                        try:
                            order = session.place_order(
                                category='linear',
                                symbol=symbol,
                                side='Sell',
                                orderType='Market',
                                qty=str(size),
                                timeInForce='GTC',
                                reduceOnly=True,
                                closeOnTrigger=False
                            )
                            if order['retMsg'] != 'OK':
                                logging.error(f"Error closing long position: {order['retMsg']}")
                                time.sleep(5)
                                continue
                        except Exception as e:
                            logging.error(f"Exception closing long position: {e}")
                            time.sleep(5)
                            continue
                        
                        sell_price = latest_price
                        new_balance = get_account_balance()
                        if new_balance is None:
                            logging.error("Failed to fetch account balance.")
                            new_balance = 0
                        sell_time = datetime.utcnow().isoformat()
                        commission = sell_price * size * commission_rate
                        total_commission = previous_commission + commission
                        outcome = (sell_price - entry_price) * size - total_commission
                        # Ao fechar uma posição long por stop loss ou reversão:
                        exit_lateral = 1 if isLateral.iloc[-1] else 0  # Define exit_lateral
                        update_data = {
                            'sell_price': sell_price,
                            'new_balance': new_balance,
                            'outcome': outcome,
                            'commission': total_commission,
                            'sell_time': sell_time,
                            'secondary_stop_loss': current_secondary_stop_loss,
                            'secondary_stop_gain': current_secondary_stop_gain
                        }
                        log_trade_exit(current_trade_id, symbol, update_data, exit_lateral)
                        logging.info(f"Exited long position at {sell_time}, price: {sell_price}")
                        # Reset tracking variables
                        current_trade_id = None
                        current_position_side = None
                        entry_price = None
                        current_secondary_stop_loss = None
                        current_secondary_stop_gain = None
                        previous_commission = 0
                elif side == 'sell':
                    # Short position
                    stop_loss = entry_price * stoploss_lateral_short
                    take_profit = entry_price * stopgain_lateral_short
                    if latest_price >= stop_loss or longCondition:
                        # Close position at stop loss or reversal
                        
                        try:
                            order = session.place_order(
                                category='linear',
                                symbol=symbol,
                                side='Buy',
                                orderType='Market',
                                qty=str(size),
                                timeInForce='GTC',
                                reduceOnly=True,
                                closeOnTrigger=False
                            )
                            if order['retMsg'] != 'OK':
                                logging.error(f"Error closing short position: {order['retMsg']}")
                                time.sleep(5)
                                continue
                        except Exception as e:
                            logging.error(f"Exception closing short position: {e}")
                            time.sleep(5)
                            continue
                        
                        sell_price = latest_price
                        new_balance = get_account_balance()
                        if new_balance is None:
                            logging.error("Failed to fetch account balance.")
                            new_balance = 0
                        sell_time = datetime.utcnow().isoformat()
                        commission = sell_price * size * commission_rate
                        total_commission = previous_commission + commission
                        outcome = (entry_price - sell_price) * size - total_commission
                        # Ao fechar uma posição short por stop loss ou reversão:
                        exit_lateral = 1 if isLateral.iloc[-1] else 0  # Define exit_lateral
                        update_data = {
                            'sell_price': sell_price,
                            'new_balance': new_balance,
                            'outcome': outcome,
                            'commission': total_commission,
                            'sell_time': sell_time,
                            'secondary_stop_loss': current_secondary_stop_loss,
                            'secondary_stop_gain': current_secondary_stop_gain
                        }
                        log_trade_exit(current_trade_id, symbol, update_data, exit_lateral)

                        logging.info(f"Exited short position at {sell_time}, price: {sell_price}")
                        # Reset tracking variables
                        current_trade_id = None
                        current_position_side = None
                        entry_price = None
                        current_secondary_stop_loss = None
                        current_secondary_stop_gain = None
                        previous_commission = 0
                    elif latest_price <= take_profit:
                        # Close position at take profit
                        
                        try:
                            order = session.place_order(
                                category='linear',
                                symbol=symbol,
                                side='Buy',
                                orderType='Market',
                                qty=str(size),
                                timeInForce='GTC',
                                reduceOnly=True,
                                closeOnTrigger=False
                            )
                            if order['retMsg'] != 'OK':
                                logging.error(f"Error closing short position: {order['retMsg']}")
                                time.sleep(5)
                                continue
                        except Exception as e:
                            logging.error(f"Exception closing short position: {e}")
                            time.sleep(5)
                            continue
                        
                        sell_price = latest_price
                        new_balance = get_account_balance()
                        if new_balance is None:
                            logging.error("Failed to fetch account balance.")
                            new_balance = 0
                        sell_time = datetime.utcnow().isoformat()
                        commission = sell_price * size * commission_rate
                        total_commission = previous_commission + commission
                        outcome = (entry_price - sell_price) * size - total_commission
                        # Ao fechar uma posição short por stop loss ou reversão:
                        exit_lateral = 1 if isLateral.iloc[-1] else 0  # Define exit_lateral
                        update_data = {
                            'sell_price': sell_price,
                            'new_balance': new_balance,
                            'outcome': outcome,
                            'commission': total_commission,
                            'sell_time': sell_time,
                            'secondary_stop_loss': current_secondary_stop_loss,
                            'secondary_stop_gain': current_secondary_stop_gain
                        }
                        log_trade_exit(current_trade_id, symbol, update_data, exit_lateral)

                        logging.info(f"Exited short position at {sell_time}, price: {sell_price}")
                        # Reset tracking variables
                        current_trade_id = None
                        current_position_side = None
                        entry_price = None
                        current_secondary_stop_loss = None
                        current_secondary_stop_gain = None
                        previous_commission = 0
            else:
                # Trending market exit conditions
                if side == 'buy':
                    # Long position
                    stop_loss = entry_price * stoploss_normal_long
                    take_profit = entry_price * stopgain_normal_long
                    if latest_price <= stop_loss or shortCondition:
                        # Close position at stop loss or reversal
                        
                        try:
                            order = session.place_order(
                                category='linear',
                                symbol=symbol,
                                side='Sell',
                                orderType='Market',
                                qty=str(size),
                                timeInForce='GTC',
                                reduceOnly=True,
                                closeOnTrigger=False
                            )
                            if order['retMsg'] != 'OK':
                                logging.error(f"Error closing long position: {order['retMsg']}")
                                time.sleep(5)
                                continue
                        except Exception as e:
                            logging.error(f"Exception closing long position: {e}")
                            time.sleep(5)
                            continue
                        
                        sell_price = latest_price
                        new_balance = get_account_balance()
                        if new_balance is None:
                            logging.error("Failed to fetch account balance.")
                            new_balance = 0
                        sell_time = datetime.utcnow().isoformat()
                        commission = sell_price * size * commission_rate
                        total_commission = previous_commission + commission
                        outcome = (sell_price - entry_price) * size - total_commission
                        # Ao fechar uma posição long por stop loss ou reversão:
                        exit_lateral = 1 if isLateral.iloc[-1] else 0  # Define exit_lateral
                        update_data = {
                            'sell_price': sell_price,
                            'new_balance': new_balance,
                            'outcome': outcome,
                            'commission': total_commission,
                            'sell_time': sell_time,
                            'secondary_stop_loss': current_secondary_stop_loss,
                            'secondary_stop_gain': current_secondary_stop_gain
                        }
                        log_trade_exit(current_trade_id, symbol, update_data, exit_lateral)

                        logging.info(f"Exited long position at {sell_time}, price: {sell_price}")
                        # Reset tracking variables
                        current_trade_id = None
                        current_position_side = None
                        entry_price = None
                        current_secondary_stop_loss = None
                        current_secondary_stop_gain = None
                        previous_commission = 0
                    elif latest_price >= take_profit:
                        # Close position at take profit
                        
                        try:
                            order = session.place_order(
                                category='linear',
                                symbol=symbol,
                                side='Sell',
                                orderType='Market',
                                qty=str(size),
                                timeInForce='GTC',
                                reduceOnly=True,
                                closeOnTrigger=False
                            )
                            if order['retMsg'] != 'OK':
                                logging.error(f"Error closing long position: {order['retMsg']}")
                                time.sleep(5)
                                continue
                        except Exception as e:
                            logging.error(f"Exception closing long position: {e}")
                            time.sleep(5)
                            continue
                        
                        sell_price = latest_price
                        new_balance = get_account_balance()
                        if new_balance is None:
                            logging.error("Failed to fetch account balance.")
                            new_balance = 0
                        sell_time = datetime.utcnow().isoformat()
                        commission = sell_price * size * commission_rate
                        total_commission = previous_commission + commission
                        outcome = (sell_price - entry_price) * size - total_commission
                        # Ao fechar uma posição long por stop loss ou reversão:
                        exit_lateral = 1 if isLateral.iloc[-1] else 0  # Define exit_lateral
                        update_data = {
                            'sell_price': sell_price,
                            'new_balance': new_balance,
                            'outcome': outcome,
                            'commission': total_commission,
                            'sell_time': sell_time,
                            'secondary_stop_loss': current_secondary_stop_loss,
                            'secondary_stop_gain': current_secondary_stop_gain
                        }
                        log_trade_exit(current_trade_id, symbol, update_data, exit_lateral)

                        logging.info(f"Exited long position at {sell_time}, price: {sell_price}")
                        # Reset tracking variables
                        current_trade_id = None
                        current_position_side = None
                        entry_price = None
                        current_secondary_stop_loss = None
                        current_secondary_stop_gain = None
                        previous_commission = 0
                elif side == 'sell':
                    # Short position
                    stop_loss = entry_price * stoploss_normal_short
                    take_profit = entry_price * stopgain_normal_short
                    if latest_price >= stop_loss or longCondition:
                        # Close position at stop loss or reversal
                        
                        try:
                            order = session.place_order(
                                category='linear',
                                symbol=symbol,
                                side='Buy',
                                orderType='Market',
                                qty=str(size),
                                timeInForce='GTC',
                                reduceOnly=True,
                                closeOnTrigger=False
                            )
                            if order['retMsg'] != 'OK':
                                logging.error(f"Error closing short position: {order['retMsg']}")
                                time.sleep(5)
                                continue
                        except Exception as e:
                            logging.error(f"Exception closing short position: {e}")
                            time.sleep(5)
                            continue
                        
                        sell_price = latest_price
                        new_balance = get_account_balance()
                        if new_balance is None:
                            logging.error("Failed to fetch account balance.")
                            new_balance = 0
                        sell_time = datetime.utcnow().isoformat()
                        commission = sell_price * size * commission_rate
                        total_commission = previous_commission + commission
                        outcome = (entry_price - sell_price) * size - total_commission
                        # Ao fechar uma posição short por stop loss ou reversão:
                        exit_lateral = 1 if isLateral.iloc[-1] else 0  # Define exit_lateral
                        update_data = {
                            'sell_price': sell_price,
                            'new_balance': new_balance,
                            'outcome': outcome,
                            'commission': total_commission,
                            'sell_time': sell_time,
                            'secondary_stop_loss': current_secondary_stop_loss,
                            'secondary_stop_gain': current_secondary_stop_gain
                        }
                        log_trade_exit(current_trade_id, symbol, update_data, exit_lateral)
                        logging.info(f"Exited short position at {sell_time}, price: {sell_price}")
                        # Reset tracking variables
                        current_trade_id = None
                        current_position_side = None
                        entry_price = None
                        current_secondary_stop_loss = None
                        current_secondary_stop_gain = None
                        previous_commission = 0
                    elif latest_price <= take_profit:
                        # Close position at take profit
                        
                        try:
                            order = session.place_order(
                                category='linear',
                                symbol=symbol,
                                side='Buy',
                                orderType='Market',
                                qty=str(size),
                                timeInForce='GTC',
                                reduceOnly=True,
                                closeOnTrigger=False
                            )
                            if order['retMsg'] != 'OK':
                                logging.error(f"Error closing short position: {order['retMsg']}")
                                time.sleep(5)
                                continue
                        except Exception as e:
                            logging.error(f"Exception closing short position: {e}")
                            time.sleep(5)
                            continue
                        
                        sell_price = latest_price
                        new_balance = get_account_balance()
                        if new_balance is None:
                            logging.error("Failed to fetch account balance.")
                            new_balance = 0
                        sell_time = datetime.utcnow().isoformat()
                        commission = sell_price * size * commission_rate
                        total_commission = previous_commission + commission
                        outcome = (entry_price - sell_price) * size - total_commission
                        # Ao fechar uma posição short por stop loss ou reversão:
                        exit_lateral = 1 if isLateral.iloc[-1] else 0  # Define exit_lateral
                        update_data = {
                            'sell_price': sell_price,
                            'new_balance': new_balance,
                            'outcome': outcome,
                            'commission': total_commission,
                            'sell_time': sell_time,
                            'secondary_stop_loss': current_secondary_stop_loss,
                            'secondary_stop_gain': current_secondary_stop_gain
                        }
                        log_trade_exit(current_trade_id, symbol, update_data, exit_lateral)
                        logging.info(f"Exited short position at {sell_time}, price: {sell_price}")
                        # Reset tracking variables
                        current_trade_id = None
                        current_position_side = None
                        entry_price = None
                        current_secondary_stop_loss = None
                        current_secondary_stop_gain = None
                        previous_commission = 0

        # Wait for 1 second before next price check
        time.sleep(1)

    except KeyboardInterrupt:
        logging.info("Bot stopped manually.")
        break
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        logging.error(traceback.format_exc())
        time.sleep(5)  # Brief pause before retrying
        continue