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
    columns = ['trade_id', 'time', 'symbol', 'buy_price', 'sell_price', 'quantity',
               'stop_loss', 'stop_gain', 'potential_loss', 'potential_gain', 'timeframe',
               'setup', 'outcome', 'commission', 'old_balance', 'new_balance',
               'secondary_stop_loss', 'secondary_stop_gain', 'sell_time', 'type', 'entry_lateral', 'exit_lateral']
    df_trade_history = pd.DataFrame(columns=columns)
    df_trade_history.to_csv(trade_history_file, index=False)

def get_previous_candle_start(dt, interval_minutes):
    """
    Calculates the start of the previous candle based on the specified interval.

    Args:
        dt (datetime): Current UTC datetime.
        interval_minutes (int): Duration of the interval in minutes.

    Returns:
        datetime: Start datetime of the previous candle.
    """
    interval = timedelta(minutes=interval_minutes)
    # Calculate the start of the current candle
    current_candle_start = dt - timedelta(
        minutes=dt.minute % interval_minutes,
        seconds=dt.second,
        microseconds=dt.microsecond
    )
    # Calculate the start of the previous candle
    previous_candle_start = current_candle_start - interval
    return previous_candle_start

def get_historical_klines_and_append(symbol, interval):
    """
    Fetches the previous candle from Bybit and appends it to the CSV, filling with NaN where necessary.

    Args:
        symbol (str): Trading pair symbol (e.g., 'BTCUSD').
        interval (int): Time interval in minutes (e.g., 60 for 1h).

    Returns:
        pd.DataFrame: DataFrame containing the data of the previous candle or None in case of error.
    """
    # Full list of CSV columns
    csv_columns = [
        'time', 'open', 'high', 'low', 'close',
        'upperBand', 'lowerBand', 'middleBand',
        'emaShort', 'emaLong',
        'adx', 'rsi', 'macdLine', 'signalLine',
        'macdHist', 'bandWidth', 'isLateral'
    ]

    try:
        now = datetime.utcnow()
        # Calculate the start of the previous candle
        previous_candle_start = get_previous_candle_start(now, interval)
        from_time_ms = int(previous_candle_start.timestamp() * 1000)  # Convert to milliseconds

        logging.debug(f"Fetching kline data for symbol: {symbol}, interval: {interval} minutes")
        logging.debug(f"From timestamp (ms): {from_time_ms}")

        # Make the request to get the previous candle
        kline = session.get_kline(
            category='linear',
            symbol=symbol,
            interval=str(interval),
            start=str(from_time_ms),
            limit=1  # Request only 1 candle: the previous complete one
        )

        logging.debug(f"API response: {kline}")

        if kline['retMsg'] != 'OK':
            logging.error(f"Error fetching kline data: {kline['retMsg']}")
            return None

        kline_list = kline['result']['list']

        logging.debug(f"kline_list: {kline_list}")

        # Check if at least 1 candle was returned
        if len(kline_list) < 1:
            logging.error("Not enough candles returned by API.")
            return None

        # Get the previous candle
        kline_data = kline_list[0]

        logging.debug(f"kline_data: {kline_data}")

        # Ensure kline_data is a list with at least 5 elements
        # [timestamp, open, high, low, close, volume, turnover]
        if not isinstance(kline_data, list) or len(kline_data) < 5:
            logging.error("Unexpected kline_data format.")
            return None

        # Create a dictionary for the new row with all columns, filling with NaN
        new_row = {column: float('nan') for column in csv_columns}

        # Fill the available columns with the returned data
        timestamp = int(kline_data[0])

        # Determine if the timestamp is in milliseconds or seconds
        if timestamp > 1e12:
            # It's in milliseconds; convert to seconds
            timestamp = int(timestamp / 1000)
            logging.debug(f"Converted timestamp from ms to s: {timestamp}")
        else:
            logging.debug(f"Timestamp is already in seconds: {timestamp}")

        new_row['time'] = timestamp
        new_row['open'] = float(kline_data[1])
        new_row['high'] = float(kline_data[2])
        new_row['low'] = float(kline_data[3])
        new_row['close'] = float(kline_data[4])
        new_row['isLateral'] = False  # Initialize as False

        logging.debug(f"New row data: {new_row}")

        # Create the DataFrame with the new row
        df_new = pd.DataFrame([new_row], columns=csv_columns)

        # Define the CSV file path
        csv_file = '/app/data/dados_atualizados.csv'

        # Append the candle to the CSV
        if os.path.exists(csv_file):
            # Check if the CSV already has the defined columns
            existing_columns = pd.read_csv(csv_file, nrows=0).columns.tolist()
            # If there are differences in the columns, align them
            if existing_columns != csv_columns:
                logging.warning("Diferença nas colunas do CSV. Ajustando para alinhar as colunas.")
                df_new = df_new[existing_columns]
            df_new.to_csv(csv_file, mode='a', header=False, index=False)
        else:
            df_new.to_csv(csv_file, mode='w', header=True, index=False)

        logging.info(f"Successfully appended new candle to CSV: {csv_file}")
        return df_new
    except Exception as e:
        logging.error(f"Exception in get_historical_klines_and_append: {e}")
        logging.error(traceback.format_exc())
        return None

def macd_func(series, fast_period, slow_period, signal_period):
    ema_fast = talib.EMA(series, fast_period)
    ema_slow = talib.EMA(series, slow_period)
    macd_line = ema_fast - ema_slow
    signal_line = talib.EMA(macd_line, signal_period)
    macd_hist = macd_line - signal_line
    return macd_line, signal_line, macd_hist

def get_adx_manual(high, low, close, di_lookback, adx_smoothing):
    tr = []
    previous_close = close.iloc[0]
    plus_dm = []
    minus_dm = []

    for i in range(1, len(close)):
        current_plus_dm = high.iloc[i] - high.iloc[i-1]
        current_minus_dm = low.iloc[i-1] - low.iloc[i]
        plus_dm.append(max(current_plus_dm, 0) if current_plus_dm > current_minus_dm else 0)
        minus_dm.append(max(current_minus_dm, 0) if current_minus_dm > current_plus_dm else 0)

        tr1 = high.iloc[i] - low.iloc[i]
        tr2 = abs(high.iloc[i] - previous_close)
        tr3 = abs(low.iloc[i] - previous_close)
        true_range = max(tr1, tr2, tr3)
        tr.append(true_range)

        previous_close = close.iloc[i]

    tr.insert(0, np.nan)
    plus_dm.insert(0, np.nan)
    minus_dm.insert(0, np.nan)

    tr = pd.Series(tr)
    plus_dm = pd.Series(plus_dm)
    minus_dm = pd.Series(minus_dm)

    atr = tr.rolling(window=di_lookback).mean()

    plus_di = 100 * (plus_dm.ewm(alpha=1/di_lookback, adjust=False).mean() / atr)
    minus_di = 100 * (minus_dm.ewm(alpha=1/di_lookback, adjust=False).mean() / atr)

    dx = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
    adx = dx.ewm(alpha=1/adx_smoothing, adjust=False).mean()

    return adx

# Function to calculate indicators
def calculate_indicators():
    """
    Calculates technical indicators and updates the CSV 'dados_atualizados.csv' with the calculated values.

    Returns:
        pd.DataFrame: Updated DataFrame with indicators or None in case of error.
    """
    try:
        # Define the path to the CSV file
        csv_file = '/app/data/dados_atualizados.csv'

        # Check if the CSV file exists
        if not os.path.exists(csv_file):
            logging.error(f"CSV file does not exist: {csv_file}")
            return None

        # Load the existing CSV including the new row
        df = pd.read_csv(csv_file)

        # Ensure the DataFrame is sorted by time
        df = df.sort_values('time').reset_index(drop=True)

        # Extract prices from the DataFrame
        close_price = df['close']
        high_price = df['high']
        low_price = df['low']

        # Indicator parameters
        emaShortLength = 11
        emaLongLength = 55
        rsiLength = 22
        macdShort = 15
        macdLong = 34
        macdSignal = 11
        adxLength = 16
        adxSmoothing = 13
        bbLength = 14
        bbMultiplier = 1.7
        lateralThreshold = 0.005

        # Calculating indicators using TA-Lib
        emaShort = talib.EMA(close_price, emaShortLength)
        emaLong = talib.EMA(close_price, emaLongLength)
        rsi = talib.RSI(close_price, timeperiod=rsiLength)
        macdLine, signalLine, macdHist = macd_func(close_price, macdShort, macdLong, macdSignal)
        upperBand, middleBand, lowerBand = talib.BBANDS(
            close_price, timeperiod=bbLength, nbdevup=bbMultiplier, nbdevdn=bbMultiplier
        )
        adx = get_adx_manual(high_price, low_price, close_price, adxLength, adxSmoothing)
        adx = adx.fillna(0).astype(int)
        bandWidth = (upperBand - lowerBand) / middleBand
        isLateral = bandWidth < lateralThreshold

        # Map indicators to DataFrame columns
        df['emaShort'] = emaShort
        df['emaLong'] = emaLong
        df['adx'] = adx
        df['rsi'] = rsi
        df['macdLine'] = macdLine
        df['signalLine'] = signalLine
        df['macdHist'] = macdHist
        df['upperBand'] = upperBand
        df['middleBand'] = middleBand
        df['lowerBand'] = lowerBand
        df['bandWidth'] = bandWidth
        df['isLateral'] = isLateral.astype(bool)

        # Save the updated CSV
        df.to_csv(csv_file, index=False)

        logging.info(f"Successfully updated indicators in CSV: {csv_file}")

        return df

    except Exception as e:
        logging.error(f"Exception in calculate_indicators: {e}")
        logging.error(traceback.format_exc())
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

# Function to ensure 'isLateral' is boolean
def ensure_isLateral_boolean(df):
    """
    Checks if the 'isLateral' column in the DataFrame is of boolean type.
    If not, converts it to boolean, where 1 becomes True and 0 becomes False.
    Then saves the updated DataFrame to '/app/data/dados_atualizados.csv'.
    
    Args:
        df (pd.DataFrame): The DataFrame to check and modify.
        
    Returns:
        pd.DataFrame: The modified DataFrame with 'isLateral' as boolean type.
    """
    if df['isLateral'].dtype != 'bool':
        # Convert values: 1 to True, others to False
        df['isLateral'] = df['isLateral'] == 1
        # Save the updated DataFrame to CSV
        csv_file = '/app/data/dados_atualizados.csv'
        df.to_csv(csv_file, index=False)
        logging.info(f"'isLateral' column converted to boolean and DataFrame saved to {csv_file}")
    return df

# Function to get the current position
def get_current_position(retries=3, backoff_factor=5):
    attempt = 0
    while attempt < retries:
        try:
            # Using the correct method to get positions
            positions = session.get_positions(
                category='linear',  # linear for perpetual contracts
                symbol=symbol       # symbol pair like BTCUSDT
            )

            # Check if the response was successful
            if positions['retMsg'] != 'OK':
                return None, None

            positions_data = positions['result']['list']

            # Check if there is an open position
            for pos in positions_data:
                size = float(pos['size'])
                if size != 0:
                    side = (pos['side']).lower()
                    entry_price = float(pos['avgPrice'])
                    return side, {'entry_price': entry_price, 'size': size, 'side': side}

            # If no position is open, return False
            return False, None

        except Exception as e:
            logging.error(f"Unexpected error in get_current_position: {e}")
            logging.error(traceback.format_exc())
            attempt += 1
            time.sleep(backoff_factor * attempt)

    logging.error("Failed to get current position after several attempts.")
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

        # Accessing totalEquity correctly in the list inside result
        total_equity = float(balance_info['result']['list'][0]['totalEquity'])
        return total_equity
    except Exception as e:
        logging.error(f"Exception in get_account_balance: {e}")
        return None

# Function to calculate quantity based on equity and price
def calculate_qty(total_equity, latest_price, leverage=1):
    try:
        # Calculate the number of contracts
        qty = (total_equity * leverage) / latest_price
        factor = 100
        # Adjust for the precision allowed by Bybit (e.g., 2 decimal places)
        qty = math.floor(qty * factor) / factor

        return qty
    except Exception as e:
        logging.error(f"Exception in calculate_qty: {e}")
        return None

# Functions to log trade entries and exits
def log_trade_entry(trade_data):
    try:
        if os.path.isfile(trade_history_file):
            df_trade_history = pd.read_csv(trade_history_file)
        else:
            columns = ['trade_id', 'time', 'symbol', 'buy_price', 'sell_price', 'quantity',
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
            update_data['exit_lateral'] = exit_lateral  # Add 'exit_lateral'
            for key, value in update_data.items():
                df_trade_history.loc[mask, key] = value
            df_trade_history.to_csv(trade_history_file, index=False)
        else:
            logging.error("Trade not found in trade history for update.")
    except Exception as e:
        logging.error(f"Exception in log_trade_exit: {e}")

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
# Variables to track current trade
current_trade_id = None
current_position_side = None
entry_price = None
current_secondary_stop_loss = None
current_secondary_stop_gain = None
previous_commission = 0  # To store the entry commission

# Initialize variables
last_fetched_hour = None  # To track the last hour when klines were fetched
last_log_time = None     # To keep track of logging every 10 minutes
previous_isLateral = None  # To detect transitions to lateral market

# Main trading loop
while True:
    try:
        current_time = datetime.utcnow()
        current_hour = current_time.hour

        # Load the indicators and data
        df = pd.read_csv('/app/data/dados_atualizados.csv')
        df = df.sort_values('time').reset_index(drop=True)
        df = ensure_isLateral_boolean(df)

        # Obtain the last row of the DataFrame for current calculations
        last_row = df.iloc[-1]
        adjusted_timestamp = last_row['time']
        emaShort = df['emaShort']
        emaLong = df['emaLong']
        rsi = df['rsi']
        macdHist = df['macdHist']
        adx = df['adx']
        isLateral = df['isLateral']
        upperBand = df['upperBand']
        lowerBand = df['lowerBand']
        bandWidth = df['bandWidth']

        # Determine if the market is trending
        trendingMarket = adx.iloc[-1] >= 12  # adxThreshold

        # Check if it's time to fetch klines (once per hour)
        if last_fetched_hour != current_hour:
            # Fetch the latest 1-hour kline data
            df_new_row = get_historical_klines_and_append(symbol, interval=60)
            if df_new_row is None or df_new_row.empty:
                logging.error("Failed to fetch historical klines or received empty data.")
                time.sleep(10)
                continue

            # Calculate indicators
            df = calculate_indicators()
            if df is None:
                logging.error("Failed to calculate indicators.")
                time.sleep(10)
                continue

            # Re-assign the variables after updating indicators
            df = df.sort_values('time').reset_index(drop=True)
            last_row = df.iloc[-1]
            adjusted_timestamp = last_row['time']
            emaShort = df['emaShort']
            emaLong = df['emaLong']
            rsi = df['rsi']
            macdHist = df['macdHist']
            adx = df['adx']
            isLateral = df['isLateral']
            upperBand = df['upperBand']
            lowerBand = df['lowerBand']
            bandWidth = df['bandWidth']

            # Ensure 'isLateral' is boolean
            df = ensure_isLateral_boolean(df)

            # Determine if the market is trending
            trendingMarket = adx.iloc[-1] >= 12  # adxThreshold

            # Detect transition to lateral or trending market
            if previous_isLateral is not None and isLateral.iloc[-1] != previous_isLateral:
                # Handle market condition transitions
                if isLateral.iloc[-1]:
                    # Entered lateral market
                    logging.info("Entered lateral market. Stopgain and Stoploss levels adjusted.")
                    logging.info(f"Stopgain and Stoploss levels for lateral market - Long: Stopgain {stopgain_lateral_long}, Stoploss {stoploss_lateral_long}; Short: Stopgain {stopgain_lateral_short}, Stoploss {stoploss_lateral_short}")
                else:
                    # Exited lateral market
                    logging.info("Exited lateral market. Stopgain and Stoploss levels adjusted.")
                    logging.info(f"Stopgain and Stoploss levels for trending market - Long: Stopgain {stopgain_normal_long}, Stoploss {stoploss_normal_long}; Short: Stopgain {stopgain_normal_short}, Stoploss {stoploss_normal_short}")
                # Update secondary_stop_loss and secondary_stop_gain if there's an open position
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

            # Update the last fetched hour
            last_fetched_hour = current_hour

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
                logging.info(f'DF INDICATORS \n{df}')
            last_log_time = current_time

        # Fetch the latest price every second
        latest_price = get_latest_price()
        if latest_price is None:
            logging.error("Failed to fetch latest price.")
            time.sleep(5)
            continue

        # Implement real-time entry and exit logic based on latest_price and indicators
        # Long and Short Conditions
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

        # Obtain current position
        current_position, position_info = get_current_position()
        if current_position is None:
            logging.info("Failed to fetch current position.")
            time.sleep(5)
            continue

        # Implementar lógica de trading
        if not current_position:
            if isLateral.iloc[-1]:
                # Estratégia de Reversão à Média no Mercado Lateral
                if (latest_price < lowerBand.iloc[-1]) and longCondition:
                    # Abrir posição long
                    total_equity = get_account_balance()
                    qty = calculate_qty(total_equity, latest_price)  # Defina seu tamanho de posição

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
                        old_balance = 0  # Defina como zero para evitar erros de cálculo
                    entry_price = latest_price
                    stop_loss = entry_price * stoploss_lateral_long
                    stop_gain = entry_price * stopgain_lateral_long
                    secondary_stop_loss = stop_loss
                    secondary_stop_gain = stop_gain
                    current_secondary_stop_loss = secondary_stop_loss
                    current_secondary_stop_gain = secondary_stop_gain
                    commission_rate = 0.0006  # 0.06%
                    commission = entry_price * qty * commission_rate
                    previous_commission = commission
                    potential_loss = ((entry_price - stop_loss) * qty) / old_balance * 100 if old_balance > 0 else 0
                    potential_gain = ((stop_gain - entry_price) * qty) / old_balance * 100 if old_balance > 0 else 0
                    trade_data = {
                        'trade_id': trade_id,
                        'time': trade_id,
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
                    # Abrir posição short
                    total_equity = get_account_balance()
                    qty = calculate_qty(total_equity, latest_price)  # Defina seu tamanho de posição

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
                        old_balance = 0  # Defina como zero para evitar erros de cálculo
                    entry_price = latest_price
                    stop_loss = entry_price * stoploss_lateral_short
                    stop_gain = entry_price * stopgain_lateral_short
                    secondary_stop_loss = stop_loss
                    secondary_stop_gain = stop_gain
                    current_secondary_stop_loss = secondary_stop_loss
                    current_secondary_stop_gain = secondary_stop_gain
                    commission_rate = 0.0006  # 0.06%
                    commission = entry_price * qty * commission_rate
                    previous_commission = commission
                    potential_loss = ((stop_loss - entry_price) * qty) / old_balance * 100 if old_balance > 0 else 0
                    potential_gain = ((entry_price - stop_gain) * qty) / old_balance * 100 if old_balance > 0 else 0
                    trade_data = {
                        'trade_id': trade_id,
                        'time': trade_id,
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
                # Estratégia de Seguimento de Tendência no Mercado em Tendência
                if longCondition:
                    # Abrir posição long
                    total_equity = get_account_balance()
                    qty = calculate_qty(total_equity, latest_price)  # Defina seu tamanho de posição

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
                        old_balance = 0  # Defina como zero para evitar erros de cálculo
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
                        'time': trade_id,
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
                    # Abrir posição short
                    total_equity = get_account_balance()
                    qty = calculate_qty(total_equity, latest_price)  # Defina seu tamanho de posição

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
                        old_balance = 0  # Defina como zero para evitar erros de cálculo
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
                        'time': trade_id,
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
            # Gerenciar posição aberta
            side = position_info['side']
            entry_price = position_info['entry_price']
            size = position_info['size']
            commission_rate = 0.0006  # 0.06%
            if isLateral.iloc[-1]:
                # Condições de saída no mercado lateral
                if side == 'buy':
                    # Posição long
                    stop_loss = entry_price * stoploss_lateral_long
                    take_profit = entry_price * stopgain_lateral_long
                    if latest_price <= stop_loss or shortCondition:
                        # Fechar posição por stop loss ou reversão
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
                        # Definir exit_lateral
                        exit_lateral = 1 if isLateral.iloc[-1] else 0
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
                        # Resetar variáveis de rastreamento
                        current_trade_id = None
                        current_position_side = None
                        entry_price = None
                        current_secondary_stop_loss = None
                        current_secondary_stop_gain = None
                        previous_commission = 0
                    elif latest_price >= take_profit:
                        # Fechar posição por take profit
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
                        # Definir exit_lateral
                        exit_lateral = 1 if isLateral.iloc[-1] else 0
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
                        # Resetar variáveis de rastreamento
                        current_trade_id = None
                        current_position_side = None
                        entry_price = None
                        current_secondary_stop_loss = None
                        current_secondary_stop_gain = None
                        previous_commission = 0
                elif side == 'sell':
                    # Posição short
                    stop_loss = entry_price * stoploss_lateral_short
                    take_profit = entry_price * stopgain_lateral_short
                    if latest_price >= stop_loss or longCondition:
                        # Fechar posição por stop loss ou reversão
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
                        # Definir exit_lateral
                        exit_lateral = 1 if isLateral.iloc[-1] else 0
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
                        # Resetar variáveis de rastreamento
                        current_trade_id = None
                        current_position_side = None
                        entry_price = None
                        current_secondary_stop_loss = None
                        current_secondary_stop_gain = None
                        previous_commission = 0
                    elif latest_price <= take_profit:
                        # Fechar posição por take profit
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
                        # Definir exit_lateral
                        exit_lateral = 1 if isLateral.iloc[-1] else 0
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
                        # Resetar variáveis de rastreamento
                        current_trade_id = None
                        current_position_side = None
                        entry_price = None
                        current_secondary_stop_loss = None
                        current_secondary_stop_gain = None
                        previous_commission = 0
            else:
                # Condições de saída no mercado de tendência
                if side == 'buy':
                    # Posição long
                    stop_loss = entry_price * stoploss_normal_long
                    take_profit = entry_price * stopgain_normal_long
                    if latest_price <= stop_loss or shortCondition:
                        # Fechar posição por stop loss ou reversão
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
                        # Definir exit_lateral
                        exit_lateral = 1 if isLateral.iloc[-1] else 0
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
                        # Resetar variáveis de rastreamento
                        current_trade_id = None
                        current_position_side = None
                        entry_price = None
                        current_secondary_stop_loss = None
                        current_secondary_stop_gain = None
                        previous_commission = 0
                    elif latest_price >= take_profit:
                        # Fechar posição por take profit
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
                        # Definir exit_lateral
                        exit_lateral = 1 if isLateral.iloc[-1] else 0
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
                        # Resetar variáveis de rastreamento
                        current_trade_id = None
                        current_position_side = None
                        entry_price = None
                        current_secondary_stop_loss = None
                        current_secondary_stop_gain = None
                        previous_commission = 0
                elif side == 'sell':
                    # Posição short
                    stop_loss = entry_price * stoploss_normal_short
                    take_profit = entry_price * stopgain_normal_short
                    if latest_price >= stop_loss or longCondition:
                        # Fechar posição por stop loss ou reversão
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
                        # Definir exit_lateral
                        exit_lateral = 1 if isLateral.iloc[-1] else 0
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
                        # Resetar variáveis de rastreamento
                        current_trade_id = None
                        current_position_side = None
                        entry_price = None
                        current_secondary_stop_loss = None
                        current_secondary_stop_gain = None
                        previous_commission = 0
                    elif latest_price <= take_profit:
                        # Fechar posição por take profit
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
                        # Definir exit_lateral
                        exit_lateral = 1 if isLateral.iloc[-1] else 0
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
                        # Resetar variáveis de rastreamento
                        current_trade_id = None
                        current_position_side = None
                        entry_price = None
                        current_secondary_stop_loss = None
                        current_secondary_stop_gain = None
                        previous_commission = 0

        # Wait 1 second before the next check
        time.sleep(1)

    except KeyboardInterrupt:
        logging.info("Bot stopped manually.")
        break
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        logging.error(traceback.format_exc())
        time.sleep(5)  # Brief pause before retrying
        continue