import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import talib
import os
import time
from dotenv import load_dotenv
from pybit.unified_trading import HTTP
import logging
import traceback

# Load API key and secret from .env file
load_dotenv()
API_KEY = os.getenv('API_KEY')
API_SECRET = os.getenv('API_SECRET')

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

# Function to fetch historical kline data
def get_historical_klines(symbol, interval, limit):
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

# Function to calculate indicators
def calculate_indicators(df):
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

# Helper functions for crossover logic
def crossover(series1, series2):
    cross = (series1 > series2) & (series1.shift(1) <= series2.shift(1))
    return cross

def crossunder(series1, series2):
    cross = (series1 < series2) & (series1.shift(1) >= series2.shift(1))
    return cross

# Function to get current position
def get_current_position():
    positions = session.get_positions(
        category='linear',
        symbol=symbol
    )
    positions_data = positions['result']['list']
    for pos in positions_data:
        # Uncomment the next line to log the position data for debugging
        logging.info(f"Position data: {pos}")
        if float(pos['size']) != 0:
            side = pos['side']
            entry_price = float(pos['avgPrice'])
            size = float(pos['size'])
            return side.lower(), {'entry_price': entry_price, 'size': size, 'side': side}
    return None, None

# Function to fetch the latest price
def get_latest_price():
    ticker = session.get_tickers(
        category='linear',
        symbol=symbol
    )
    price = float(ticker['result']['list'][0]['lastPrice'])
    return price

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

# Main trading loop
while True:
    try:
        current_time = datetime.utcnow()
        # Check if it's time to update the indicators (every hour)
        if last_candle_time is None or (current_time - last_candle_time).seconds >= 3600:
            # Fetch the latest 1-hour kline data
            df = get_historical_klines(symbol, interval=60, limit=200)
            df = df.sort_values('timestamp')

            # Calculate indicators
            df = calculate_indicators(df)

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

            previous_isLateral = isLateral.iloc[-1]

            # Update last_candle_time
            last_candle_time = current_time

            logging.info(f"Indicators updated at {current_time}")

        # Log bot status every 10 minutes
        if last_log_time is None or (current_time - last_log_time).total_seconds() >= 600:
            current_position, position_info = get_current_position()
            if not current_position:
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
            last_log_time = current_time

        # Fetch the latest price every second
        latest_price = get_latest_price()

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

        # Implement trading logic
        if not current_position:
            if isLateral.iloc[-1]:
                # Mean Reversion Strategy in Lateral Market
                if (latest_price < lowerBand.iloc[-1]) and longCondition:
                    # Open long position
                    qty = 0.01  # Define your position size
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
                    logging.info(f"Entered long position at {datetime.utcnow()}, price: {latest_price}")
                    logging.info(f"Stoploss set at {latest_price * stoploss_lateral_long:.2f}, Take Profit set at {latest_price * stopgain_lateral_long:.2f}")
                    trade_count += 1
                elif (latest_price > upperBand.iloc[-1]) and shortCondition:
                    # Open short position
                    qty = 0.01  # Define your position size
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
                    logging.info(f"Entered short position at {datetime.utcnow()}, price: {latest_price}")
                    logging.info(f"Stoploss set at {latest_price * stoploss_lateral_short:.2f}, Take Profit set at {latest_price * stopgain_lateral_short:.2f}")
                    trade_count += 1
            else:
                # Trend Following Strategy in Trending Market
                if longCondition:
                    # Open long position
                    qty = 0.01  # Define your position size
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
                    logging.info(f"Entered long position at {datetime.utcnow()}, price: {latest_price}")
                    logging.info(f"Stoploss set at {latest_price * stoploss_normal_long:.2f}, Take Profit set at {latest_price * stopgain_normal_long:.2f}")
                    trade_count += 1
                elif shortCondition:
                    # Open short position
                    qty = 0.01  # Define your position size
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
                    logging.info(f"Entered short position at {datetime.utcnow()}, price: {latest_price}")
                    logging.info(f"Stoploss set at {latest_price * stoploss_normal_short:.2f}, Take Profit set at {latest_price * stopgain_normal_short:.2f}")
                    trade_count += 1
        else:
            # Manage open position
            side = position_info['side']
            entry_price = position_info['entry_price']
            size = position_info['size']
            if isLateral.iloc[-1]:
                # Lateral market exit conditions
                if side == 'buy':
                    # Long position
                    stop_loss = entry_price * stoploss_lateral_long
                    take_profit = entry_price * stopgain_lateral_long
                    if latest_price <= stop_loss or shortCondition:
                        # Close position at stop loss or reversal
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
                        logging.info(f"Exited long position at {datetime.utcnow()}, price: {latest_price}")
                    elif latest_price >= take_profit:
                        # Close position at take profit
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
                        logging.info(f"Exited long position at {datetime.utcnow()}, price: {latest_price}")
                elif side == 'sell':
                    # Short position
                    stop_loss = entry_price * stoploss_lateral_short
                    take_profit = entry_price * stopgain_lateral_short
                    if latest_price >= stop_loss or longCondition:
                        # Close position at stop loss or reversal
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
                        logging.info(f"Exited short position at {datetime.utcnow()}, price: {latest_price}")
                    elif latest_price <= take_profit:
                        # Close position at take profit
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
                        logging.info(f"Exited short position at {datetime.utcnow()}, price: {latest_price}")
            else:
                # Trending market exit conditions
                if side == 'buy':
                    # Long position
                    stop_loss = entry_price * stoploss_normal_long
                    take_profit = entry_price * stopgain_normal_long
                    if latest_price <= stop_loss or shortCondition:
                        # Close position at stop loss or reversal
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
                        logging.info(f"Exited long position at {datetime.utcnow()}, price: {latest_price}")
                    elif latest_price >= take_profit:
                        # Close position at take profit
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
                        logging.info(f"Exited long position at {datetime.utcnow()}, price: {latest_price}")
                elif side == 'sell':
                    # Short position
                    stop_loss = entry_price * stoploss_normal_short
                    take_profit = entry_price * stopgain_normal_short
                    if latest_price >= stop_loss or longCondition:
                        # Close position at stop loss or reversal
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
                        logging.info(f"Exited short position at {datetime.utcnow()}, price: {latest_price}")
                    elif latest_price <= take_profit:
                        # Close position at take profit
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
                        logging.info(f"Exited short position at {datetime.utcnow()}, price: {latest_price}")

        # Wait for 1 second before next price check
        time.sleep(1)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        logging.error(traceback.format_exc())
        time.sleep(5)
