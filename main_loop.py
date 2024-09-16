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
trade_history_file = 'trade_history.csv'
if not os.path.isfile(trade_history_file):
    columns = ['trade_id', 'timestamp', 'symbol', 'buy_price', 'sell_price', 'quantity',
               'stop_loss', 'stop_gain', 'potential_loss', 'potential_gain', 'timeframe',
               'setup', 'outcome', 'commission', 'old_balance', 'new_balance',
               'secondary_stop_loss', 'secondary_stop_gain', 'sell_time']
    df_trade_history = pd.DataFrame(columns=columns)
    df_trade_history.to_csv(trade_history_file, index=False)

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
        total_equity = float(balance_info['result']['totalEquity'])
        return total_equity
    except Exception as e:
        logging.error(f"Exception in get_account_balance: {e}")
        return None

# Functions to log trade entries
def log_trade_entry(trade_data):
    try:
        if os.path.isfile(trade_history_file):
            df_trade_history = pd.read_csv(trade_history_file)
        else:
            columns = ['trade_id', 'timestamp', 'symbol', 'buy_price', 'sell_price', 'quantity',
                       'stop_loss', 'stop_gain', 'potential_loss', 'potential_gain', 'timeframe',
                       'setup', 'outcome', 'commission', 'old_balance', 'new_balance',
                       'secondary_stop_loss', 'secondary_stop_gain', 'sell_time']
            df_trade_history = pd.DataFrame(columns=columns)
        df_trade_history = df_trade_history.append(trade_data, ignore_index=True)
        df_trade_history.to_csv(trade_history_file, index=False)
    except Exception as e:
        logging.error(f"Exception in log_trade_entry: {e}")

# Initialize variables
trade_executed = False  # Flag to check if the forced trade has been executed

# Main trading loop
while True:
    try:
        current_time = datetime.utcnow()

        # Fetch the latest price
        latest_price = get_latest_price()
        if latest_price is None:
            logging.error("Failed to fetch latest price.")
            time.sleep(5)
            continue

        # Force a buy order of $100 immediately if not already executed
        if not trade_executed:
            # Calculate quantity to buy $100 worth of BTC
            qty = 100 / latest_price  # Calculate quantity based on $100 and latest price
            qty = round(qty, 6)  # Round to 6 decimal places for precision

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
            old_balance = get_account_balance()
            if old_balance is None:
                logging.error("Failed to fetch account balance.")
                old_balance = 0  # Set to zero to avoid calculation errors
            entry_price = latest_price
            commission_rate = 0.0006  # 0.06% (taker fee for market order)
            commission = entry_price * qty * commission_rate
            trade_data = {
                'trade_id': trade_id,
                'timestamp': trade_id,
                'symbol': symbol,
                'buy_price': entry_price,
                'sell_price': '',
                'quantity': qty,
                'stop_loss': '',
                'stop_gain': '',
                'potential_loss': '',
                'potential_gain': '',
                'commission': commission,
                'old_balance': old_balance,
                'new_balance': '',
                'timeframe': '',
                'setup': 'Forced Buy Test',
                'outcome': '',
                'secondary_stop_loss': '',
                'secondary_stop_gain': '',
                'sell_time': ''
            }
            log_trade_entry(trade_data)
            logging.info(f"Forced buy order executed at {trade_id}, price: {entry_price}, quantity: {qty}")
            logging.info(f"Trade details logged to {trade_history_file}")
            trade_executed = True  # Set the flag to prevent further forced trades

            # Wait for a moment before continuing
            time.sleep(1)
            continue  # Skip the rest of the loop

        # Since we don't want to sell, we can simply keep the bot running
        # Optionally, you can add a condition to exit the loop after a certain time
        logging.info("Forced trade executed. Bot is idle as per your request.")
        time.sleep(60)  # Wait for 60 seconds before the next check

    except KeyboardInterrupt:
        logging.info("Bot stopped manually.")
        break
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        logging.error(traceback.format_exc())
        time.sleep(5)  # Brief pause before retrying
        continue
