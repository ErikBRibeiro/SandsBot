import pandas as pd
import numpy as np
from datetime import datetime
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

# Adjust the path to /app/data/trade_history.csv
trade_history_file = '/app/data/trade_history.csv'

# Check if the trade history CSV exists, if not create it with headers
if not os.path.isfile(trade_history_file):
    columns = ['trade_id', 'timestamp', 'symbol', 'buy_price', 'sell_price', 'quantity',
               'stop_loss', 'stop_gain', 'potential_loss', 'potential_gain', 'timeframe',
               'setup', 'outcome', 'commission', 'old_balance', 'new_balance',
               'secondary_stop_loss', 'secondary_stop_gain', 'sell_time']
    df_trade_history = pd.DataFrame(columns=columns)
    df_trade_history.to_csv(trade_history_file, index=False)
    logging.info(f"Created new trade history file at {trade_history_file}")

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

# Function to log the trade entry
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

# Function to update the trade with sell details
def update_trade_with_sell(trade_id, sell_price, sell_time, commission):
    try:
        df_trade_history = pd.read_csv(trade_history_file)
        
        # Locate the trade by trade_id
        mask = (df_trade_history['trade_id'] == trade_id) & (df_trade_history['sell_price'].isna())
        if not mask.any():
            logging.error(f"Trade with ID {trade_id} not found or already sold.")
            return

        # Update the trade with sell information
        df_trade_history.loc[mask, 'sell_price'] = sell_price
        df_trade_history.loc[mask, 'sell_time'] = sell_time
        df_trade_history.loc[mask, 'commission'] += commission  # Add sell commission
        df_trade_history.to_csv(trade_history_file, index=False)

        logging.info(f"Trade {trade_id} updated with sell price: {sell_price}, sell time: {sell_time}, commission: {commission}")
    except Exception as e:
        logging.error(f"Exception in update_trade_with_sell: {e}")

# Initialize variables
trade_executed = False  # Flag to check if the trade has been executed
sell_executed = False   # Flag to check if the sell has been executed

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

        # Step 1: Force a short trade (sell to open) if not already executed
        if not trade_executed:
            qty = 0.001  # Amount of BTC to short
            try:
                order = session.place_order(
                    category='linear',
                    symbol=symbol,
                    side='Sell',
                    orderType='Market',
                    qty=str(qty),  # Enter a short position
                    timeInForce='GTC',
                    reduceOnly=False,
                    closeOnTrigger=False
                )
                if order['retMsg'] != 'OK':
                    logging.error(f"Error placing short order: {order['retMsg']}")
                    time.sleep(5)
                    continue
            except Exception as e:
                logging.error(f"Exception placing short order: {e}")
                time.sleep(5)
                continue

            # Log the trade entry
            trade_id = datetime.utcnow().isoformat()
            entry_price = latest_price
            commission_rate = 0.0003  # 0.03% (taker fee for market order)
            commission = entry_price * qty * commission_rate
            trade_data = {
                'trade_id': trade_id,
                'timestamp': trade_id,
                'symbol': symbol,
                'buy_price': '',  # Short trade, so no buy price
                'sell_price': entry_price,  # Entry price for short
                'quantity': qty,
                'stop_loss': '',
                'stop_gain': '',
                'potential_loss': '',
                'potential_gain': '',
                'commission': commission,
                'old_balance': '',
                'new_balance': '',
                'timeframe': 'short',
                'setup': 'Forced Short Test',
                'outcome': '',
                'secondary_stop_loss': '',
                'secondary_stop_gain': '',
                'sell_time': ''
            }
            log_trade_entry(trade_data)
            logging.info(f"Forced short order executed at {trade_id}, price: {entry_price}, quantity: {qty}")
            trade_executed = True  # Set flag to prevent re-entry

            # Wait 20 seconds before exiting the short trade
            time.sleep(20)

        # Step 2: Exit the short trade by buying back (close the short position)
        if trade_executed and not sell_executed:
            try:
                order = session.place_order(
                    category='linear',
                    symbol=symbol,
                    side='Buy',  # Closing the short position by buying back
                    orderType='Market',
                    qty=str(qty),
                    timeInForce='GTC',
                    reduceOnly=True,
                    closeOnTrigger=False
                )
                if order['retMsg'] != 'OK':
                    logging.error(f"Error closing short order: {order['retMsg']}")
                    time.sleep(5)
                    continue
            except Exception as e:
                logging.error(f"Exception closing short order: {e}")
                time.sleep(5)
                continue

            # Log the trade exit
            sell_time = datetime.utcnow().isoformat()
            sell_price = latest_price
            commission = sell_price * qty * commission_rate  # Calculate commission for the exit
            update_trade_with_sell(trade_id, sell_price, sell_time, commission)

            logging.info(f"Forced exit from short position at {sell_time}, price: {sell_price}, quantity: {qty}")
            sell_executed = True  # Set flag to prevent re-entry

            # Exit the loop after the trade is closed
            break

    except KeyboardInterrupt:
        logging.info("Bot stopped manually.")
        break
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        logging.error(traceback.format_exc())
        time.sleep(5)  # Brief pause before retrying
        continue
