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

# Load the existing trade history
trade_history_file = 'trade_history.csv'
if not os.path.isfile(trade_history_file):
    logging.error(f"{trade_history_file} not found. Ensure you have a trade history to update.")
    sys.exit(1)

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

# Function to update the trade log with sell details
def update_trade_with_sell(trade_id, sell_price, sell_time, commission):
    try:
        # Load the trade history
        df_trade_history = pd.read_csv(trade_history_file)
        
        # Locate the trade by trade_id
        mask = (df_trade_history['trade_id'] == trade_id) & (df_trade_history['sell_price'].isna())
        if not mask.any():
            logging.error(f"Trade with ID {trade_id} not found or already sold.")
            return

        # Update the trade with sell information
        df_trade_history.loc[mask, 'sell_price'] = sell_price
        df_trade_history.loc[mask, 'sell_time'] = sell_time
        df_trade_history.loc[mask, 'commission'] += commission  # Add commission for the sell
        df_trade_history.to_csv(trade_history_file, index=False)

        logging.info(f"Trade {trade_id} updated with sell price: {sell_price}, sell time: {sell_time}, commission: {commission}")
    except Exception as e:
        logging.error(f"Exception in update_trade_with_sell: {e}")

# Initialize variables
sell_executed = False  # Flag to check if the forced sell has been executed

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

        # Force a sell order for 0.001 BTC
        if not sell_executed:
            qty = 0.001  # Amount of BTC to sell
            try:
                order = session.place_order(
                    category='linear',
                    symbol=symbol,
                    side='Sell',
                    orderType='Market',
                    qty=str(qty),  # Sell the same quantity that was bought
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

            # Log the sell details
            sell_time = datetime.utcnow().isoformat()
            sell_price = latest_price
            commission_rate = 0.0003  # 0.03% (taker fee for market order)
            commission = sell_price * qty * commission_rate  # Calculate commission for sell order

            # Use the trade ID from the buy transaction to update the log
            df_trade_history = pd.read_csv(trade_history_file)
            last_trade_id = df_trade_history.iloc[-1]['trade_id']  # Assuming the last trade is the one to update

            # Update the trade log with the sell information
            update_trade_with_sell(last_trade_id, sell_price, sell_time, commission)

            logging.info(f"Forced sell order executed at {sell_time}, price: {sell_price}, quantity: {qty}")
            logging.info(f"Trade details updated in {trade_history_file}")
            sell_executed = True  # Set the flag to prevent further forced sells

            # Exit the loop after selling
            break

    except KeyboardInterrupt:
        logging.info("Bot stopped manually.")
        break
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        logging.error(traceback.format_exc())
        time.sleep(5)  # Brief pause before retrying
        continue
