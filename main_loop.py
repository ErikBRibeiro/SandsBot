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
               'secondary_stop_loss', 'secondary_stop_gain', 'sell_time', 'type', 'entry_lateral', 'exit_lateral']
    df_trade_history = pd.DataFrame(columns=columns)
    df_trade_history.to_csv(trade_history_file, index=False)

def get_previous_candle_start(dt, interval_minutes):
    """
    Calcula o início do candle anterior com base no intervalo especificado.
    
    Args:
        dt (datetime): Data e hora atuais em UTC.
        interval_minutes (int): Duração do intervalo em minutos.
    
    Returns:
        datetime: Data e hora do início do candle anterior.
    """
    interval = timedelta(minutes=interval_minutes)
    # Calcula o início do candle atual
    current_candle_start = dt - timedelta(
        minutes=dt.minute % interval_minutes,
        seconds=dt.second,
        microseconds=dt.microsecond
    )
    # Calcula o início do candle anterior
    previous_candle_start = current_candle_start - interval
    return previous_candle_start

def get_historical_klines_and_append(symbol, interval):
    """
    Busca o candle anterior da Bybit e adiciona ao CSV, preenchendo com NaN onde necessário.
    
    Args:
        symbol (str): Símbolo do par de negociação (e.g., 'BTCUSD').
        interval (int): Intervalo de tempo em minutos (e.g., 60 para 1h).
    
    Returns:
        pd.DataFrame: DataFrame contendo os dados do candle anterior ou None em caso de erro.
    """
    # Lista completa de colunas do CSV
    csv_columns = [
        'time', 'open', 'high', 'low', 'close',
        'Upper Band', 'Lower Band', 'Middle Band',
        'EMA Curta (21)', 'EMA Longa (55)',
        'ADX', 'ADX Plus', 'ADX Minus',
        'RSI', 'MACD Line', 'Signal Line',
        'MACD Histogram', 'BandWidth'
    ]
    
    try:
        now = datetime.utcnow()
        # Calcula o início do candle anterior
        previous_candle_start = get_previous_candle_start(now, interval)
        from_time_ms = int(previous_candle_start.timestamp() * 1000)  # Converte para milissegundos
        
        logging.debug(f"Fetching kline data for symbol: {symbol}, interval: {interval} minutes")
        logging.debug(f"From timestamp (ms): {from_time_ms}")
        
        # Faz a requisição para obter o candle anterior
        kline = session.get_kline(
            category='linear',
            symbol=symbol,
            interval=str(interval),
            start=str(from_time_ms),
            limit=1  # Solicita apenas 1 candle: o anterior completo
        )
        
        logging.debug(f"API response: {kline}")
        
        if kline['retMsg'] != 'OK':
            logging.error(f"Error fetching kline data: {kline['retMsg']}")
            return None
        
        kline_list = kline['result']['list']
        
        logging.debug(f"kline_list: {kline_list}")
        
        # Verifica se pelo menos 1 candle foi retornado
        if len(kline_list) < 1:
            logging.error("Not enough candles returned by API.")
            return None
        
        # Obtém o candle anterior
        kline_data = kline_list[0]
        
        logging.debug(f"kline_data: {kline_data}")
        
        # Certifique-se de que kline_data seja uma lista com pelo menos 5 elementos
        # [timestamp, open, high, low, close, volume, turnover]
        if not isinstance(kline_data, list) or len(kline_data) < 5:
            logging.error("Unexpected kline_data format.")
            return None
        
        # Cria um dicionário para a nova linha com todas as colunas, preenchendo com NaN
        new_row = {column: float('nan') for column in csv_columns}
        
        # Preenche as colunas disponíveis com os dados retornados
        timestamp = int(kline_data[0])
        # Determinar a unidade do timestamp
        if timestamp > 1e12:
            unit = 'ms'
        else:
            unit = 's'
        
        try:
            new_row['time'] = pd.to_datetime(timestamp, unit=unit)
        except Exception as e:
            logging.error(f"Error converting timestamp: {timestamp} with unit={unit}")
            logging.error(traceback.format_exc())
            return None
        
        new_row['open'] = float(kline_data[1])
        new_row['high'] = float(kline_data[2])
        new_row['low'] = float(kline_data[3])
        new_row['close'] = float(kline_data[4])
        
        # Se existir, adicione volume e turnover (ajuste conforme necessário)
        # Se o CSV não tiver essas colunas, ignore
        # new_row['volume'] = float(kline_data[5]) if len(kline_data) > 5 else float('nan')
        # new_row['turnover'] = float(kline_data[6]) if len(kline_data) > 6 else float('nan')
        
        logging.debug(f"New row data: {new_row}")
        
        # Cria o DataFrame com a nova linha
        df_new = pd.DataFrame([new_row], columns=csv_columns)
        
        # Define o caminho do arquivo CSV
        csv_file = '/app/data/dados_atualizados.csv'
        
        # Adiciona o candle ao CSV
        if os.path.exists(csv_file):
            # Verifica se o CSV já possui as colunas definidas
            existing_columns = pd.read_csv(csv_file, nrows=0).columns.tolist()
            # Se houver diferenças nas colunas, alinha-as
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
    # Inverter os dados para processar do mais antigo para o mais recente
    high = high.iloc[::-1].reset_index(drop=True)
    low = low.iloc[::-1].reset_index(drop=True)
    close = close.iloc[::-1].reset_index(drop=True)

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

    # Reverter os resultados para a ordem original (do mais recente para o mais antigo)
    plus_di = plus_di.iloc[::-1].reset_index(drop=True)
    minus_di = minus_di.iloc[::-1].reset_index(drop=True)
    adx = adx.iloc[::-1].reset_index(drop=True)

    return plus_di, minus_di, adx

# Function to calculate indicators
def calculate_indicators(df):
    try:
        close_price = df['close']
        high_price = df['high']
        low_price = df['low']

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
        macdLine, signalLine, macdHist = macd_func(close_price, macdShort, macdLong, macdSignal)
        upperBand, middleBand, lowerBand = talib.BBANDS(
            close_price, timeperiod=bbLength, nbdevup=bbMultiplier, nbdevdn=bbMultiplier
        )
        plus_di, minus_di, adx = get_adx_manual(high_price, low_price, close_price, adxLength, adxSmoothing)
        adx = adx.fillna(0).astype(int)
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
            for key, value in update_data.items():
                df_trade_history.loc[mask, key] = value
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
last_fetched_hour = None  # Para rastrear a última hora em que os klines foram buscados
last_log_time = None     # Para manter o controle do log a cada 10 minutos
previous_isLateral = None  # Para detectar transições para o mercado lateral

# Variables to track current trade
current_trade_id = None
current_position_side = None
entry_price = None
current_secondary_stop_loss = None
current_secondary_stop_gain = None
previous_commission = 0  # Para armazenar a comissão da entrada

# Main trading loop
while True:
    try:
        current_time = datetime.utcnow()
        current_hour = current_time.hour
        current_minute = current_time.minute
        current_second = current_time.second

        # Verificar se é o momento de buscar os klines (segundo 1 de cada hora)
        if last_fetched_hour != current_hour:
            # Buscar os últimos dados de kline de 1 hora
            df = get_historical_klines_and_append(symbol, interval=60)
            if df is None or df.empty:
                logging.error("Failed to fetch historical klines or received empty data.")
                time.sleep(10)
                continue
            df = df.sort_values('timestamp')

            # Calcular indicadores
            df = calculate_indicators(df)
            if df is None:
                logging.error("Failed to calculate indicators.")
                time.sleep(10)
                continue

            # Obter a última linha do DataFrame para usar nos cálculos atuais
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

            # Determinar se o mercado está em tendência
            trendingMarket = adx.iloc[-1] >= 12  # adxThreshold

            # Detectar transição para mercado lateral ou de tendência
            if previous_isLateral is not None and isLateral.iloc[-1] != previous_isLateral:
                if isLateral.iloc[-1]:
                    # Entrou no mercado lateral
                    logging.info("Entered lateral market. Stopgain and Stoploss levels adjusted.")
                    logging.info(f"Stopgain and Stoploss levels for lateral market - Long: Stopgain {stopgain_lateral_long}, Stoploss {stoploss_lateral_long}; Short: Stopgain {stopgain_lateral_short}, Stoploss {stoploss_lateral_short}")
                else:
                    # Saiu do mercado lateral
                    logging.info("Exited lateral market. Stopgain and Stoploss levels adjusted.")
                    logging.info(f"Stopgain and Stoploss levels for trending market - Long: Stopgain {stopgain_normal_long}, Stoploss {stoploss_normal_long}; Short: Stopgain {stopgain_normal_short}, Stoploss {stoploss_normal_short}")
                # Atualizar stop_loss e stop_gain secundários se houver uma posição aberta
                if current_trade_id is not None:
                    if isLateral.iloc[-1]:
                        # Agora está no mercado lateral
                        if current_position_side == 'buy':
                            current_secondary_stop_loss = entry_price * stoploss_lateral_long
                            current_secondary_stop_gain = entry_price * stopgain_lateral_long
                        elif current_position_side == 'sell':
                            current_secondary_stop_loss = entry_price * stoploss_lateral_short
                            current_secondary_stop_gain = entry_price * stopgain_lateral_short
                    else:
                        # Agora está no mercado de tendência
                        if current_position_side == 'buy':
                            current_secondary_stop_loss = entry_price * stoploss_normal_long
                            current_secondary_stop_gain = entry_price * stopgain_normal_long
                        elif current_position_side == 'sell':
                            current_secondary_stop_loss = entry_price * stoploss_normal_short
                            current_secondary_stop_gain = entry_price * stopgain_normal_short
                    # Atualizar o CSV
                    update_data = {
                        'secondary_stop_loss': current_secondary_stop_loss,
                        'secondary_stop_gain': current_secondary_stop_gain
                    }
                    log_trade_update(current_trade_id, symbol, update_data)

            previous_isLateral = isLateral.iloc[-1]

            # Atualizar a última hora de busca
            last_fetched_hour = current_hour

            logging.info(f"Indicators updated at {current_time}")

        # Logar o status do bot a cada 10 minutos
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

        # Implementar lógica de entrada e saída em tempo real baseada no latest_price e indicadores
        # Condições de Long e Short
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

        # Obter posição atual
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

        # Espera de 1 segundo antes da próxima verificação
        time.sleep(1)

    except KeyboardInterrupt:
        logging.info("Bot stopped manually.")
        break
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        logging.error(traceback.format_exc())
        time.sleep(5)  # Brief pause before retrying
        continue