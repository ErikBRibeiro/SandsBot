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

# Função para converter timestamp Unix para datetime
def unix_to_datetime(unix_time):
    return datetime.utcfromtimestamp(unix_time)

# **Modificação: Adicionar a linha inicial com indicadores calculados**
# Função para adicionar a linha inicial com os indicadores calculados
def adicionar_linha_inicial(df):
    # Linha fornecida com indicadores já calculados
    linha_inicial = {
        'time': 1726650000,
        'open': 60172.4,
        'high': 60172.4,
        'low': 59630,
        'close': 59630,
        'Upper Band': 60580.68472207522,
        'Lower Band': 59880.35813506749,
        'Middle Band': 60230.52142857135,
        'EMA Curta (21)': 60162.58945705477,
        'EMA Longa (55)': 59553.543958859795,
        'ADX': 21.402484453635605,
        'ADX Plus': 21.174773194781164,
        'ADX Minus': 23.277589391454462,
        'RSI': 49.67691640584306,
        'MACD Line': 345.8235424582599,
        'Signal Line': 418.44656574649,
        'MACD Histogram': -72.62302328823012,
        'BandWidth': 0.011627436894071428
    }
    
    # Converter para DataFrame
    df_linha_inicial = pd.DataFrame([linha_inicial])
    # Converter 'time' para datetime
    df_linha_inicial['timestamp'] = pd.to_datetime(df_linha_inicial['time'], unit='s')
    
    # Adicionar as colunas dos indicadores ao DataFrame se não existirem
    if not all(col in df.columns for col in df_linha_inicial.columns):
        df = pd.concat([df_linha_inicial, df], ignore_index=True)
        logging.info("Linha inicial com indicadores calculados adicionada ao DataFrame de candles.")
    else:
        logging.info("Linha inicial já está presente no DataFrame de candles.")

    # Verificar colunas após a adição da linha inicial
    logging.info(f"Colunas do DataFrame após adicionar a linha inicial: {df.columns}")
    return df


# Função para buscar dados históricos de klines
def get_historical_klines(symbol, interval, limit):
    try:
        now = datetime.utcnow()
        from_time = now - timedelta(hours=limit)  # Intervalo de 1 hora
        from_time_ms = int(from_time.timestamp() * 1000)
        kline = session.get_kline(
            category='linear',
            symbol=symbol,
            interval=str(interval),
            start=str(from_time_ms),
            limit=limit
        )
        if kline['retMsg'] != 'OK':
            logging.error(f"Erro ao buscar dados de kline: {kline['retMsg']}")
            return None
        kline_data = kline['result']['list']
        df = pd.DataFrame(kline_data, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'
        ])
        
        # Garantir que 'timestamp' seja numérico antes de converter para datetime
        df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)
        df['turnover'] = df['turnover'].astype(float)
        
        # Adicionar a coluna 'time'
        df['time'] = df['timestamp'].apply(lambda x: int(x.timestamp()))
        
        # Adicionar a linha inicial com os indicadores calculados
        df = adicionar_linha_inicial(df)

        return df
    except Exception as e:
        logging.error(f"Exception em get_historical_klines: {e}")
        return None

# Função para calcular as EMAs a partir de valores iniciais
def calcular_ema(series, period, valor_inicial):
    alpha = 2 / (period + 1)
    ema = [valor_inicial]
    for price in series.iloc[1:]:
        new_ema = (price * alpha) + (ema[-1] * (1 - alpha))
        ema.append(new_ema)
    return pd.Series(ema, index=series.index)

# Função para calcular indicadores
def calculate_indicators(df):
    try:
        close_price = df['close']
        high_price = df['high']
        low_price = df['low']

        # Parâmetros
        emaShortLength = 21  # EMA Curta (21)
        emaLongLength = 55   # EMA Longa (55)
        rsiLength = 14
        macdShort = 12
        macdLong = 26
        macdSignal = 9
        adxLength = 14
        adxSmoothing = 14
        bbLength = 20
        bbMultiplier = 2
        lateralThreshold = 0.005

        # Calculando EMAs a partir dos valores iniciais fornecidos
        valor_inicial_ema_curta = df['EMA Curta (21)'].iloc[0]
        valor_inicial_ema_longa = df['EMA Longa (55)'].iloc[0]

        emaShort = calcular_ema(close_price, emaShortLength, valor_inicial_ema_curta)
        emaLong = calcular_ema(close_price, emaLongLength, valor_inicial_ema_longa)

        # RSI
        rsi = talib.RSI(close_price, timeperiod=rsiLength)

        # MACD
        macdLine, signalLine, macdHist = talib.MACD(close_price, fastperiod=macdShort, slowperiod=macdLong, signalperiod=macdSignal)

        # Bandas de Bollinger
        upperBand, middleBand, lowerBand = talib.BBANDS(
            close_price, timeperiod=bbLength, nbdevup=bbMultiplier, nbdevdn=bbMultiplier, matype=0
        )

        # ADX
        adx = talib.ADX(high_price, low_price, close_price, timeperiod=adxLength)
        plus_di = talib.PLUS_DI(high_price, low_price, close_price, timeperiod=adxLength)
        minus_di = talib.MINUS_DI(high_price, low_price, close_price, timeperiod=adxLength)

        # Bandwidth e isLateral
        bandWidth = (upperBand - lowerBand) / middleBand
        isLateral = bandWidth < lateralThreshold

        # Atribuir aos DataFrame
        df['EMA Curta (21)'] = emaShort
        df['EMA Longa (55)'] = emaLong
        df['RSI'] = rsi
        df['MACD Line'] = macdLine
        df['Signal Line'] = signalLine
        df['MACD Histogram'] = macdHist
        df['Upper Band'] = upperBand
        df['Middle Band'] = middleBand
        df['Lower Band'] = lowerBand
        df['BandWidth'] = bandWidth
        df['isLateral'] = isLateral
        df['ADX'] = adx
        df['ADX Plus'] = plus_di
        df['ADX Minus'] = minus_di

        return df
    except Exception as e:
        logging.error(f"Exception em calculate_indicators: {e}")
        return None

# Funções auxiliares para lógica de crossover
def crossover(series1, series2):
    try:
        cross = (series1 > series2) & (series1.shift(1) <= series2.shift(1))
        return cross
    except Exception as e:
        logging.error(f"Exception na função crossover: {e}")
        return pd.Series([False])

def crossunder(series1, series2):
    try:
        cross = (series1 < series2) & (series1.shift(1) >= series2.shift(1))
        return cross
    except Exception as e:
        logging.error(f"Exception na função crossunder: {e}")
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

# Função para buscar o preço mais recente
def get_latest_price():
    try:
        ticker = session.get_tickers(
            category='linear',
            symbol=symbol
        )
        if ticker['retMsg'] != 'OK':
            logging.error(f"Erro ao buscar preço mais recente: {ticker['retMsg']}")
            return None
        price = float(ticker['result']['list'][0]['lastPrice'])
        return price
    except Exception as e:
        logging.error(f"Exception em get_latest_price: {e}")
        return None

# Função para obter o saldo da conta
def get_account_balance():
    try:
        balance_info = session.get_wallet_balance(accountType='UNIFIED')
        if balance_info['retMsg'] != 'OK':
            logging.error(f"Erro ao buscar saldo da conta: {balance_info['retMsg']}")
            return None
        
        # Acessando o totalEquity corretamente na lista dentro de result
        total_equity = float(balance_info['result']['list'][0]['totalEquity'])
        return total_equity
    except Exception as e:
        logging.error(f"Exception em get_account_balance: {e}")
        return None

# Funções para logar entradas e saídas de trades
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
        logging.error(f"Exception em log_trade_entry: {e}")

def log_trade_update(trade_id, symbol, update_data):
    try:
        if not os.path.isfile(trade_history_file):
            logging.error("Arquivo de histórico de trades não existe.")
            return
        df_trade_history = pd.read_csv(trade_history_file)
        mask = (df_trade_history['trade_id'] == trade_id) & (df_trade_history['symbol'] == symbol)
        if mask.any():
            for key, value in update_data.items():
                df_trade_history.loc[mask, key] = value
            df_trade_history.to_csv(trade_history_file, index=False)
        else:
            logging.error("Trade não encontrado no histórico para atualização.")
    except Exception as e:
        logging.error(f"Exception em log_trade_update: {e}")

def log_trade_exit(trade_id, symbol, update_data, exit_lateral):
    try:
        if not os.path.isfile(trade_history_file):
            logging.error("Arquivo de histórico de trades não existe.")
            return
        df_trade_history = pd.read_csv(trade_history_file)
        mask = (df_trade_history['trade_id'] == trade_id) & (df_trade_history['symbol'] == symbol)
        if mask.any():
            update_data['exit_lateral'] = exit_lateral  # Adiciona 'exit_lateral'
            for key, value in update_data.items():
                df_trade_history.loc[mask, key] = value
            df_trade_history.to_csv(trade_history_file, index=False)
        else:
            logging.error("Trade não encontrado no histórico para saída.")
    except Exception as e:
        logging.error(f"Exception em log_trade_exit: {e}")

def calculate_qty(total_equity, latest_price, leverage=1):
    try:
        # Calcular o número de contratos
        qty = total_equity / latest_price
        factor = 100
        # Ajustar para a precisão permitida pela Bybit (exemplo: 2 casas decimais)
        qty = math.floor(qty * factor) / factor
        
        return qty
    except Exception as e:
        logging.error(f"Exception em calculate_qty: {e}")
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

# Parâmetros de trading
stopgain_lateral_long = 1.11
stoploss_lateral_long = 0.973
stopgain_lateral_short = 0.973
stoploss_lateral_short = 1.09
stopgain_normal_long = 1.32
stoploss_normal_long = 0.92
stopgain_normal_short = 0.77
stoploss_normal_short = 1.12
adxThreshold = 12
trade_count = 0  # Contador de trades

# Inicializar variáveis
last_candle_time = None  # Para rastrear quando atualizar indicadores
last_log_time = None     # Para rastrear logs a cada 10 minutos
previous_isLateral = None  # Para detectar transição para mercado lateral

# Variáveis para rastrear o trade atual
current_trade_id = None
current_position_side = None
entry_price = None
current_secondary_stop_loss = None
current_secondary_stop_gain = None
entry_commission = 0
exit_commission = 0

# Função principal de trading
while True:
    try:
        current_time = datetime.utcnow()
        # Verificar se é hora de atualizar os indicadores (a cada hora)
        if last_candle_time is None or (current_time - last_candle_time).seconds >= 3600:
            # Buscar os últimos 1000 candles de 1 hora
            df = get_historical_klines(symbol, interval=60, limit=1000)
            if df is None or df.empty:
                logging.error("Falha ao buscar candles históricos ou dados recebidos vazios.")
                time.sleep(10)
                continue
            df = df.sort_values('timestamp')

            # Calcular indicadores
            df = calculate_indicators(df)
            if df is None:
                logging.error("Falha ao calcular indicadores.")
                time.sleep(10)
                continue

            # Obter a última linha de dados
            last_row = df.iloc[-1]
            adjusted_timestamp = last_row['timestamp']
            emaShort = df['EMA Curta (21)']
            emaLong = df['EMA Longa (55)']
            rsi = df['RSI']
            macdHist = df['MACD Histogram']
            adx = df['ADX']
            isLateral = df['isLateral']
            upperBand = df['Upper Band']
            lowerBand = df['Lower Band']
            bandWidth = df['BandWidth']

            # Determinar mercado em tendência
            trendingMarket = adx.iloc[-1] >= adxThreshold  # adxThreshold

            # Detectar transição para mercado lateral
            if previous_isLateral is not None and isLateral.iloc[-1] != previous_isLateral:
                if isLateral.iloc[-1]:
                    # Entrou no mercado lateral
                    logging.info("Entrou no mercado lateral. Níveis de Stopgain e Stoploss ajustados.")
                    logging.info(f"Stopgain e Stoploss para mercado lateral - Long: Stopgain {stopgain_lateral_long}, Stoploss {stoploss_lateral_long}; Short: Stopgain {stopgain_lateral_short}, Stoploss {stoploss_lateral_short}")
                else:
                    # Saiu do mercado lateral
                    logging.info("Saiu do mercado lateral. Níveis de Stopgain e Stoploss ajustados.")
                    logging.info(f"Stopgain e Stoploss para mercado em tendência - Long: Stopgain {stopgain_normal_long}, Stoploss {stoploss_normal_long}; Short: Stopgain {stopgain_normal_short}, Stoploss {stoploss_normal_short}")
                # Atualizar secondary stop_loss e stop_gain se houver uma posição aberta
                if current_trade_id is not None:
                    if isLateral.iloc[-1]:
                        # Agora no mercado lateral
                        if current_position_side == 'buy':
                            current_secondary_stop_loss = entry_price * stoploss_lateral_long
                            current_secondary_stop_gain = entry_price * stopgain_lateral_long
                        elif current_position_side == 'sell':
                            current_secondary_stop_loss = entry_price * stoploss_lateral_short
                            current_secondary_stop_gain = entry_price * stopgain_lateral_short
                    else:
                        # Agora no mercado em tendência
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

            # Atualizar last_candle_time
            last_candle_time = current_time

            logging.info(f"Indicadores atualizados em {current_time}")

        # Logar status do bot a cada 10 minutos
        if last_log_time is None or (current_time - last_log_time).total_seconds() >= 600:
            current_position, position_info = get_current_position()
            if current_position is None:
                logging.info("Status do bot: Incapaz de buscar posição atual.")
            elif not current_position:
                logging.info("Status do bot: Fora de trade.")
            else:
                side = position_info['side']
                entry_price = position_info['entry_price']
                size = position_info['size']
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
                logging.info(f"Status do bot: Em uma posição {side.lower()}.")
                logging.info(f"Stoploss Atual: {stop_loss:.2f}, Take Profit: {take_profit:.2f}")
                logging.info(f'EMA Curta (21): {df["EMA Curta (21)"].iloc[-1]}')
                logging.info(f'EMA Longa (55): {df["EMA Longa (55)"].iloc[-1]}')
                pd.set_option("display.max_columns", None)
                logging.info(f'DF COMPLETO:\n{df}')
            last_log_time = current_time

        # Buscar o preço mais recente a cada segundo
        latest_price = get_latest_price()
        if latest_price is None:
            logging.error("Falha ao buscar preço mais recente.")
            time.sleep(5)
            continue

        # Implementar lógica de entrada e saída em tempo real com base no latest_price e indicadores
        # Condições para Long e Short
        longCondition = (
            crossover(df['EMA Curta (21)'], df['EMA Longa (55)']).iloc[-1]
            and (df['RSI'].iloc[-1] < 60)
            and (df['MACD Histogram'].iloc[-1] > 0.5)
            and trendingMarket
        )
        shortCondition = (
            crossunder(df['EMA Curta (21)'], df['EMA Longa (55)']).iloc[-1]
            and (df['RSI'].iloc[-1] > 40)
            and (df['MACD Histogram'].iloc[-1] < -0.5)
            and trendingMarket
        )

        # Obter posição atual
        current_position, position_info = get_current_position()
        if current_position is None:
            logging.info("Falha ao buscar posição atual.")
            time.sleep(5)
            continue

        # Implementar lógica de trading
        if not current_position:

            if isLateral.iloc[-1]:
                # Estratégia de Reversão à Média no Mercado Lateral
                if (latest_price < df['Lower Band'].iloc[-1]) and longCondition:
                    # Abrir posição longa
                    total_equity = get_account_balance()
                    if total_equity is None:
                        logging.error("Falha ao buscar saldo da conta.")
                        time.sleep(5)
                        continue
                    qty = calculate_qty(total_equity, latest_price)  # Definir tamanho da posição
                    
                    if qty is None:
                        logging.error("Falha ao calcular a quantidade.")
                        time.sleep(5)
                        continue
                    
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
                            logging.error(f"Erro ao colocar ordem de compra: {order['retMsg']}")
                            time.sleep(5)
                            continue
                    except Exception as e:
                        logging.error(f"Exception ao colocar ordem de compra: {e}")
                        time.sleep(5)
                        continue
                    
                    trade_id = datetime.utcnow().isoformat()
                    current_trade_id = trade_id
                    current_position_side = 'buy'
                    old_balance = total_equity
                    entry_price = latest_price
                    stop_loss = entry_price * stoploss_lateral_long
                    stop_gain = entry_price * stopgain_lateral_long
                    secondary_stop_loss = stop_loss
                    secondary_stop_gain = stop_gain
                    current_secondary_stop_loss = secondary_stop_loss
                    current_secondary_stop_gain = secondary_stop_gain
                    commission_rate = 0.00032  # 0.032%
                    entry_commission = old_balance * commission_rate
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
                        'commission': entry_commission,
                        'old_balance': old_balance - entry_commission,  # Subtrai comissão
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
                    logging.info(f"Entrou em posição longa em {trade_id}, preço: {entry_price}")
                    logging.info(f"Stoploss definido em {stop_loss:.2f}, Take Profit definido em {stop_gain:.2f}")
                    trade_count += 1
                elif (latest_price > df['Upper Band'].iloc[-1]) and shortCondition:
                    # Abrir posição curta
                    total_equity = get_account_balance()
                    if total_equity is None:
                        logging.error("Falha ao buscar saldo da conta.")
                        time.sleep(5)
                        continue
                    qty = calculate_qty(total_equity, latest_price)  # Definir tamanho da posição
                    
                    if qty is None:
                        logging.error("Falha ao calcular a quantidade.")
                        time.sleep(5)
                        continue
                    
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
                            logging.error(f"Erro ao colocar ordem de venda: {order['retMsg']}")
                            time.sleep(5)
                            continue
                    except Exception as e:
                        logging.error(f"Exception ao colocar ordem de venda: {e}")
                        time.sleep(5)
                        continue
                    
                    trade_id = datetime.utcnow().isoformat()
                    current_trade_id = trade_id
                    current_position_side = 'sell'
                    old_balance = total_equity
                    entry_price = latest_price
                    stop_loss = entry_price * stoploss_lateral_short
                    stop_gain = entry_price * stopgain_lateral_short
                    secondary_stop_loss = stop_loss
                    secondary_stop_gain = stop_gain
                    current_secondary_stop_loss = secondary_stop_loss
                    current_secondary_stop_gain = secondary_stop_gain
                    commission_rate = 0.00032  # 0.032%
                    entry_commission = old_balance * commission_rate
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
                        'commission': entry_commission,
                        'old_balance': old_balance - entry_commission,  # Subtrai comissão
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
                    logging.info(f"Entrou em posição curta em {trade_id}, preço: {entry_price}")
                    logging.info(f"Stoploss definido em {stop_loss:.2f}, Take Profit definido em {stop_gain:.2f}")
                    trade_count += 1

            else:
                # Estratégia de Seguimento de Tendência no Mercado em Tendência
                if longCondition:
                    # Abrir posição longa
                    total_equity = get_account_balance()
                    if total_equity is None:
                        logging.error("Falha ao buscar saldo da conta.")
                        time.sleep(5)
                        continue
                    qty = calculate_qty(total_equity, latest_price)  # Definir tamanho da posição
                    
                    if qty is None:
                        logging.error("Falha ao calcular a quantidade.")
                        time.sleep(5)
                        continue
                    
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
                            logging.error(f"Erro ao colocar ordem de compra: {order['retMsg']}")
                            time.sleep(5)
                            continue
                    except Exception as e:
                        logging.error(f"Exception ao colocar ordem de compra: {e}")
                        time.sleep(5)
                        continue
                    
                    trade_id = datetime.utcnow().isoformat()
                    current_trade_id = trade_id
                    current_position_side = 'buy'
                    old_balance = total_equity
                    entry_price = latest_price
                    stop_loss = entry_price * stoploss_normal_long
                    stop_gain = entry_price * stopgain_normal_long
                    secondary_stop_loss = stop_loss
                    secondary_stop_gain = stop_gain
                    current_secondary_stop_loss = secondary_stop_loss
                    current_secondary_stop_gain = secondary_stop_gain
                    commission_rate = 0.00032  # 0.032%
                    entry_commission = old_balance * commission_rate
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
                        'commission': entry_commission,
                        'old_balance': old_balance - entry_commission,  # Subtrai comissão
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
                    logging.info(f"Entrou em posição longa em {trade_id}, preço: {entry_price}")
                    logging.info(f"Stoploss definido em {stop_loss:.2f}, Take Profit definido em {stop_gain:.2f}")
                    trade_count += 1
                elif shortCondition:
                    # Abrir posição curta
                    total_equity = get_account_balance()
                    if total_equity is None:
                        logging.error("Falha ao buscar saldo da conta.")
                        time.sleep(5)
                        continue
                    qty = calculate_qty(total_equity, latest_price)  # Definir tamanho da posição
                    
                    if qty is None:
                        logging.error("Falha ao calcular a quantidade.")
                        time.sleep(5)
                        continue
                    
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
                            logging.error(f"Erro ao colocar ordem de venda: {order['retMsg']}")
                            time.sleep(5)
                            continue
                    except Exception as e:
                        logging.error(f"Exception ao colocar ordem de venda: {e}")
                        time.sleep(5)
                        continue
                    
                    trade_id = datetime.utcnow().isoformat()
                    current_trade_id = trade_id
                    current_position_side = 'sell'
                    old_balance = total_equity
                    entry_price = latest_price
                    stop_loss = entry_price * stoploss_normal_short
                    stop_gain = entry_price * stopgain_normal_short
                    secondary_stop_loss = stop_loss
                    secondary_stop_gain = stop_gain
                    current_secondary_stop_loss = secondary_stop_loss
                    current_secondary_stop_gain = secondary_stop_gain
                    commission_rate = 0.00032  # 0.032%
                    entry_commission = old_balance * commission_rate
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
                        'commission': entry_commission,
                        'old_balance': old_balance - entry_commission,  # Subtrai comissão
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
                    logging.info(f"Entrou em posição curta em {trade_id}, preço: {entry_price}")
                    logging.info(f"Stoploss definido em {stop_loss:.2f}, Take Profit definido em {stop_gain:.2f}")
                    trade_count += 1

        else:
            # Gerenciar posição aberta
            side = position_info['side']
            entry_price = position_info['entry_price']
            size = position_info['size']
            total_equity = get_account_balance()
            if total_equity is None:
                logging.error("Falha ao buscar saldo da conta.")
                total_equity = 0
            commission_rate = 0.00032  # 0.032%
            if isLateral.iloc[-1]:
                # Condições de saída no mercado lateral
                if side == 'buy':
                    # Posição Longa
                    stop_loss = entry_price * stoploss_lateral_long
                    take_profit = entry_price * stopgain_lateral_long
                    if latest_price <= stop_loss or shortCondition:
                        # Fechar posição no Stop Loss ou reversão
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
                                logging.error(f"Erro ao fechar posição longa: {order['retMsg']}")
                                time.sleep(5)
                                continue
                        except Exception as e:
                            logging.error(f"Exception ao fechar posição longa: {e}")
                            time.sleep(5)
                            continue
                        
                        sell_price = latest_price
                        exit_commission = total_equity * commission_rate
                        new_balance = total_equity - exit_commission
                        sell_time = datetime.utcnow().isoformat()
                        total_commission = entry_commission + exit_commission
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
                        logging.info(f"Fechou posição longa em {sell_time}, preço: {sell_price}")
                        # Resetar variáveis de tracking
                        current_trade_id = None
                        current_position_side = None
                        entry_price = None
                        current_secondary_stop_loss = None
                        current_secondary_stop_gain = None
                        entry_commission = 0
                        exit_commission = 0
                    elif latest_price >= take_profit:
                        # Fechar posição no Take Profit
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
                                logging.error(f"Erro ao fechar posição longa: {order['retMsg']}")
                                time.sleep(5)
                                continue
                        except Exception as e:
                            logging.error(f"Exception ao fechar posição longa: {e}")
                            time.sleep(5)
                            continue
                        
                        sell_price = latest_price
                        exit_commission = total_equity * commission_rate
                        new_balance = total_equity - exit_commission
                        sell_time = datetime.utcnow().isoformat()
                        total_commission = entry_commission + exit_commission
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
                        logging.info(f"Fechou posição longa em {sell_time}, preço: {sell_price}")
                        # Resetar variáveis de tracking
                        current_trade_id = None
                        current_position_side = None
                        entry_price = None
                        current_secondary_stop_loss = None
                        current_secondary_stop_gain = None
                        entry_commission = 0
                        exit_commission = 0
                elif side == 'sell':
                    # Posição Curta
                    stop_loss = entry_price * stoploss_lateral_short
                    take_profit = entry_price * stopgain_lateral_short
                    if latest_price >= stop_loss or longCondition:
                        # Fechar posição no Stop Loss ou reversão
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
                                logging.error(f"Erro ao fechar posição curta: {order['retMsg']}")
                                time.sleep(5)
                                continue
                        except Exception as e:
                            logging.error(f"Exception ao fechar posição curta: {e}")
                            time.sleep(5)
                            continue
                        
                        sell_price = latest_price
                        exit_commission = total_equity * commission_rate
                        new_balance = total_equity - exit_commission
                        sell_time = datetime.utcnow().isoformat()
                        total_commission = entry_commission + exit_commission
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
                        logging.info(f"Fechou posição curta em {sell_time}, preço: {sell_price}")
                        # Resetar variáveis de tracking
                        current_trade_id = None
                        current_position_side = None
                        entry_price = None
                        current_secondary_stop_loss = None
                        current_secondary_stop_gain = None
                        entry_commission = 0
                        exit_commission = 0
                    elif latest_price <= take_profit:
                        # Fechar posição no Take Profit
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
                                logging.error(f"Erro ao fechar posição curta: {order['retMsg']}")
                                time.sleep(5)
                                continue
                        except Exception as e:
                            logging.error(f"Exception ao fechar posição curta: {e}")
                            time.sleep(5)
                            continue
                        
                        sell_price = latest_price
                        exit_commission = total_equity * commission_rate
                        new_balance = total_equity - exit_commission
                        sell_time = datetime.utcnow().isoformat()
                        total_commission = entry_commission + exit_commission
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
                        logging.info(f"Fechou posição curta em {sell_time}, preço: {sell_price}")
                        # Resetar variáveis de tracking
                        current_trade_id = None
                        current_position_side = None
                        entry_price = None
                        current_secondary_stop_loss = None
                        current_secondary_stop_gain = None
                        entry_commission = 0
                        exit_commission = 0
            else:
                # Condições de saída no mercado em tendência
                if side == 'buy':
                    # Posição Longa
                    stop_loss = entry_price * stoploss_normal_long
                    take_profit = entry_price * stopgain_normal_long
                    if latest_price <= stop_loss or shortCondition:
                        # Fechar posição no Stop Loss ou reversão
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
                                logging.error(f"Erro ao fechar posição longa: {order['retMsg']}")
                                time.sleep(5)
                                continue
                        except Exception as e:
                            logging.error(f"Exception ao fechar posição longa: {e}")
                            time.sleep(5)
                            continue
                        
                        sell_price = latest_price
                        exit_commission = total_equity * commission_rate
                        new_balance = total_equity - exit_commission
                        sell_time = datetime.utcnow().isoformat()
                        total_commission = entry_commission + exit_commission
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
                        logging.info(f"Fechou posição longa em {sell_time}, preço: {sell_price}")
                        # Resetar variáveis de tracking
                        current_trade_id = None
                        current_position_side = None
                        entry_price = None
                        current_secondary_stop_loss = None
                        current_secondary_stop_gain = None
                        entry_commission = 0
                        exit_commission = 0
                    elif latest_price >= take_profit:
                        # Fechar posição no Take Profit
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
                                logging.error(f"Erro ao fechar posição longa: {order['retMsg']}")
                                time.sleep(5)
                                continue
                        except Exception as e:
                            logging.error(f"Exception ao fechar posição longa: {e}")
                            time.sleep(5)
                            continue
                        
                        sell_price = latest_price
                        exit_commission = total_equity * commission_rate
                        new_balance = total_equity - exit_commission
                        sell_time = datetime.utcnow().isoformat()
                        total_commission = entry_commission + exit_commission
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
                        logging.info(f"Fechou posição longa em {sell_time}, preço: {sell_price}")
                        # Resetar variáveis de tracking
                        current_trade_id = None
                        current_position_side = None
                        entry_price = None
                        current_secondary_stop_loss = None
                        current_secondary_stop_gain = None
                        entry_commission = 0
                        exit_commission = 0
                elif side == 'sell':
                    # Posição Curta
                    stop_loss = entry_price * stoploss_normal_short
                    take_profit = entry_price * stopgain_normal_short
                    if latest_price >= stop_loss or longCondition:
                        # Fechar posição no Stop Loss ou reversão
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
                                logging.error(f"Erro ao fechar posição curta: {order['retMsg']}")
                                time.sleep(5)
                                continue
                        except Exception as e:
                            logging.error(f"Exception ao fechar posição curta: {e}")
                            time.sleep(5)
                            continue
                        
                        sell_price = latest_price
                        exit_commission = total_equity * commission_rate
                        new_balance = total_equity - exit_commission
                        sell_time = datetime.utcnow().isoformat()
                        total_commission = entry_commission + exit_commission
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
                        logging.info(f"Fechou posição curta em {sell_time}, preço: {sell_price}")
                        # Resetar variáveis de tracking
                        current_trade_id = None
                        current_position_side = None
                        entry_price = None
                        current_secondary_stop_loss = None
                        current_secondary_stop_gain = None
                        entry_commission = 0
                        exit_commission = 0
                    elif latest_price <= take_profit:
                        # Fechar posição no Take Profit
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
                                logging.error(f"Erro ao fechar posição curta: {order['retMsg']}")
                                time.sleep(5)
                                continue
                        except Exception as e:
                            logging.error(f"Exception ao fechar posição curta: {e}")
                            time.sleep(5)
                            continue
                        
                        sell_price = latest_price
                        exit_commission = total_equity * commission_rate
                        new_balance = total_equity - exit_commission
                        sell_time = datetime.utcnow().isoformat()
                        total_commission = entry_commission + exit_commission
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
                        logging.info(f"Fechou posição curta em {sell_time}, preço: {sell_price}")
                        # Resetar variáveis de tracking
                        current_trade_id = None
                        current_position_side = None
                        entry_price = None
                        current_secondary_stop_loss = None
                        current_secondary_stop_gain = None
                        entry_commission = 0
                        exit_commission = 0

        # Esperar 1 segundo antes da próxima verificação de preço
        time.sleep(1)

    except KeyboardInterrupt:
        logging.info("Bot parado manualmente.")
        break
    except Exception as e:
        logging.error(f"Ocorreu um erro inesperado: {e}")
        logging.error(traceback.format_exc())
        time.sleep(5)  # Pausa breve antes de tentar novamente
        continue
