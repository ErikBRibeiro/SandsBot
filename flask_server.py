from flask import Flask, request, jsonify
from pybit.unified_trading import HTTP
import time
import logging
import numpy as np
import os
import sys
from dotenv import load_dotenv, find_dotenv

app = Flask(__name__)

# Configuração básica de logging para o console
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

# Carregar as variáveis de ambiente do arquivo .env
load_dotenv(find_dotenv())

# Chave secreta para autenticação de Webhook
SECRET_KEY = os.getenv('SECRET_KEY', '1221')  # Padrão para '1221' se não estiver definido

# Dicionário para armazenar as sessões da API
api_sessions = {}

# Lista de contas na ordem desejada
accounts_order = ['ERIK', 'NATAN']

# Criar sessões da API para cada conta
for account in accounts_order:
    api_key = os.getenv(f'BYBIT_API_KEY_{account}')
    api_secret = os.getenv(f'BYBIT_API_SECRET_{account}')
    
    if not api_key or not api_secret:
        logging.error(f"API_KEY and/or API_SECRET not found for account {account}. Please check your .env file.")
        sys.exit(1)
    
    # Inicializar a sessão da API
    session = HTTP(
        api_key=api_key,
        api_secret=api_secret,
        # testnet=True  # Descomente esta linha se estiver usando a Testnet
    )
    
    # Armazenar a sessão no dicionário
    api_sessions[account] = session

def get_usdt_balance(session):
    try:
        response = session.get_wallet_balance(accountType='UNIFIED', coin='USDT')
        if response['retCode'] == 0:
            coin_list = response['result']['list'][0]['coin']
            for coin in coin_list:
                if coin['coin'] == 'USDT':
                    usdt_balance = float(coin['walletBalance'])
                    logging.info(f"Saldo USDT disponível: {usdt_balance}")
                    return usdt_balance
            # Se USDT não for encontrado
            logging.error("USDT não encontrado na lista de moedas.")
            return 0.0
        else:
            logging.error(f"Erro ao obter saldo: {response['retMsg']}")
            return 0.0
    except Exception as e:
        logging.error(f"Erro ao obter saldo: {e}")
        return 0.0

def get_current_price(symbol='BTCUSDT'):
    try:
        # Usamos a sessão da primeira conta apenas para obter o preço
        session = list(api_sessions.values())[0]
        response = session.get_tickers(
            category='linear',
            symbol=symbol
        )
        if response['retCode'] == 0:
            price = float(response['result']['list'][0]['lastPrice'])
            return price
        else:
            logging.error(f"Erro ao obter preço: {response['retMsg']}")
            return 0.0
    except Exception as e:
        logging.error(f"Erro ao obter preço: {e}")
        return 0.0

def calculate_qty(usdt_balance, price, leverage=1):
    if price == 0:
        return 0
    qty = usdt_balance * leverage / price
    qty = np.floor(qty * 1000) / 1000  # Ajusta para 3 casas decimais
    return str(qty)

def get_current_position(session, symbol='BTCUSDT'):
    try:
        response = session.get_positions(
            category='linear',
            symbol=symbol
        )
        if response['retCode'] == 0:
            positions = response['result']['list']
            for pos in positions:
                if pos['symbol'] == symbol and float(pos['size']) > 0:
                    return pos
            return None
        else:
            logging.error(f"Erro ao obter posição: {response['retMsg']}")
            return None
    except Exception as e:
        logging.error(f"Erro ao obter posição: {e}")
        return None

def close_position(session, position):
    try:
        if position['side'] == 'Buy':
            side = 'Sell'
        elif position['side'] == 'Sell':
            side = 'Buy'
        else:
            logging.warning("Lado da posição desconhecido.")
            return

        order = session.place_order(
            category='linear',
            symbol=position['symbol'],
            side=side,
            orderType='Market',
            qty=position['size'],
            timeInForce='GTC',
            reduceOnly=True
        )
        if order['retCode'] == 0:
            logging.info(f"Posição {position['side']} fechada com sucesso.")
        else:
            logging.error(f"Erro ao fechar posição: {order['retMsg']}")
    except Exception as e:
        logging.error(f"Erro ao fechar posição: {e}")

def open_position(session, action, symbol='BTCUSDT', leverage=1):
    try:
        usdt_balance = get_usdt_balance(session)
        price = get_current_price(symbol)
        if price == 0.0:
            logging.error("Preço inválido. Abortando operação.")
            return

        qty = calculate_qty(usdt_balance, price, leverage)
        if float(qty) <= 0:
            logging.error("Quantidade calculada inválida. Abortando operação.")
            return

        side = 'Buy' if action == 'long' else 'Sell'
        order = session.place_order(
            category='linear',
            symbol=symbol,
            side=side,
            orderType='Market',
            qty=qty,
            timeInForce='GTC',
            reduceOnly=False
        )
        if order['retCode'] == 0:
            logging.info(f"Ordem de {side} executada com sucesso. Qty: {qty}")
        else:
            logging.error(f"Erro ao executar ordem: {order['retMsg']}")
    except Exception as e:
        logging.error(f"Erro ao executar ordem: {e}")

@app.route('/webhook', methods=['POST'])
def webhook():
    start_time = time.time()
    
    data = request.get_json()
    logging.info(f"Dados recebidos: {data}")

    # Verificação de autenticação
    received_secret = data.get('secret')
    if received_secret != SECRET_KEY:
        logging.warning("Autenticação falhou.")
        return jsonify({'message': 'Autenticação falhou'}), 403

    action = data.get('action')
    symbol = data.get('symbol', 'BTCUSDT')  # Pode ajustar conforme necessário
    leverage = 1  # Alavancagem será sempre 1

    if action not in ['long', 'short', 'exit']:
        logging.warning("Ação inválida recebida.")
        return jsonify({'message': 'Ação inválida'}), 400

    # Iterar sobre as contas na ordem desejada
    for account in accounts_order:
        logging.info(f"Processando ação para a conta: {account}")
        session = api_sessions[account]

        # Obter a posição atual para esta conta
        position = get_current_position(session, symbol)

        if action == 'long':
            if position:
                if position['side'] == 'Sell':
                    # Fechar posição short antes de abrir long
                    close_position(session, position)
                    open_position(session, 'long', symbol, leverage)
                elif position['side'] == 'Buy':
                    logging.info(f"Conta {account}: Já está em posição long. Nenhuma ação necessária.")
                else:
                    logging.warning(f"Conta {account}: Lado da posição desconhecido.")
            else:
                # Nenhuma posição aberta, abrir long
                open_position(session, 'long', symbol, leverage)

        elif action == 'short':
            if position:
                if position['side'] == 'Buy':
                    # Fechar posição long antes de abrir short
                    close_position(session, position)
                    open_position(session, 'short', symbol, leverage)
                elif position['side'] == 'Sell':
                    logging.info(f"Conta {account}: Já está em posição short. Nenhuma ação necessária.")
                else:
                    logging.warning(f"Conta {account}: Lado da posição desconhecido.")
            else:
                # Nenhuma posição aberta, abrir short
                open_position(session, 'short', symbol, leverage)

        elif action == 'exit':
            if position:
                # Fechar posição aberta
                close_position(session, position)
                logging.info(f"Conta {account}: Posição fechada com sucesso.")
            else:
                logging.info(f"Conta {account}: Nenhuma posição aberta para fechar.")

    end_time = time.time()
    latency = end_time - start_time
    logging.info(f"Latência total: {latency:.3f} segundos")

    return jsonify({'message': 'Ação executada', 'latency': latency}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
