from flask import Flask, request, jsonify
from pybit.unified_trading import HTTP
import os
import time
import logging
import numpy as np

app = Flask(__name__)

# Configurações da API da Bybit
BYBIT_API_KEY = 'a4Ps0ivR7ErkobhWRn'
BYBIT_API_SECRET = 'BhCNOpL0ttkwhHpq0QryXVrPYdx7yJLVvGQ0'
# BYBIT_ENDPOINT = 'https://api.bybit.com'  # Verifique o endpoint correto na documentação da Bybit

# Chave secreta para autenticação de Webhook
SECRET_KEY = '1221'

# Inicializar a sessão da API
session = HTTP(
    api_key=BYBIT_API_KEY,
    api_secret=BYBIT_API_SECRET
)

# Configuração básica de logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

def get_usdt_balance():
    try:
        response = session.get_wallet_balance(coin='USDT')
        if response['ret_code'] == 0:
            usdt_balance = float(response['result']['USDT']['available_balance'])
            return usdt_balance
        else:
            logging.error(f"Erro ao obter saldo: {response['ret_msg']}")
            return 0.0
    except Exception as e:
        logging.error(f"Erro ao obter saldo: {e}")
        return 0.0

def get_current_price(symbol='BTCUSDT'):
    try:
        response = session.latest_information_for_symbol(symbol=symbol)
        if response['ret_code'] == 0:
            price = float(response['result'][0]['last_price'])
            return price
        else:
            logging.error(f"Erro ao obter preço: {response['ret_msg']}")
            return 0.0
    except Exception as e:
        logging.error(f"Erro ao obter preço: {e}")
        return 0.0

def calculate_qty(usdt_balance, price, leverage=1):
    """
    Calcula a quantidade de contratos baseando-se no saldo em USDT e no preço atual.
    """
    # Evitar dividir por zero
    if price == 0:
        return 0

    # Calcular quantidade de contratos
    qty = usdt_balance * leverage / price
    qty = np.floor(qty)  # Ajuste a precisão conforme necessário
    return qty

def get_current_position(symbol='BTCUSDT'):
    try:
        response = session.get_positions(symbol=symbol)
        if response['ret_code'] == 0:
            positions = response['result']['list']
            for pos in positions:
                if pos['symbol'] == symbol and pos['size'] > 0:
                    return pos
        return None
    except Exception as e:
        logging.error(f"Erro ao obter posição: {e}")
        return None

def close_position(position):
    try:
        if position['side'] == 'Buy':
            side = 'Sell'
        elif position['side'] == 'Sell':
            side = 'Buy'
        else:
            logging.warning("Lado da posição desconhecido.")
            return

        order = session.place_active_order(
            symbol=position['symbol'],
            side=side,
            order_type='Market',
            qty=position['size'],
            time_in_force='GoodTillCancel',
            reduce_only=True
        )
        if order['ret_code'] == 0:
            logging.info(f"Posição {position['side']} fechada com sucesso.")
        else:
            logging.error(f"Erro ao fechar posição: {order['ret_msg']}")
    except Exception as e:
        logging.error(f"Erro ao fechar posição: {e}")

def open_position(action, symbol='BTCUSDT', leverage=1):
    try:
        usdt_balance = get_usdt_balance()
        price = get_current_price(symbol)
        if price == 0.0:
            logging.error("Preço inválido. Abortando operação.")
            return
        
        qty = calculate_qty(usdt_balance, price, leverage)
        if qty <= 0:
            logging.error("Quantidade calculada inválida. Abortando operação.")
            return
        
        side = 'Buy' if action == 'long' else 'Sell'
        order = session.place_active_order(
            symbol=symbol,
            side=side,
            order_type='Market',
            qty=qty,
            time_in_force='GoodTillCancel',
            reduce_only=False
        )
        if order['ret_code'] == 0:
            logging.info(f"Ordem de {side} executada com sucesso. Qty: {qty}")
        else:
            logging.error(f"Erro ao executar ordem: {order['ret_msg']}")
    except Exception as e:
        logging.error(f"Erro ao executar ordem: {e}")

#@app.route('/', methods=['GET'])
#def welcome():
#    return jsonify({'message': 'HELLO'}), 200
@app.route('/', methods=['GET'])
def home():
    return jsonify({'message': 'API teste'}), 200

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
    leverage = int(data.get('leverage', 1))  # Alavancagem padrão

    if action not in ['long', 'short']:
        logging.warning("Ação inválida recebida.")
        return jsonify({'message': 'Ação inválida'}), 400

    # Obter a posição atual
    position = get_current_position(symbol)

    if action == 'long':
        if position:
            if position['side'] == 'Sell':
                # Fechar posição short antes de abrir long
                close_position(position)
                open_position('long', symbol, leverage)
            elif position['side'] == 'Buy':
                logging.info("Já está em posição long. Nenhuma ação necessária.")
            else:
                logging.warning("Lado da posição desconhecido.")
        else:
            # Nenhuma posição aberta, abrir long
            open_position('long', symbol, leverage)

    elif action == 'short':
        if position:
            if position['side'] == 'Buy':
                # Fechar posição long antes de abrir short
                close_position(position)
                open_position('short', symbol, leverage)
            elif position['side'] == 'Sell':
                logging.info("Já está em posição short. Nenhuma ação necessária.")
            else:
                logging.warning("Lado da posição desconhecido.")
        else:
            # Nenhuma posição aberta, abrir short
            open_position('short', symbol, leverage)

    end_time = time.time()
    latency = end_time - start_time
    logging.info(f"Latência total: {latency:.3f} segundos")

    return jsonify({'message': 'Ação executada', 'latency': latency}), 200

if __name__ == '__main__':
    # Se estiver rodando localmente, use ngrok para expor a porta 5000
    # Exemplo: ngrok http 5000
    # Certifique-se de configurar a URL do ngrok no TradingView
    app.run(host='0.0.0.0', port=5000)
