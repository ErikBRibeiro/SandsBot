from flask import Flask, request, jsonify
from pybit.unified_trading import HTTP
import time
import logging
import numpy as np

app = Flask(__name__)

# Configurações da API da Bybit
BYBIT_API_KEY = 'SEU_API_KEY'
BYBIT_API_SECRET = 'SEU_API_SECRET'

# Chave secreta para autenticação de Webhook
SECRET_KEY = '1221'

# Inicializar a sessão da API (adicionando testnet=True se estiver usando a Testnet)
session = HTTP(
    api_key=BYBIT_API_KEY,
    api_secret=BYBIT_API_SECRET,
    # testnet=True  # Descomente esta linha se estiver usando a Testnet
)

# Configuração básica de logging para o console
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

# Teste uma chamada simples, como obter o saldo
try:
    response = session.get_wallet_balance(coin='USDT')
    logging.info(response)
except Exception as e:
    logging.info(f"Erro ao obter saldo: {e}")

def get_usdt_balance():
    try:
        response = session.get_wallet_balance(coin='USDT')
        if response['retCode'] == 0:
            usdt_balance = float(response['result']['list'][0]['coin'][0]['availableToWithdraw'])
            return usdt_balance
        else:
            logging.error(f"Erro ao obter saldo: {response['retMsg']}")
            return 0.0
    except Exception as e:
        logging.error(f"Erro ao obter saldo: {e}")
        return 0.0

def get_current_price(symbol='BTCUSDT'):
    try:
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

def get_current_position(symbol='BTCUSDT'):
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

def close_position(position):
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

def open_position(action, symbol='BTCUSDT', leverage=1):
    try:
        usdt_balance = get_usdt_balance()
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
    app.run(host='0.0.0.0', port=5000)
