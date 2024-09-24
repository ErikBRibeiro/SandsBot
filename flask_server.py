from flask import Flask, request, jsonify
from pybit.unified_trading import HTTP
import time
import logging
import numpy as np
import os
import sys
import pandas as pd  # Importar pandas
from dotenv import load_dotenv, find_dotenv

app = Flask(__name__)

# Configuração básica de logging para o console
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

# Carregar as variáveis de ambiente do arquivo .env
load_dotenv(find_dotenv())

# Chave secreta para autenticação de Webhook
SECRET_KEY = os.getenv('SECRET_KEY', '1221')  # Padrão para '1221' se não estiver definido

# Lista de contas na ordem desejada
accounts_order = ['FERNANDO', 'ERIK', 'NATAN']

# Dicionário para armazenar as sessões da API e dados da conta
api_sessions = {}
account_data = {}

# Caminho para salvar o arquivo CSV
csv_file_path = '/app/data/trade_history.csv'

# Criar sessões da API para cada conta e inicializar dados
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
    
    # Inicializar dados da conta
    account_data[account] = {
        'entry_balance': None,
        'entry_time': None,
        'entry_price': None
    }

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

def write_to_csv(data_row):
    # Definir os nomes das colunas
    columns = ['api_owner', 'alert_time', 'action_time', 'type', 'btc_price', 'balance', 'outcome', 'PnL', 'latency']
    
    # Criar um DataFrame com a linha de dados
    df_new = pd.DataFrame([data_row], columns=columns)
    
    # Verificar se o arquivo CSV já existe
    if os.path.isfile(csv_file_path):
        # Ler o arquivo existente
        df_existing = pd.read_csv(csv_file_path)
        # Concatenar o novo registro
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        # Se não existe, o DataFrame combinado é apenas o novo registro
        df_combined = df_new
    
    try:
        # Salvar o DataFrame combinado no arquivo CSV
        df_combined.to_csv(csv_file_path, index=False)
    except Exception as e:
        logging.error(f"Erro ao escrever no arquivo CSV: {e}")

@app.route('/webhook', methods=['POST'])
def webhook():
    webhook_start_time = time.time()
    alert_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    
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
    
    # Obter o preço atual do BTC
    btc_price = get_current_price(symbol)
    if btc_price == 0.0:
        logging.error("Preço inválido. Abortando operação.")
        return jsonify({'message': 'Erro ao obter preço'}), 500
    
    # Iterar sobre as contas na ordem desejada
    for account in accounts_order:
        account_start_time = time.time()
        logging.info(f"Processando ação para a conta: {account}")
        session = api_sessions[account]
        
        # Obter o saldo antes da ação
        usdt_balance_before = get_usdt_balance(session)
        
        # Obter a posição atual para esta conta
        position = get_current_position(session, symbol)
        
        outcome = 0.0  # Inicializar outcome
        pnl = 0.0      # Inicializar PnL
        
        if action == 'long':
            if position:
                if position['side'] == 'Sell':
                    # Fechar posição short antes de abrir long
                    logging.info(f"Conta {account}: Fechando posição short antes de abrir long.")
                    
                    # Fechar posição
                    close_position(session, position)
                    
                    # Obter saldo após fechar a posição
                    usdt_balance_after_exit = get_usdt_balance(session)
                    
                    # Calcular outcome e PnL para o exit
                    entry_balance = account_data[account]['entry_balance']
                    if entry_balance is not None and entry_balance > 0:
                        pnl_exit = usdt_balance_after_exit - entry_balance
                        outcome_exit = (pnl_exit / entry_balance) * 100
                    else:
                        logging.warning(f"Conta {account}: Saldo de entrada não registrado. Não é possível calcular o outcome.")
                        pnl_exit = 0.0
                        outcome_exit = 0.0
                    
                    # Registrar o exit no CSV
                    action_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                    account_end_time = time.time()
                    latency = account_end_time - account_start_time
                    data_row_exit = {
                        'api_owner': account,
                        'alert_time': alert_time,
                        'action_time': action_time,
                        'type': 'exit',
                        'btc_price': btc_price,
                        'balance': usdt_balance_after_exit,
                        'outcome': outcome_exit,
                        'PnL': pnl_exit,
                        'latency': latency
                    }
                    write_to_csv(data_row_exit)
                    
                    # Resetar dados de entrada
                    account_data[account]['entry_balance'] = None
                    account_data[account]['entry_time'] = None
                    account_data[account]['entry_price'] = None
                    
                    # Abrir nova posição long
                    open_position(session, 'long', symbol, leverage)
                    
                    # Obter saldo após abrir a nova posição
                    usdt_balance_after_entry = get_usdt_balance(session)
                    
                    # Registrar dados de entrada
                    account_data[account]['entry_balance'] = usdt_balance_after_entry
                    account_data[account]['entry_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                    account_data[account]['entry_price'] = btc_price
                    
                    # Registrar a nova entrada no CSV com outcome e PnL zero
                    action_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                    account_end_time = time.time()
                    latency = account_end_time - account_start_time
                    data_row_entry = {
                        'api_owner': account,
                        'alert_time': alert_time,
                        'action_time': action_time,
                        'type': 'long',
                        'btc_price': btc_price,
                        'balance': usdt_balance_after_entry,
                        'outcome': 0.0,
                        'PnL': 0.0,
                        'latency': latency
                    }
                    write_to_csv(data_row_entry)
                    
                elif position['side'] == 'Buy':
                    logging.info(f"Conta {account}: Já está em posição long. Nenhuma ação necessária.")
                else:
                    logging.warning(f"Conta {account}: Lado da posição desconhecido.")
            else:
                # Nenhuma posição aberta, abrir long
                open_position(session, 'long', symbol, leverage)
                
                # Obter saldo após abrir a posição
                usdt_balance_after_entry = get_usdt_balance(session)
                
                # Registrar dados de entrada
                account_data[account]['entry_balance'] = usdt_balance_after_entry
                account_data[account]['entry_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                account_data[account]['entry_price'] = btc_price
                
                # Registrar a nova entrada no CSV com outcome e PnL zero
                action_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                account_end_time = time.time()
                latency = account_end_time - account_start_time
                data_row_entry = {
                    'api_owner': account,
                    'alert_time': alert_time,
                    'action_time': action_time,
                    'type': 'long',
                    'btc_price': btc_price,
                    'balance': usdt_balance_after_entry,
                    'outcome': 0.0,
                    'PnL': 0.0,
                    'latency': latency
                }
                write_to_csv(data_row_entry)
                
        elif action == 'short':
            if position:
                if position['side'] == 'Buy':
                    # Fechar posição long antes de abrir short
                    logging.info(f"Conta {account}: Fechando posição long antes de abrir short.")
                    
                    # Fechar posição
                    close_position(session, position)
                    
                    # Obter saldo após fechar a posição
                    usdt_balance_after_exit = get_usdt_balance(session)
                    
                    # Calcular outcome e PnL para o exit
                    entry_balance = account_data[account]['entry_balance']
                    if entry_balance is not None and entry_balance > 0:
                        pnl_exit = usdt_balance_after_exit - entry_balance
                        outcome_exit = (pnl_exit / entry_balance) * 100
                    else:
                        logging.warning(f"Conta {account}: Saldo de entrada não registrado. Não é possível calcular o outcome.")
                        pnl_exit = 0.0
                        outcome_exit = 0.0
                    
                    # Registrar o exit no CSV
                    action_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                    account_end_time = time.time()
                    latency = account_end_time - account_start_time
                    data_row_exit = {
                        'api_owner': account,
                        'alert_time': alert_time,
                        'action_time': action_time,
                        'type': 'exit',
                        'btc_price': btc_price,
                        'balance': usdt_balance_after_exit,
                        'outcome': outcome_exit,
                        'PnL': pnl_exit,
                        'latency': latency
                    }
                    write_to_csv(data_row_exit)
                    
                    # Resetar dados de entrada
                    account_data[account]['entry_balance'] = None
                    account_data[account]['entry_time'] = None
                    account_data[account]['entry_price'] = None
                    
                    # Abrir nova posição short
                    open_position(session, 'short', symbol, leverage)
                    
                    # Obter saldo após abrir a nova posição
                    usdt_balance_after_entry = get_usdt_balance(session)
                    
                    # Registrar dados de entrada
                    account_data[account]['entry_balance'] = usdt_balance_after_entry
                    account_data[account]['entry_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                    account_data[account]['entry_price'] = btc_price
                    
                    # Registrar a nova entrada no CSV com outcome e PnL zero
                    action_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                    account_end_time = time.time()
                    latency = account_end_time - account_start_time
                    data_row_entry = {
                        'api_owner': account,
                        'alert_time': alert_time,
                        'action_time': action_time,
                        'type': 'short',
                        'btc_price': btc_price,
                        'balance': usdt_balance_after_entry,
                        'outcome': 0.0,
                        'PnL': 0.0,
                        'latency': latency
                    }
                    write_to_csv(data_row_entry)
                    
                elif position['side'] == 'Sell':
                    logging.info(f"Conta {account}: Já está em posição short. Nenhuma ação necessária.")
                else:
                    logging.warning(f"Conta {account}: Lado da posição desconhecido.")
            else:
                # Nenhuma posição aberta, abrir short
                open_position(session, 'short', symbol, leverage)
                
                # Obter saldo após abrir a posição
                usdt_balance_after_entry = get_usdt_balance(session)
                
                # Registrar dados de entrada
                account_data[account]['entry_balance'] = usdt_balance_after_entry
                account_data[account]['entry_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                account_data[account]['entry_price'] = btc_price
                
                # Registrar a nova entrada no CSV com outcome e PnL zero
                action_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                account_end_time = time.time()
                latency = account_end_time - account_start_time
                data_row_entry = {
                    'api_owner': account,
                    'alert_time': alert_time,
                    'action_time': action_time,
                    'type': 'short',
                    'btc_price': btc_price,
                    'balance': usdt_balance_after_entry,
                    'outcome': 0.0,
                    'PnL': 0.0,
                    'latency': latency
                }
                write_to_csv(data_row_entry)
                
        elif action == 'exit':
            if position:
                # Fechar posição aberta
                close_position(session, position)
                logging.info(f"Conta {account}: Posição fechada com sucesso.")
                
                # Obter saldo após fechar a posição
                usdt_balance_after_exit = get_usdt_balance(session)
                
                # Calcular outcome e PnL
                entry_balance = account_data[account]['entry_balance']
                if entry_balance is not None and entry_balance > 0:
                    pnl = usdt_balance_after_exit - entry_balance
                    outcome = (pnl / entry_balance) * 100
                else:
                    logging.warning(f"Conta {account}: Saldo de entrada não registrado. Não é possível calcular o outcome.")
                    pnl = 0.0
                    outcome = 0.0
                
                # Registrar o exit no CSV
                action_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                account_end_time = time.time()
                latency = account_end_time - account_start_time
                data_row_exit = {
                    'api_owner': account,
                    'alert_time': alert_time,
                    'action_time': action_time,
                    'type': 'exit',
                    'btc_price': btc_price,
                    'balance': usdt_balance_after_exit,
                    'outcome': outcome,
                    'PnL': pnl,
                    'latency': latency
                }
                write_to_csv(data_row_exit)
                
                # Resetar dados de entrada
                account_data[account]['entry_balance'] = None
                account_data[account]['entry_time'] = None
                account_data[account]['entry_price'] = None
                
            else:
                logging.info(f"Conta {account}: Nenhuma posição aberta para fechar.")
                # Opcionalmente, você pode registrar um 'exit' mesmo se não houver posição aberta
                # Nesse caso, outcome e PnL serão zero
                action_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                account_end_time = time.time()
                latency = account_end_time - account_start_time
                data_row_exit = {
                    'api_owner': account,
                    'alert_time': alert_time,
                    'action_time': action_time,
                    'type': 'exit',
                    'btc_price': btc_price,
                    'balance': usdt_balance_before,  # Saldo não mudou
                    'outcome': 0.0,
                    'PnL': 0.0,
                    'latency': latency
                }
                write_to_csv(data_row_exit)
    
    webhook_end_time = time.time()
    total_latency = webhook_end_time - webhook_start_time
    logging.info(f"Latência total: {total_latency:.3f} segundos")
    
    return jsonify({'message': 'Ação executada', 'latency': total_latency}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
