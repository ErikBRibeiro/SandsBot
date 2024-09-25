from flask import Flask, request, jsonify
from pybit.unified_trading import HTTP
import time
import logging
import numpy as np
import os
import sys
import pandas as pd
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
accounts_order = ['FERNANDO', 'PABLO', 'HAMUCHY', 'ZE', 'ERIK']

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
        logging.error(f"API_KEY and/or API_SECRET not found for account {account}. "
                      f"Please check your .env file.")
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

def get_usdt_balance(session, account_name):
    try:
        response = session.get_wallet_balance(accountType='UNIFIED', coin='USDT')
        if response['retCode'] == 0:
            coin_list = response['result']['list'][0]['coin']
            for coin in coin_list:
                if coin['coin'] == 'USDT':
                    usdt_balance = float(coin['walletBalance'])
                    logging.info(f"Conta {account_name}: Saldo USDT disponível: {usdt_balance}")
                    return usdt_balance
            # Se USDT não for encontrado
            logging.error(f"Conta {account_name}: USDT não encontrado na lista de moedas.")
            return 0.0
        else:
            logging.error(f"Conta {account_name}: Erro ao obter saldo: {response['retMsg']}")
            return 0.0
    except Exception as e:
        logging.error(f"Conta {account_name}: Erro ao obter saldo: {e}")
        return 0.0

# Após inicializar as sessões, obter e registrar o saldo de cada conta
for account in accounts_order:
    session = api_sessions[account]
    balance = get_usdt_balance(session, account)
    logging.info(f"Conta {account}: Saldo inicial: {balance} USDT")

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

def calculate_qty(usdt_balance, price, leverage=1, balance_percentage=0.98):
    if price == 0:
        return 0
    # Usar uma porcentagem do saldo disponível
    adjusted_balance = usdt_balance * balance_percentage
    qty = adjusted_balance * leverage / price
    qty = np.floor(qty * 1000) / 1000  # Ajusta para 3 casas decimais
    return qty

def get_current_position(session, account_name, symbol='BTCUSDT'):
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
            logging.error(f"Conta {account_name}: Erro ao obter posição: {response['retMsg']}")
            return None
    except Exception as e:
        logging.error(f"Conta {account_name}: Erro ao obter posição: {e}")
        return None

def close_position(session, position, account_name):
    try:
        if position['side'] == 'Buy':
            side = 'Sell'
        elif position['side'] == 'Sell':
            side = 'Buy'
        else:
            logging.warning(f"Conta {account_name}: Lado da posição desconhecido.")
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
            logging.info(f"Conta {account_name}: Posição {position['side']} fechada com sucesso.")
        else:
            logging.error(f"Conta {account_name}: Erro ao fechar posição: {order['retMsg']}")
    except Exception as e:
        logging.error(f"Conta {account_name}: Erro ao fechar posição: {e}")

def open_position(session, action, account_name, symbol='BTCUSDT', leverage=1):
    try:
        usdt_balance = get_usdt_balance(session, account_name)
        price = get_current_price(symbol)
        if price == 0.0:
            logging.error(f"Conta {account_name}: Preço inválido. Abortando operação.")
            return

        balance_percentage = 0.98  # Sempre usar 98% do saldo

        # Calcular a quantidade
        adjusted_balance = usdt_balance * balance_percentage
        qty = adjusted_balance * leverage / price
        qty = np.floor(qty * 1000) / 1000  # Ajustar para 3 casas decimais

        # Verificar se qty é menor que 0.001 BTC (mínimo da Bybit)
        if qty < 0.001:
            logging.error(f"Conta {account_name}: Quantidade calculada ({qty}) é menor que o mínimo "
                          f"permitido (0.001 BTC). Abortando operação.")
            return

        side = 'Buy' if action == 'long' else 'Sell'
        order_executed = False

        # Tentar executar a ordem 3 vezes
        for attempt in range(1, 4):
            order = session.place_order(
                category='linear',
                symbol=symbol,
                side=side,
                orderType='Market',
                qty=str(qty),  # qty deve ser uma string
                timeInForce='GTC',
                reduceOnly=False
            )
            if order['retCode'] == 0:
                logging.info(f"Conta {account_name}: Ordem de {side} executada com sucesso na "
                             f"tentativa {attempt}. Qty: {qty}")
                order_executed = True
                break  # Ordem executada com sucesso, sair do loop
            else:
                logging.error(f"Conta {account_name}: Erro ao executar ordem na tentativa {attempt}: "
                              f"{order['retMsg']} (retCode {order['retCode']})")
                if attempt < 3:
                    logging.info(f"Conta {account_name}: Tentando novamente...")
                else:
                    logging.error(f"Conta {account_name}: Não foi possível executar a ordem após "
                                  f"3 tentativas.")
        if not order_executed:
            logging.error(f"Conta {account_name}: Falha ao executar a ordem após múltiplas tentativas.")
    except Exception as e:
        logging.error(f"Conta {account_name}: Erro ao executar ordem: {e}")

def write_to_csv(data_row):
    # Definir os nomes das colunas
    columns = ['api_owner', 'alert_time', 'action_time', 'type', 'btc_price',
               'balance', 'outcome', 'PnL', 'latency']

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
        usdt_balance_before = get_usdt_balance(session, account)

        # Obter a posição atual para esta conta
        position = get_current_position(session, account, symbol)

        outcome = 0.0  # Inicializar outcome
        pnl = 0.0      # Inicializar PnL

        if action == 'long':
            if position:
                if position['side'] == 'Sell':
                    # Fechar posição short antes de abrir long
                    logging.info(f"Conta {account}: Fechando posição short antes de abrir long.")

                    # Fechar posição
                    close_position(session, position, account)

                    # Obter saldo após fechar a posição
                    usdt_balance_after_exit = get_usdt_balance(session, account)

                    # Calcular outcome e PnL para o exit
                    entry_balance = account_data[account]['entry_balance']
                    if entry_balance is not None and entry_balance > 0:
                        pnl_exit = usdt_balance_after_exit - entry_balance
                        outcome_exit = (pnl_exit / entry_balance) * 100
                    else:
                        logging.warning(f"Conta {account}: Saldo de entrada não registrado. "
                                        f"Não é possível calcular o outcome.")
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
                    open_position(session, 'long', account, symbol, leverage)

                    # Obter saldo após abrir a nova posição
                    usdt_balance_after_entry = get_usdt_balance(session, account)

                    # Registrar dados de entrada
                    account_data[account]['entry_balance'] = usdt_balance_after_entry
                    account_data[account]['entry_time'] = time.strftime('%Y-%m-%d %H:%M:%S',
                                                                        time.localtime())
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
                open_position(session, 'long', account, symbol, leverage)

                # Obter saldo após abrir a posição
                usdt_balance_after_entry = get_usdt_balance(session, account)

                # Registrar dados de entrada
                account_data[account]['entry_balance'] = usdt_balance_after_entry
                account_data[account]['entry_time'] = time.strftime('%Y-%m-%d %H:%M:%S',
                                                                    time.localtime())
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
                    close_position(session, position, account)

                    # Obter saldo após fechar a posição
                    usdt_balance_after_exit = get_usdt_balance(session, account)

                    # Calcular outcome e PnL para o exit
                    entry_balance = account_data[account]['entry_balance']
                    if entry_balance is not None and entry_balance > 0:
                        pnl_exit = usdt_balance_after_exit - entry_balance
                        outcome_exit = (pnl_exit / entry_balance) * 100
                    else:
                        logging.warning(f"Conta {account}: Saldo de entrada não registrado. "
                                        f"Não é possível calcular o outcome.")
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
                    open_position(session, 'short', account, symbol, leverage)

                    # Obter saldo após abrir a nova posição
                    usdt_balance_after_entry = get_usdt_balance(session, account)

                    # Registrar dados de entrada
                    account_data[account]['entry_balance'] = usdt_balance_after_entry
                    account_data[account]['entry_time'] = time.strftime('%Y-%m-%d %H:%M:%S',
                                                                        time.localtime())
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
                open_position(session, 'short', account, symbol, leverage)

                # Obter saldo após abrir a posição
                usdt_balance_after_entry = get_usdt_balance(session, account)

                # Registrar dados de entrada
                account_data[account]['entry_balance'] = usdt_balance_after_entry
                account_data[account]['entry_time'] = time.strftime('%Y-%m-%d %H:%M:%S',
                                                                    time.localtime())
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
                close_position(session, position, account)
                logging.info(f"Conta {account}: Posição fechada com sucesso.")

                # Obter saldo após fechar a posição
                usdt_balance_after_exit = get_usdt_balance(session, account)

                # Calcular outcome e PnL
                entry_balance = account_data[account]['entry_balance']
                if entry_balance is not None and entry_balance > 0:
                    pnl = usdt_balance_after_exit - entry_balance
                    outcome = (pnl / entry_balance) * 100
                else:
                    logging.warning(f"Conta {account}: Saldo de entrada não registrado. "
                                    f"Não é possível calcular o outcome.")
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
                # Opcionalmente, registrar um 'exit' mesmo se não houver posição aberta
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
