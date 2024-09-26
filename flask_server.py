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
SECRET_KEY = os.getenv('SECRET_KEY', '2112')  # Padrão para '1221' se não estiver definido

# Lista de contas na ordem desejada
accounts_order = ['TESTE1', 'TESTE2', 'TESTE3']

# Dicionário para armazenar as sessões da API e dados da conta
api_sessions = {}
account_data = {}

# Caminhos para salvar os arquivos CSV
csv_file_path = '/app/data/trade_history.csv'
error_csv_path = '/app/data/errors_history.csv'

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

def write_error_to_csv(account_name, code, message):
    columns = ['account', 'code', 'message', 'timestamp']
    data_row = {
        'account': account_name,
        'code': code,
        'message': message,
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    }
    df_new = pd.DataFrame([data_row], columns=columns)
    # Verificar se o arquivo CSV já existe
    if os.path.isfile(error_csv_path):
        df_existing = pd.read_csv(error_csv_path)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_combined = df_new
    try:
        df_combined.to_csv(error_csv_path, index=False)
    except Exception as e:
        logging.error(f"Erro ao escrever no arquivo de erros CSV: {e}")

def get_usdt_balance(session, account_name):
    try:
        response = session.get_wallet_balance(accountType='UNIFIED', coin='USDT')
        if response['retCode'] == 0:
            coin_list = response['result']['list'][0]['coin']
            for coin in coin_list:
                if coin['coin'] == 'USDT':
                    usdt_balance = float(coin['walletBalance'])
                    return usdt_balance
            # Se USDT não for encontrado
            message = "USDT não encontrado na lista de moedas."
            logging.error(f"Conta {account_name}: {message}")
            write_error_to_csv(account_name, response['retCode'], message)
            return 0.0
        else:
            message = f"Erro ao obter saldo: {response['retMsg']}"
            logging.error(f"Conta {account_name}: {message}")
            write_error_to_csv(account_name, response['retCode'], message)
            return 0.0
    except Exception as e:
        message = f"Erro ao obter saldo: {e}"
        logging.error(f"Conta {account_name}: {message}")
        write_error_to_csv(account_name, 'Exception', str(e))
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

def get_open_positions_info(session, account_name):
    try:
        response = session.get_positions(
            category='linear',
            settleCoin='USDT'  # Adicionado o settleCoin para obter todas as posições USDT
        )
        if response['retCode'] == 0:
            positions = response['result']['list']
            open_positions = [pos for pos in positions if float(pos['size']) > 0]
            num_open_positions = len(open_positions)

            total_size_btc = sum(float(pos['size']) for pos in open_positions)
            # Obter o preço atual do BTC
            btc_price = get_current_price('BTCUSDT')
            total_value_usdt = total_size_btc * btc_price

            return {
                'num_open_positions': num_open_positions,
                'total_size_btc': total_size_btc,
                'total_value_usdt': total_value_usdt
            }
        else:
            message = f"Erro ao obter posições: {response['retMsg']}"
            logging.error(f"Conta {account_name}: {message}")
            write_error_to_csv(account_name, response['retCode'], message)
            return {
                'num_open_positions': 0,
                'total_size_btc': 0.0,
                'total_value_usdt': 0.0
            }
    except Exception as e:
        message = f"Erro ao obter posições: {e}"
        logging.error(f"Conta {account_name}: {message}")
        write_error_to_csv(account_name, 'Exception', str(e))
        return {
            'num_open_positions': 0,
            'total_size_btc': 0.0,
            'total_value_usdt': 0.0
        }

# Após inicializar as sessões, obter e registrar o saldo e posições de cada conta
btc_price = get_current_price('BTCUSDT')  # Obter uma única vez para otimização
for account in accounts_order:
    session = api_sessions[account]
    balance = get_usdt_balance(session, account)
    positions_info = get_open_positions_info(session, account)
    num_open_positions = positions_info['num_open_positions']
    total_size_btc = positions_info['total_size_btc']
    total_value_usdt = total_size_btc * btc_price

    # Formatar os valores para exibição
    balance_formatted = f"{balance:,.2f}"
    total_size_btc_formatted = f"{total_size_btc:,.6f}"
    total_value_usdt_formatted = f"{total_value_usdt:,.2f}"

    logging.info(f"Conta {account}: Saldo USDT no Unified: {balance_formatted} USDT, "
                 f"Contratos abertos: {num_open_positions}, "
                 f"Valor total dos contratos abertos: {total_size_btc_formatted} BTC "
                 f"(~{total_value_usdt_formatted} USDT)")

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
            message = f"Erro ao obter posição: {response['retMsg']}"
            logging.error(f"Conta {account_name}: {message}")
            write_error_to_csv(account_name, response['retCode'], message)
            return None
    except Exception as e:
        message = f"Erro ao obter posição: {e}"
        logging.error(f"Conta {account_name}: {message}")
        write_error_to_csv(account_name, 'Exception', str(e))
        return None

def close_position(session, position, account_name):
    try:
        if position['side'] == 'Buy':
            side = 'Sell'
        elif position['side'] == 'Sell':
            side = 'Buy'
        else:
            message = "Lado da posição desconhecido."
            logging.warning(f"Conta {account_name}: {message}")
            write_error_to_csv(account_name, 'UnknownSide', message)
            return None  # Indica falha

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

            # Obter o ID da ordem
            order_id = order['result']['orderId']

            # Aguardar brevemente para garantir que os detalhes da execução estejam disponíveis
            time.sleep(0.5)

            # Obter detalhes da execução usando get_executions
            executions = session.get_executions(
                category='linear',
                symbol=position['symbol'],
                orderId=order_id
            )

            if executions['retCode'] == 0 and 'list' in executions['result']:
                total_qty = 0.0
                total_value = 0.0
                for exec in executions['result']['list']:
                    exec_qty = float(exec['execQty'])
                    exec_price = float(exec['execPrice'])
                    total_qty += exec_qty
                    total_value += exec_qty * exec_price
                if total_qty > 0:
                    average_price = total_value / total_qty
                else:
                    average_price = 0.0
                return {
                    'average_price': average_price,
                    'total_qty': total_qty
                }
            else:
                message = f"Erro ao obter detalhes da execução: {executions['retMsg']}"
                logging.error(f"Conta {account_name}: {message}")
                write_error_to_csv(account_name, executions['retCode'], message)
                return None  # Indica falha
        else:
            message = f"Erro ao fechar posição: {order['retMsg']}"
            logging.error(f"Conta {account_name}: {message}")
            write_error_to_csv(account_name, order['retCode'], order['retMsg'])
            return None  # Indica falha
    except Exception as e:
        message = f"Erro ao fechar posição: {e}"
        logging.error(f"Conta {account_name}: {message}")
        write_error_to_csv(account_name, 'Exception', str(e))
        return None  # Indica falha

def open_position(session, action, account_name, symbol='BTCUSDT', leverage=1):
    try:
        balance_percentage = 0.98  # Sempre usar 98% do saldo
        side = 'Buy' if action == 'long' else 'Sell'

        successful_orders = []  # Lista para armazenar ordens bem-sucedidas
        errors_occurred = False

        # Tentar executar a ordem 3 vezes
        for attempt in range(1, 4):
            # Obter o saldo atual
            usdt_balance = get_usdt_balance(session, account_name)
            price = get_current_price(symbol)
            if price == 0.0:
                message = "Preço inválido. Abortando operação."
                logging.error(f"Conta {account_name}: {message}")
                write_error_to_csv(account_name, 'PriceError', message)
                errors_occurred = True
                break  # Abort further attempts

            # Calcular a quantidade
            adjusted_balance = usdt_balance * balance_percentage
            qty = adjusted_balance * leverage / price
            qty = np.floor(qty * 1000) / 1000  # Ajustar para 3 casas decimais

            # Verificar se qty é menor que 0.001 BTC (mínimo da Bybit)
            if qty < 0.001:
                message = f"Quantidade calculada ({qty}) é menor que o mínimo permitido (0.001 BTC). Abortando tentativa {attempt}."
                logging.error(f"Conta {account_name}: {message}")
                write_error_to_csv(account_name, 'MinQtyError', message)
                errors_occurred = True
                break  # Abort further attempts

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
                logging.info(f"Conta {account_name}: Ordem de {side} executada com sucesso na tentativa {attempt}. Qty: {qty}")

                # Obter o ID da ordem
                order_id = order['result']['orderId']

                # Aguardar brevemente para garantir que os detalhes da execução estejam disponíveis
                time.sleep(0.5)

                # Obter detalhes da execução usando get_executions
                executions = session.get_executions(
                    category='linear',
                    symbol=symbol,
                    orderId=order_id
                )

                if executions['retCode'] == 0 and 'list' in executions['result']:
                    for exec in executions['result']['list']:
                        exec_qty = float(exec['execQty'])
                        exec_price = float(exec['execPrice'])
                        successful_orders.append({
                            'qty': exec_qty,
                            'price': exec_price
                        })
                else:
                    message = f"Erro ao obter detalhes da execução: {executions['retMsg']}"
                    logging.error(f"Conta {account_name}: {message}")
                    write_error_to_csv(account_name, executions['retCode'], message)
                    errors_occurred = True
                    break  # Abort further attempts

            else:
                message = f"Erro ao executar ordem na tentativa {attempt}: {order['retMsg']} (retCode {order['retCode']})"
                logging.error(f"Conta {account_name}: {message}")
                write_error_to_csv(account_name, order['retCode'], order['retMsg'])
                errors_occurred = True
                break  # Abort further attempts

        if errors_occurred:
            logging.error(f"Conta {account_name}: Falha ao executar a ordem. Operação abortada.")
            return None  # Indica falha

        if successful_orders:
            # Calcular o preço médio ponderado
            total_qty = sum([o['qty'] for o in successful_orders])
            if total_qty > 0:
                weighted_avg_price = sum([o['qty'] * o['price'] for o in successful_orders]) / total_qty
            else:
                weighted_avg_price = 0.0
            logging.info(f"Conta {account_name}: Preço médio ponderado: {weighted_avg_price:.2f}, Quantidade total: {total_qty}")
            return {
                'average_price': weighted_avg_price,
                'total_qty': total_qty
            }
        else:
            message = "Nenhuma ordem executada com sucesso."
            logging.error(f"Conta {account_name}: {message}")
            write_error_to_csv(account_name, 'NoSuccessOrders', message)
            return None  # Indica falha

    except Exception as e:
        message = f"Erro ao executar ordem: {e}"
        logging.error(f"Conta {account_name}: {message}")
        write_error_to_csv(account_name, 'Exception', str(e))
        return None  # Indica falha

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
                    close_result = close_position(session, position, account)
                    if close_result is None:
                        logging.error(f"Conta {account}: Falha ao fechar posição. Abortando operação.")
                        continue  # Pular para a próxima conta

                    # Obter saldo após fechar a posição
                    usdt_balance_after_exit = get_usdt_balance(session, account)

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
                    action_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                    account_end_time = time.time()
                    latency = account_end_time - account_start_time
                    data_row_exit = {
                        'api_owner': account,
                        'alert_time': alert_time,
                        'action_time': action_time_str,
                        'type': 'exit',
                        'btc_price': close_result['average_price'],
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
                    result = open_position(session, 'long', account, symbol, leverage)

                    if result is not None:
                        # Obter saldo após abrir a posição
                        usdt_balance_after_entry = get_usdt_balance(session, account)

                        # Registrar dados de entrada
                        account_data[account]['entry_balance'] = usdt_balance_after_entry
                        account_data[account]['entry_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                        account_data[account]['entry_price'] = result['average_price']

                        # Registrar a nova entrada no CSV
                        action_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                        account_end_time = time.time()
                        latency = account_end_time - account_start_time
                        data_row_entry = {
                            'api_owner': account,
                            'alert_time': alert_time,
                            'action_time': action_time_str,
                            'type': 'long',
                            'btc_price': result['average_price'],
                            'balance': usdt_balance_after_entry,
                            'outcome': 0.0,
                            'PnL': 0.0,
                            'latency': latency
                        }
                        write_to_csv(data_row_entry)
                    else:
                        logging.error(f"Conta {account}: Não foi possível abrir a posição long.")

                elif position['side'] == 'Buy':
                    logging.info(f"Conta {account}: Já está em posição long. Nenhuma ação necessária.")
                else:
                    message = "Lado da posição desconhecido."
                    logging.warning(f"Conta {account}: {message}")
                    write_error_to_csv(account, 'UnknownSide', message)
            else:
                # Nenhuma posição aberta, abrir long
                result = open_position(session, 'long', account, symbol, leverage)

                if result is not None:
                    # Obter saldo após abrir a posição
                    usdt_balance_after_entry = get_usdt_balance(session, account)

                    # Registrar dados de entrada
                    account_data[account]['entry_balance'] = usdt_balance_after_entry
                    account_data[account]['entry_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                    account_data[account]['entry_price'] = result['average_price']

                    # Registrar a nova entrada no CSV
                    action_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                    account_end_time = time.time()
                    latency = account_end_time - account_start_time
                    data_row_entry = {
                        'api_owner': account,
                        'alert_time': alert_time,
                        'action_time': action_time_str,
                        'type': 'long',
                        'btc_price': result['average_price'],
                        'balance': usdt_balance_after_entry,
                        'outcome': 0.0,
                        'PnL': 0.0,
                        'latency': latency
                    }
                    write_to_csv(data_row_entry)
                else:
                    logging.error(f"Conta {account}: Não foi possível abrir a posição long.")

        elif action == 'short':
            if position:
                if position['side'] == 'Buy':
                    # Fechar posição long antes de abrir short
                    logging.info(f"Conta {account}: Fechando posição long antes de abrir short.")

                    # Fechar posição
                    close_result = close_position(session, position, account)
                    if close_result is None:
                        logging.error(f"Conta {account}: Falha ao fechar posição. Abortando operação.")
                        continue  # Pular para a próxima conta

                    # Obter saldo após fechar a posição
                    usdt_balance_after_exit = get_usdt_balance(session, account)

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
                    action_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                    account_end_time = time.time()
                    latency = account_end_time - account_start_time
                    data_row_exit = {
                        'api_owner': account,
                        'alert_time': alert_time,
                        'action_time': action_time_str,
                        'type': 'exit',
                        'btc_price': close_result['average_price'],
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
                    result = open_position(session, 'short', account, symbol, leverage)

                    if result is not None:
                        # Obter saldo após abrir a posição
                        usdt_balance_after_entry = get_usdt_balance(session, account)

                        # Registrar dados de entrada
                        account_data[account]['entry_balance'] = usdt_balance_after_entry
                        account_data[account]['entry_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                        account_data[account]['entry_price'] = result['average_price']

                        # Registrar a nova entrada no CSV
                        action_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                        account_end_time = time.time()
                        latency = account_end_time - account_start_time
                        data_row_entry = {
                            'api_owner': account,
                            'alert_time': alert_time,
                            'action_time': action_time_str,
                            'type': 'short',
                            'btc_price': result['average_price'],
                            'balance': usdt_balance_after_entry,
                            'outcome': 0.0,
                            'PnL': 0.0,
                            'latency': latency
                        }
                        write_to_csv(data_row_entry)
                    else:
                        logging.error(f"Conta {account}: Não foi possível abrir a posição short.")

                elif position['side'] == 'Sell':
                    logging.info(f"Conta {account}: Já está em posição short. Nenhuma ação necessária.")
                else:
                    message = "Lado da posição desconhecido."
                    logging.warning(f"Conta {account}: {message}")
                    write_error_to_csv(account, 'UnknownSide', message)
            else:
                # Nenhuma posição aberta, abrir short
                result = open_position(session, 'short', account, symbol, leverage)

                if result is not None:
                    # Obter saldo após abrir a posição
                    usdt_balance_after_entry = get_usdt_balance(session, account)

                    # Registrar dados de entrada
                    account_data[account]['entry_balance'] = usdt_balance_after_entry
                    account_data[account]['entry_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                    account_data[account]['entry_price'] = result['average_price']

                    # Registrar a nova entrada no CSV
                    action_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                    account_end_time = time.time()
                    latency = account_end_time - account_start_time
                    data_row_entry = {
                        'api_owner': account,
                        'alert_time': alert_time,
                        'action_time': action_time_str,
                        'type': 'short',
                        'btc_price': result['average_price'],
                        'balance': usdt_balance_after_entry,
                        'outcome': 0.0,
                        'PnL': 0.0,
                        'latency': latency
                    }
                    write_to_csv(data_row_entry)
                else:
                    logging.error(f"Conta {account}: Não foi possível abrir a posição short.")

        elif action == 'exit':
            if position:
                # Fechar posição aberta
                close_result = close_position(session, position, account)
                if close_result is not None:
                    logging.info(f"Conta {account}: Posição fechada com sucesso.")

                    # Obter saldo após fechar a posição
                    usdt_balance_after_exit = get_usdt_balance(session, account)

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
                    action_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                    account_end_time = time.time()
                    latency = account_end_time - account_start_time
                    data_row_exit = {
                        'api_owner': account,
                        'alert_time': alert_time,
                        'action_time': action_time_str,
                        'type': 'exit',
                        'btc_price': close_result['average_price'],
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
                    logging.error(f"Conta {account}: Não foi possível fechar a posição. Nenhum registro será feito no CSV.")
            else:
                logging.info(f"Conta {account}: Nenhuma posição aberta para fechar.")
                # Não registrar no CSV se não houver posição aberta

    webhook_end_time = time.time()
    total_latency = webhook_end_time - webhook_start_time
    logging.info(f"Latência total: {total_latency:.3f} segundos")

    return jsonify({'message': 'Ação executada', 'latency': total_latency}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)