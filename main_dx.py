# Codigo que realiza compra ao ser executado

import os
import sys
import logging
import math
from dotenv import load_dotenv, find_dotenv
from pybit.unified_trading import HTTP

# Carregar a chave e o segredo da API do arquivo .env
load_dotenv(find_dotenv())
API_KEY = os.getenv('BYBIT_API_KEY')
API_SECRET = os.getenv('BYBIT_API_SECRET')

if not API_KEY or not API_SECRET:
    logging.error("API_KEY e/ou API_SECRET não encontrados. Por favor, verifique seu arquivo .env.")
    sys.exit(1)

# Inicializar o cliente Bybit usando a classe HTTP do unified_trading
session = HTTP(
    testnet=False,  # Defina como True se quiser usar o testnet
    api_key=API_KEY,
    api_secret=API_SECRET
)

symbol = 'BTCUSDT'

# Configurar o logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bot_log.log"),
        logging.StreamHandler()
    ]
)

# Função para obter o saldo total da conta
def get_account_balance():
    try:
        balance_info = session.get_wallet_balance(accountType='UNIFIED')
        if balance_info['retMsg'] != 'OK':
            logging.error(f"Erro ao obter o saldo da conta: {balance_info['retMsg']}")
            return None

        # Acessando totalEquity corretamente na lista dentro de result
        total_equity = float(balance_info['result']['list'][0]['totalEquity'])
        return total_equity
    except Exception as e:
        logging.error(f"Exceção em get_account_balance: {e}")
        return None

# Função para calcular a quantidade baseada no equity e no preço
def calculate_qty(total_equity, latest_price, leverage=1):
    try:
        # Calcular o número de contratos
        qty = (total_equity * leverage) / latest_price
        factor = 100
        # Ajustar para a precisão permitida pela Bybit (por exemplo, 2 casas decimais)
        qty = math.floor(qty * factor) / factor

        return qty
    except Exception as e:
        logging.error(f"Exceção em calculate_qty: {e}")
        return None

# Obter o equity total da conta
total_equity = get_account_balance()
if total_equity is None:
    logging.error("Não foi possível obter o saldo da conta.")
    sys.exit(1)

# Obter o preço mais recente do símbolo
try:
    ticker_info = session.get_tickers(
        category='linear',
        symbol=symbol
    )
    if ticker_info['retMsg'] != 'OK':
        logging.error(f"Erro ao obter o ticker: {ticker_info['retMsg']}")
        sys.exit(1)
    latest_price = float(ticker_info['result']['list'][0]['lastPrice'])
except Exception as e:
    logging.error(f"Exceção ao obter o preço mais recente: {e}")
    sys.exit(1)

# Calcular a quantidade
qty = calculate_qty(total_equity, latest_price)
if qty is None or qty <= 0:
    logging.error("Quantidade calculada inválida.")
    sys.exit(1)

# Obter a posição atual
positions = session.get_positions(
    category='linear',
    symbol=symbol
)

# Encontrar o tamanho da posição atual
current_qty = 0
for position in positions['result']['list']:
    if position['side'] == 'Buy' and float(position['size']) > 0:
        current_qty = float(position['size'])
        break

# Fechar a posição de compra atual, se existir
if current_qty > 0:
    logging.info(f"Fechando posição long atual de tamanho {current_qty}")
    close_order = session.place_order(
        category='linear',
        symbol=symbol,
        side='Sell',
        orderType='Market',
        qty=str(current_qty),
        timeInForce='GTC',
        reduceOnly=True,
        closeOnTrigger=False
    )
    logging.info(f"Resposta da ordem de fechamento: {close_order}")
else:
    logging.info("Nenhuma posição long para fechar.")

# Entrar em uma nova posição de venda (short)
logging.info(f"Colocando nova ordem de venda de tamanho {qty}")
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
logging.info(f"Resposta da nova ordem de venda: {order}")
