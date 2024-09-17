import pandas as pd
from datetime import datetime

# Definir o caminho para o arquivo CSV
caminho_csv = '/app/data/trade_history.csv'  # Substitua pelo caminho do seu arquivo CSV

# Ler o CSV existente
try:
    df = pd.read_csv(caminho_csv)
except FileNotFoundError:
    # Se o arquivo não existir, criar um DataFrame vazio com as colunas apropriadas
    colunas = [
        "trade_id", "timestamp", "symbol", "entry_price", "exit_price",
        "quantity", "stop_loss", "stop_gain", "potential_loss",
        "potential_gain", "timeframe", "setup", "outcome", "commission",
        "old_balance", "new_balance", "secondary_stop_loss",
        "secondary_stop_gain", "exit_time", "type", "entry_lateral",
        "exit_lateral"
    ]
    df = pd.DataFrame(columns=colunas)

# Definir os dados da nova linha
nova_linha = {
    "trade_id": "2024-09-17T13:13:21.000000",
    "timestamp": "2024-09-17T13:13:21.000000",
    "symbol": "BTCUSDT",
    "entry_price": 59324.7,
    "exit_price": "",  # Campo vazio
    "quantity": 16.847,
    "stop_loss": 54578.72,
    "stop_gain": 78308.60,
    "potential_loss": -8.00,
    "potential_gain": 32.00,
    "timeframe": "1h",
    "setup": "GPTAN",
    "outcome": "",  # Campo vazio
    "commission": "",  # Campo vazio
    "old_balance": 1000000,
    "new_balance": "",  # Campo vazio
    "secondary_stop_loss": "",  # Campo vazio
    "secondary_stop_gain": "",  # Campo vazio
    "exit_time": "",  # Campo vazio
    "type": "long",
    "entry_lateral": 0,
    "exit_lateral": ""  # Campo vazio
}

# Adicionar a nova linha ao DataFrame
df_nova = pd.DataFrame([nova_linha])
df_atualizado = pd.concat([df, df_nova], ignore_index=True)

# Salvar o DataFrame atualizado de volta no CSV
df.to_csv(caminho_csv, index=False)

print("Nova linha adicionada com sucesso!")
