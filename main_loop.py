import pandas as pd
import os

# Caminho atualizado do arquivo CSV existente
trade_history_file = '/app/data/trade_history.csv'

# Verificar se o arquivo existe
if os.path.isfile(trade_history_file):
    # Carregar o CSV existente
    df_trade_history = pd.read_csv(trade_history_file)
    
    # Verificar se a coluna 'type' já existe
    if 'type' not in df_trade_history.columns:
        # Adicionar a nova coluna 'type' com valores em branco
        df_trade_history['type'] = ''
        
        # Salvar o CSV com a nova coluna
        df_trade_history.to_csv(trade_history_file, index=False)
        print("Coluna 'type' adicionada com sucesso.")
    else:
        print("A coluna 'type' já existe no arquivo.")
else:
    print(f"O arquivo {trade_history_file} não foi encontrado.")
