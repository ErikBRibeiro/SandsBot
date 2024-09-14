from pybit import HTTP
import pandas as pd
from datetime import datetime

# Configuração da API da Bybit
session = HTTP("https://api.bybit.com")

# Função para coletar dados históricos
def get_bybit_data(symbol, interval, start_time, end_time):
    # Convertendo datas para timestamps UNIX
    start_time_unix = int(datetime.strptime(start_time, '%Y-%m-%d').timestamp())
    end_time_unix = int(datetime.strptime(end_time, '%Y-%m-%d').timestamp())
    
    data = []
    while start_time_unix < end_time_unix:
        # Solicitar dados da Bybit
        result = session.query_kline(symbol=symbol, interval=interval, limit=200, from_time=start_time_unix)
        if 'result' not in result or len(result['result']) == 0:
            break  # Parar se não houver mais dados

        data.extend(result['result'])
        
        # Atualizar o tempo de início para o próximo candle após o último retornado
        last_timestamp = result['result'][-1]['open_time']
        start_time_unix = last_timestamp + 1  # Adicionar 1 segundo para evitar duplicatas

    # Transformar os dados em DataFrame
    df = pd.DataFrame(data)
    df['open_time'] = pd.to_datetime(df['open_time'], unit='s')
    
    return df

# Parâmetros para buscar os dados
symbol = 'BTCUSDT'
interval = '60'  # 1h candles
start_time = '2024-01-01'
end_time = '2024-09-01'

# Obtenção dos dados
df_btcusdt = get_bybit_data(symbol, interval, start_time, end_time)

# Exibir as primeiras linhas do dataframe
print(df_btcusdt.head())

# Salvar o DataFrame como CSV, se necessário
df_btcusdt.to_csv('BTCUSDT_bybit_data_1h.csv', index=False)
