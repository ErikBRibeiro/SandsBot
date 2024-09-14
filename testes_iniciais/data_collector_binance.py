from binance.client import Client
import pandas as pd
import time

# Coloque sua API key e secret da Binance aqui
api_key = 'OwlZZsiPG8ZhPQw9mNqS7iEzkBgiiHLt7AvtskYKlvbFS8fdPBT0bJ6qfWP71Ndk'
api_secret = 'LqKfrWgAlX47SzFlEHzrMiwHkEtJGozd3Wc1NorIkY8BCfkOG21BkjJhk6p6pF4D'

client = Client(api_key, api_secret)

# Função para obter dados históricos
def get_historical_futures(symbol, interval, start_str, end_str=None):
    # Coleta dados em lotes de 500
    data = []
    while True:
        try:
            klines = client.futures_klines(symbol=symbol, interval=interval, startTime=start_str, endTime=end_str, limit=500)
            if len(klines) == 0:
                break
            data.extend(klines)
            start_str = klines[-1][0]  # Atualiza o timestamp inicial
            time.sleep(0.5)  # Evitar exceder o limite de requests
        except Exception as e:
            print(f'Erro: {e}')
            break
    return data

# Parâmetros
symbol = 'BTCUSDT'
interval = Client.KLINE_INTERVAL_1HOUR  # Ajuste para o intervalo que desejar
start_date = '1 Jan, 2024'
end_date = '1 Sep, 2024'

# Coletando os dados
candles = get_historical_futures(symbol, interval, start_date, end_date)

# Convertendo para DataFrame
columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
df = pd.DataFrame(candles, columns=columns)

# Transformando o timestamp em formato legível
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

# Salvando os dados em um arquivo CSV
df.to_csv('BTCUSDT_futures_data.csv', index=False)

print("Dados coletados e salvos com sucesso.")
