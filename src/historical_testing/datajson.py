from binance import Client
import pandas as pd
from datetime import datetime
import os
import json

def get_binance_data(symbol, interval, start_date_str, end_date_str, api_key, api_secret):
    client = Client(api_key, api_secret)

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    json_filename = f"klines_{symbol}_{interval}_{start_date_str}_to_{end_date_str}.json"

    if os.path.exists(json_filename):
        print(f"Arquivo JSON encontrado para o período {start_date_str} a {end_date_str}. Utilizando dados do JSON.")
        with open(json_filename, 'r') as json_file:
            data_dict = json.load(json_file)
            df = pd.DataFrame(data_dict)
            if 'timestamp' not in df.columns:
                df['timestamp'] = pd.to_datetime(df['open_time'], unit='ms')
            df.set_index('timestamp', inplace=True)
    else:
        print(f"Nenhum arquivo JSON encontrado para o período {start_date_str} a {end_date_str}. Solicitando dados da API.")
        klines = client.get_historical_klines(symbol, interval, start_date.strftime("%d %b %Y %H:%M:%S"), end_date.strftime("%d %b %Y %H:%M:%S"))
        df = pd.DataFrame(klines, columns=['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'])
        df['timestamp'] = pd.to_datetime(df['open_time'], unit='ms')
        df.set_index('timestamp', inplace=True)
        df['close'] = df['close'].astype(float)
        df['low'] = df['low'].astype(float)
        df['high'] = df['high'].astype(float)

        df_reset = df.reset_index()
        df_reset['timestamp'] = df_reset['timestamp'].astype(str)
        data_dict = df_reset.to_dict(orient='records')

        with open(json_filename, 'w') as json_file:
            json.dump(data_dict, json_file)
        print(f"Dados solicitados da API e salvos em {json_filename} para consultas futuras.")

    return df

if __name__ == "__main__":
    api_key = "ok9V7x0ETItBjXJJXp3HZNQbx1rAN26OiGIaPey7DMDm2d2612gU5aVQdT0E82bz"
    api_secret = "k15suaXdqzbwfvrYX0qcvNAkXET8EnqjO9JraQhduShjIGQ0YX0kiqXEntTdlRq0"
    symbol = "BTCUSDT"
    interval = "1h"
    start_date_str = "2023-01-01"
    end_date_str = "2023-10-25"

    df = get_binance_data(symbol, interval, start_date_str, end_date_str, api_key, api_secret)
