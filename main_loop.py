import pandas as pd
df = pd.read_csv('/app/BYBIT_BTCUSDT.P.csv')
df.to_csv('/app/data/BYBIT_BTCUSDT.P.csv', index=False)