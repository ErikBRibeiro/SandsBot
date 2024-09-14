# Fechar uma posição short com stoploss
def short_stoploss(current_price, stoploss):
    if current_price >= stoploss:  # Troca de > para >=
        return True
    return False

# Fechar uma posição long com stoploss
def long_stoploss(current_price, stoploss):
    if current_price <= stoploss:  # Troca de < para <=
        return True
    return False

# Define o preço de fechamento em stoploss para uma operação short
def set_short_stoploss_max_candles(data, candles=2):
    return max(data['high'].head(candles).tolist())

# Define o preço de fechamento em stoploss para uma operação long
def set_long_stoploss_min_candles(data, candles=2):
    return min(data['low'].head(candles).tolist())
