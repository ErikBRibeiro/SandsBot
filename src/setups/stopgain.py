# Fechar uma posição short com stopgain
def short_stopgain(current_price, stopgain):
    if current_price <= stopgain:  # Troca de < para <=
        return True
    return False

# Fechar uma posição long com stopgain
def long_stopgain(current_price, stopgain):
    if current_price >= stopgain:  # Troca de > para >=
        return True
    return False

# Define o preço de fechamento em stopgain para uma operação short
def set_short_stopgain_ratio(short_price, stoploss, gain_ratio):
    return short_price - (stoploss - short_price) * gain_ratio

# Define o preço de fechamento em stopgain para uma operação long
def set_long_stopgain_ratio(long_price, stoploss, gain_ratio):
    return long_price + (long_price - stoploss) * gain_ratio

# Define o preço de fechamento em stopgain para uma operação short
def set_short_stopgain_percentage(short_price, gain_percentage):
    return short_price - short_price * gain_percentage / 100

# Define o preço de fechamento em stopgain para uma posição long
def set_long_stopgain_percentage(long_price, gain_percentage):
    return long_price + long_price * gain_percentage / 100
