# compra quando toca a média móvel - geralmente para fechar posições de venda
def buy_ema_touch(data, ema):
    if data['low'].iloc[0] < data[ema].iloc[0]:
        return True
    return False

# venda quando toca a média móvel - geralmente para fechar posições de compra
def sell_ema_touch(data, ema):
    if data['high'].iloc[0] > data[ema].iloc[0]:
        return True
    return False

# fix: falta ajustar para padrão da função double_ema_rompimento
# preferencial para tempos gráficos abaixo de 4h
def buy_ema_breakout(previous_ema, pre_previous_ema, previous_high, current_price):
    if previous_ema > pre_previous_ema and previous_high < current_price:
        return True
    return False

def sell_ema_breakout(data, ema):
    if data['close'].iloc[1] < data[ema].iloc[1]:
        if data['low'].iloc[0] < data['low'].iloc[1]:
            return True
    return False

# compra quando fecha acima da ema, na abertura da próxima
def buy_ema_close(data, ema):
    if data['close'].iloc[1] > data[ema].iloc[1]:
        return True
    return False

# vende quando fecha abaixo da ema, na abertura da próxima
def sell_ema_close(data, ema):
    if data['close'].iloc[1] < data[ema].iloc[1]:
        return True
    return False

# preferencial para tempos gráficos iguais ou acima de 4h
def buy_ema_violation(data, ema):
    if data['high'].iloc[1] > data[ema].iloc[1]:
        return True
    return True

def sell_ema_violation(data, ema):
    if data['low'].iloc[1] < data[ema].iloc[1]:
        return True
    return False

def buy_double_ema_breakout(data, ema1, ema2):
    if data['close'].iloc[1] > data[ema2].iloc[1]:
        if data['close'].iloc[1] > data[ema1].iloc[1]:
            if data['high'].iloc[0] > data['high'].iloc[1]:
                return True
    return False

def sell_double_ema_breakout(data, ema1, ema2):
    if data['close'].iloc[1] < data[ema2].iloc[1]:
        if data['close'].iloc[1] < data[ema1].iloc[1]:
            if data['low'].iloc[0] < data['low'].iloc[1]:
                return True
    return False

def buy_double_ema_close(data, ema1, ema2):
    if data['close'].iloc[1] > data[ema2].iloc[1]:
        if data['close'].iloc[1] > data[ema1].iloc[1]:
            return True
    return False

def sell_double_ema_close(data, ema1, ema2):
    if data['close'].iloc[1] < data[ema2].iloc[1]:
        if data['close'].iloc[1] < data[ema1].iloc[1]:
            return True
    return False

def buy_double_ema_violation(data, ema1, ema2):
    if data['high'].iloc[1] > data[ema2].iloc[1]:
        if data['high'].iloc[1] > data[ema1].iloc[1]:
            return True
    return False

def sell_double_ema_violation(data, ema1, ema2):
    if data['low'].iloc[1] < data[ema2].iloc[1]:
        if data['low'].iloc[1] < data[ema1].iloc[1]:
            return True
    return False

def buy_triple_ema_breakout(data, ema1, ema2, ema3):
    if data['close'].iloc[1] > data[ema3].iloc[1]:
        if data['close'].iloc[1] > data[ema2].iloc[1]:
            if data['close'].iloc[1] > data[ema1].iloc[1]:
                if data['high'].iloc[0] > data['high'].iloc[1]:
                    return True
    return False

def sell_triple_ema_breakout(data, ema1, ema2, ema3):
    if data['close'].iloc[1] < data[ema3].iloc[1]:
        if data['close'].iloc[1] < data[ema2].iloc[1]:
            if data['close'].iloc[1] < data[ema1].iloc[1]:
                if data['low'].iloc[0] < data['low'].iloc[1]:
                    return True
    return False

def buy_triple_ema_close(data, ema1, ema2, ema3):
    if data['close'].iloc[1] > data[ema3].iloc[1]:
        if data['close'].iloc[1] > data[ema2].iloc[1]:
            if data['close'].iloc[1] > data[ema1].iloc[1]:
                return True
    return False

def sell_triple_ema_close(data, ema1, ema2, ema3):
    if data['close'].iloc[1] < data[ema3].iloc[1]:
        if data['close'].iloc[1] < data[ema2].iloc[1]:
            if data['close'].iloc[1] < data[ema1].iloc[1]:
                return True
    return False

def buy_triple_ema_violation(data, ema1, ema2, ema3):
    if data['high'].iloc[1] > data[ema3].iloc[1]:
        if data['high'].iloc[1] > data[ema2].iloc[1]:
            if data['high'].iloc[1] > data[ema1].iloc[1]:
                return True
    return False

def sell_triple_ema_violation(data, ema1, ema2, ema3):
    if data['low'].iloc[1] < data[ema3].iloc[1]:
        if data['low'].iloc[1] < data[ema2].iloc[1]:
            if data['low'].iloc[1] < data[ema1].iloc[1]:
                return True
    return False