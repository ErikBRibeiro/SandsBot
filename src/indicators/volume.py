def is_above_average(data, period=14):
    if data['volume'].iloc[-2] > data['volume'].rolling(period).mean().iloc[-2]:
        return True
    return False