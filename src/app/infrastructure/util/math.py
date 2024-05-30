from decimal import Decimal, ROUND_HALF_UP
# import math

def normal_round(n, decimals=0):
    # # https://stackoverflow.com/a/52617883
    # try:
    #     expoN = n * 10 ** decimals
    #     if abs(expoN) - abs(math.floor(expoN)) < 0.5:
    #         return math.floor(expoN) / 10 ** decimals
    #     return math.ceil(expoN) / 10 ** decimals
    # except Exception as e:
    #     return n  # TODO: is this exception handling granular enough? Intended to handle NaN etc
    
    multiplier = 10 ** decimals
    return float(Decimal(n).scaleb(decimals).quantize(Decimal('1'), rounding=ROUND_HALF_UP).scaleb(-decimals))

