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

    # Create a Decimal from the input number
    d = Decimal(str(n))
    # Define the rounding format based on the number of decimals
    rounding_format = '1.' + '0' * decimals
    # Perform the rounding
    rounded_value = d.quantize(Decimal(rounding_format), rounding=ROUND_HALF_UP)
    return float(rounded_value)
    
    multiplier = 10 ** decimals
    # return float(Decimal(n).scaleb(decimals).quantize(Decimal('1'), rounding=ROUND_HALF_UP).scaleb(-decimals))

    # Use Decimal for precise rounding
    d = Decimal(n).quantize(Decimal('1.' + '0' * decimals), rounding=ROUND_HALF_UP)
    return float(d) if decimals == 0 else d

