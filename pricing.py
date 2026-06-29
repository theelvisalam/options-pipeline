import math
def black_scholes(S, K, T, V, r):
    d1 = math.log((S / K)) + (r + ((V ** 2) / 2)) * (T)
    d2 = d1 - (V)
