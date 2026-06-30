from math import log, sqrt, exp
from scipy.stats import norm

def black_scholes(S, K, T, V, r):
    rawD1 = (log(S / K) + (r + (V ** 2) / 2)) / (V * sqrt(T))
    rawD2 = rawD1 - V * sqrt(T)
    d1 = norm.cdf(rawD1)
    d2 = norm.cdf(rawD2)
    
    C = (S * d1) - (K * exp(-r * T)) * d2
    vega = S * norm.pdf(rawD1) * sqrt(T)
    theta = (-(S * V * norm.pdf(rawD1)) / (2 * sqrt(T))) - (r * K * exp(-r * T) * d2)
    rho = K * T * exp(-r * T) * d2
    gamma = norm.pdf(rawD1) / (S * V * sqrt(T))

    return {
        'call_price': C,
        'delta': d1,
        'theta': theta,
        'vega': vega,
        'rho': rho,
        'gamma': gamma
    }
