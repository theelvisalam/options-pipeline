import yfinance as yf

def get_tickers(tk_list):
    tk_data = {}
    for tk in tk_list:
        tk_data[tk] = yf.Ticker(tk)
    return tk_data

def get_expirations(tk_data):
    tk_exp = {}
    for tk, data in tk_data.items():
        tk_exp[tk] = data.options
    return tk_exp

def get_chains(tk_list, tk_data, tk_exp): 
    tk_chain = []    
    for tk in tk_list:
        for x in tk_exp[tk]:
            tk_chain.append(tk_data[tk].option_chain(x))
    return tk_chain
