import yfinance as yf
import asyncio

def message_handler(message):
    print('Recieved Message:', message)

async def main():
    tk_list = [
        'QQQ', 
        'SPY'
    ]
    tk_data = {}
    tk_exp = {}
    tk_chain = []

#   get ticker data 
    for tk in tk_list:
        tk_data[tk] = yf.Ticker(tk)
    print(tk_data)
    print("--------")
#   get options expiration dates 
    for tk, data in tk_data.items():
        tk_exp[tk] = data.options
    print(tk_exp)
    print("--------")
#   get options chains
    for tk in tk_list:
        for x in tk_exp[tk]:
            tk_chain.append(tk_data[tk].option_chain(x))
    print(tk_chain)
    print("--------")

    # def options_expirations(ticker_list):
        # for t in ticker_list:
            # expirations = t.options
        # return expirations
    # options_expirations(tickers)
    
    async with yf.AsyncWebSocket() as ws:
        await ws.subscribe(
            tk_list
        )
        await ws.listen(message_handler)
        
asyncio.run(main())

