from tickers import get_tickers, get_expirations, get_chains
import yfinance as yf
import asyncio

tk_list = ['QQQ']

tk_data = get_tickers(tk_list)
tk_exp = get_tickers(tk_data)
tk_chain = get_chains(tk_list, tk_data, tk_exp)

def message_handler(message):
    print('Recieved Message:', message)

async def main(): 
    async with yf.AsyncWebSocket() as ws:
        await ws.subscribe(tk_list)
        await ws.listen(message_handler)
        
