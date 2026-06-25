import yfinance as yf

def message_handler(message):
    print('Recieved Message:', message)

async def start_stream(tk_list):
    async with yf.AsyncWebSocket() as ws:
        await ws.subscribe(tk_list)
        await ws.listen(message_handler)
