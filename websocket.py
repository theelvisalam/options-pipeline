import yfinance as yf
import asyncio

def message_handler(message):
    print('Recieved Message:', message)

async def main():
    tk_list = [
        'QQQ', 
        'SPY'
    ] 
    async with yf.AsyncWebSocket() as ws:
        await ws.subscribe(
            tk_list
        )
        await ws.listen(message_handler)
        
asyncio.run(main())

