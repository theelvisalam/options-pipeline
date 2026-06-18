import yfinance as yf
import asyncio

# qqq = yf.Ticker('QQQ')

# info = qqq.info
# for k, v in info.items():
    # print(k,v)

def message_handler(message):
    print('Recieved Message:', message)


async def main():

    async with yf.AsyncWebSocket() as ws:
        await ws.subscribe(
            ['QQQ',
             'SPY']
        )
        await ws.listen()

asyncio.run(main())
# {'id': 'QQQ', 'price': 735.975, 'time': '1781797426000', 'exchange': 'NGM', 'quote_type': 20, 'market_hours': 1, 'change_percent': 1.8636373, 'day_volume': '18299202', 'change': 13.464966, 'price_hint': '2'}

