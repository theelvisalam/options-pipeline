from kafka_producer import publish_price_tick
import yfinance as yf

def message_handler(message):
    print('Recieved Message:', message)
    if 'price' in message and 'id' in message:
        publish_price_tick(message['id'], message['price'])

async def start_stream(tk_list):
    async with yf.AsyncWebSocket() as ws:
        await ws.subscribe(tk_list)
        await ws.listen(message_handler)
