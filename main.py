from kafka_producer import publish_chain
from tickers import get_tickers, get_expirations, get_chains
from websocket import start_stream
import asyncio
import threading

tk_list = ['QQQ', 'SPY', 'SNDK']

tk_data = get_tickers(tk_list)
tk_exp = get_expirations(tk_data)
tk_chain = get_chains(tk_list, tk_data, tk_exp)

threading.Thread(target=publish_chain, args=(tk_chain,), daemon=True).start()

asyncio.run(start_stream(tk_list))
