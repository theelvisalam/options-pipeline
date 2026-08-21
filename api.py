from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from confluent_kafka import Consumer
import threading
import json

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins = ["http://localhost:5173"],
    allow_methods = ["GET"],
    allow_headers = ["*"]
)

latest_greeks = {}

def consume_loop():
    conf = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'api-cache',
        'auto.offset.reset': 'latest'
    }
    c = Consumer(conf)
    c.subscribe(['computed-greeks'])

    while True:
        msg = c.poll(1.0)
        if msg is None or msg.error():
            continue
        data = json.loads(msg.value().decode('utf-8'))
        key = f"{data['ticker']}-{data['strike']}-{data['expiration']}"
        latest_greeks[key] = data

@app.on_event("startup")
def start_background_consumer():
    thread = threading.Thread(target=consume_loop, daemon=True)
    thread.start()

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/greeks/{ticker}")
def get_greeks(ticker: str):
    matches = {k: v for k, v in latest_greeks.items() if v['ticker'] == ticker}
    return matches
