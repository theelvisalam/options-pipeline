from pricing import black_scholes
from kafka_producer import publish_greeks
from confluent_kafka import Consumer, KafkaException, KafkaError
import datetime
import logging
import sys
import json

conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'pricing-engine',
    'auto.offset.reset': 'earliest',
    'topic.metadata.refresh.interval.ms': 5000
}

logger = logging.getLogger('consumer')
logger.setLevel(logging.DEBUG)

c = Consumer(conf, logger=logger)
c.subscribe(['price-ticks', 'publish-chain'])

latest_prices = {}
latest_chains = {}

def try_compute(ticker, K, expiration, current_price, V, r=0.05):
    dateNow = datetime.date.today()
    exp = datetime.datetime.strptime(expiration, '%Y-%m-%d').date()
    T = (exp - dateNow).days / 365
    if T <= 0 or V <= 0 or K <= 0 or current_price <= 0:
        return
    try:
        greeks = black_scholes(current_price, K, T, V, r)
        publish_greeks(ticker, K, expiration, greeks)
    except (ValueError, ZeroDivisionError) as e:
        print(f'Skipping bad input: {e}')

try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                continue
            raise KafkaException(msg.error())
        data = json.loads(msg.value().decode('utf-8'))
        print(f'Received: {msg.topic()}')
        if msg.topic() == 'price-ticks':
            tk = data['ticker']
            latest_prices[tk] = data['price']
            for K, expiration, V in latest_chains.get(tk, []):
                try_compute(tk, K, expiration, data['price'], V)
        elif msg.topic() == 'publish-chain':
            tk = data['ticker']
            K = data['strike']
            expiration = data['expiration']
            V = data['impliedVolatility']
            latest_chains.setdefault(tk, []).append((K, expiration, V))
            current_price = latest_prices.get(tk)
            if current_price is not None:
                try_compute(tk, K, expiration, current_price, V)
except KeyboardInterrupt:
    sys.stderr.write('%% Aborted by user\n')
finally:
    c.close()
