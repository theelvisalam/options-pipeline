from pricing import black_scholes
from kafka_producer import publish_greeks
from confluent_kafka import Consumer, KafkaException
import datetime
import logging
import sys
import json

conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'pricing-engine',
    'auto.offset.reset': 'earliest'
}

logger = logging.getLogger('consumer')
logger.setLevel(logging.DEBUG)

c = Consumer(
    conf,
    logger=logger
)

c.subscribe(['price-ticks', 'publish-chain'])

latest_prices = {}

try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        data = json.loads(msg.value().decode('utf-8'))
        print(f'Topic: {msg.topic()} | Data: {data}')
        # ---------- INPUTS --------------
        # Risk Free Rate
        r = 0.05
        if msg.topic() == 'price-ticks':
            # Ticker
            tk = data['ticker']
            # Underlying Price
            latest_prices[tk] = data['price']
        elif msg.topic() == 'publish-chain':
            current_price = latest_prices.get(data['ticker'])
            if current_price is None:
                continue
            dateNow = datetime.date.today()
            exp = datetime.datetime.strptime(data['expiration'], '%Y-%m-%d').date()
            timedelta = exp - dateNow
            # Time To Expiration in Years
            T = timedelta.days / 365
            if T <= 0:
                continue
            # Underlying Price
            S = current_price
            # Strike Price
            K = data['strike']
            # Implied Volatility
            V = data['impliedVolatility']
            greeks = black_scholes(S, K, T, V, r)
            publish_greeks(data['ticker'], K, data['expiration'], greeks)
except KeyboardInterrupt:
    sys.stderr.write('%% Aborted by user\n')
finally:
    c.close()
