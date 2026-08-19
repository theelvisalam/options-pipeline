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
        r = 0.05
        if msg.topic() == 'price-ticks':
            tk = data['ticker']
            latest_prices[tk] = data['price']
        elif msg.topic() == 'publish-chain':
            current_price = latest_prices.get(data['ticker'])
            if current_price is None:
                continue
            dateNow = datetime.date.today()
            exp = datetime.datetime.strptime(data['expiration'], '%Y-%m-%d').date()
            timedelta = exp - dateNow
            T = timedelta.days / 365
            if T <= 0:
                continue
            S = current_price
            K = data['strike']
            V = data['impliedVolatility']
            greeks = black_scholes(S, K, T, V, r)
            publish_greeks(data['ticker'], K, data['expiration'], greeks)
except KeyboardInterrupt:
    sys.stderr.write('%% Aborted by user\n')
finally:
    c.close()
