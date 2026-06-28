from confluent_kafka import Consumer, KafkaExeption
import sys

c = Consumer({
    'boostrap.servers': '127.0.0.1',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['price-ticks', 'publish-chain'])

try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
except KeyboardInterrupt:
    sys.stderr.write('%% Aborted by user\n')
finally:
    c.close()
