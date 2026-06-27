from confluent_kafka import Consumer

c = Consumer({
    'boostrap.servers': '127.0.0.1',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['price-ticks'])

while True:
    msg = c.poll(1.0)

