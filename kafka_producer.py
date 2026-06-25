from confluent_kafka import Producer

p = Producer({'bootstrap.servers': 'broker1, broker2'})

def delivery_report():
    if err is not None:
        print(f'Message Delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic} [{msg.partition}]')

def publish_price_tick(ticker, price):
    message = {'ticker': ticker, 'price': price}
    p.produce(
        'price-ticks',
        key = ticker,
        value = json.dumps(message)
        callback = delivery_report
    )
    producer.flush()


