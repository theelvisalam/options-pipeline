from confluent_kafka import Producer

p = Producer({'bootstrap.servers': 'broker1, broker2'})

def delivery_report():
    if err is not None:
        print(f'Message Delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic} [{msg.partition}]')

def publish_chains(tk_chain):
    for chain in tk_chain:
        


