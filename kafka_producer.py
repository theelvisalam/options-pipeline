from confluent_kafka import Producer
import json
p = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report():
    if err is not None:
        print(f'Message Delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic} [{msg.partition}]')

def publish_chain(tk_chain):
    for chain in tk_chain:
        for _, row in chain.calls.iterrows():
            '''
            ROWS IN chain.calls
            contractSymbol
            lastTradeDate
            strike
            lastPrice
            bid
            ask
            change
            percentChange
            volume
            openInterest
            impliedVolatility
            inTheMoney
            contractSize
            currency
            '''
            message = {
                'type': 'call',
                'expiration': str(row.name),
                'strike': row['strike'],
                'bid': row['bid'],
                'ask': row['ask'],
                'impliedVolatility': row['impliedVolatility'],
                'volume': row['volume']
            }
            p.produce(
                'publish-chain',
                key = str(row['strike']),
                value = json.dumps(message),
                callback = delivery_report

            )
            p.flush()

def publish_price_tick(ticker, price):
    message = {
        'ticker': ticker,
        'price': price
    }
    p.produce(
        'price-ticks',
        key = ticker,
        value = json.dumps(message),
        callback = delivery_report
    )
    p.flush()

