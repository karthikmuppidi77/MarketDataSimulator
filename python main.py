from kafka import KafkaProducer
import json
import random
import time

producer = KafkaProducer(
    bootstrap_servers='your.kafka.server:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

stock_list = ['TCS', 'INFY', 'RELIANCE', 'HDFC']

while True:
    stock_data = {
        'ticker': random.choice(stock_list),
        'bid': round(random.uniform(1000, 2000), 2),
        'ask': round(random.uniform(2000, 3000), 2),
        'volume': random.randint(100, 1000)
    }
    print("Sending: ", stock_data)
    producer.send('stock-topic', value=stock_data)
    time.sleep(2)
