from confluent_kafka import Producer
import json

producer = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    if err:
        print(f'Delivery failed: {err}')
    else:
        print(f'Message sent to {msg.topic()} [{msg.partition()}]')

producer.produce(
    'test-topic',
    value=json.dumps({'message': 'Kafka is working!'}).encode('utf-8'),
    callback=delivery_report
)

producer.flush()