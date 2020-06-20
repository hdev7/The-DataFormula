import time
from kafka import KafkaProducer

msg = ('kafkakafkakafka' * 20).encode()[:100]
size = 1000000
producer = KafkaProducer(bootstrap_servers='localhost:9092')

def kafka_python_producer_sync(producer, size):
    for _ in range(size):
        future = producer.send('topic', msg)
        result = future.get(timeout=60)
    producer.flush()

def success(metadata):
    print(metadata.topic)

def error(exception):
    print(exception)

def kafka_python_producer_async(producer, size):
    for _ in range(size):
        producer.send('topic', msg).add_callback(success).add_errback(error)
    producer.flush()
