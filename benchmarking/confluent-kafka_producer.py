from confluent_kafka import Producer
from python_kafka import Timer

producer = Producer({'bootstrap.servers': 'localhost:9092'})
msg = ('kafkatest' * 20).encode()[:100]
size = 1000000

def delivery_report(err, decoded_message, original_message):
    if err is not None:
        print(err)

def confluent_producer_async():
    for _ in range(size):
        producer.produce(
            "topic1",
            msg,
            callback=lambda err, decoded_message, original_message=msg: delivery_report(  # noqa
                err, decoded_message, original_message
            ),
        )
    producer.flush()

def confluent_producer_sync():
    for _ in range(100000):
        producer.produce(
            "topic1",
            msg,
            callback=lambda err, decoded_message, original_message=msg: delivery_report(  # noqa
                err, decoded_message, original_message
            ),
        )
        producer.flush()
