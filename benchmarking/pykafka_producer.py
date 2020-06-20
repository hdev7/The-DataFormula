from pykafka import KafkaClient
client = KafkaClient(hosts="localhost:9092")
topic = client.topics[b'topic1']
msg = ('kafkatest' * 20).encode()[:100]

def pykafka_producer_sync(size):
    with topic.get_sync_producer() as producer:
        for i in range(size):
            producer.produce(msg)
        producer.stop()

def pykafka_producer_async(size):
    with topic.get_producer() as producer:
        for i in range(size):
            producer.produce(msg)
        producer.stop()

def pykafka_producer_async_report(size):
    with topic.get_producer(delivery_reports=True, min_queued_messages=1) as producer:
        for i in range(size):
            producer.produce(msg)
            while True:
                try:
                    report, exc = producer.get_delivery_report(block=False)
                    print(report)
                except Exception:
                    break
        producer.stop()
