from pykafka import KafkaClient
from pykafka.simpleconsumer import OffsetType

client = KafkaClient(hosts="localhost:9092")
topic = client.topics[b'topic1']

def pykafka_consumer():
    consumer = topic.get_simple_consumer(consumer_group="mygroup", auto_offset_reset=OffsetType.EARLIEST)
    for message in consumer:
        print(message)
