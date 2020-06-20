from confluent_kafka import Consumer, TopicPartition
size = 1000000

consumer = Consumer(
    {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'mygroup',
        'auto.offset.reset': 'earliest',
    }
)

def consume_session_window(consumer, timeout=1, session_max=5):
    session = 0
    while True:
        message = consumer.poll(timeout)
        if message is None:
            session += 1
            if session > session_max:
                break
            continue
        if message.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        yield message
    consumer.close()

def consume(consumer, timeout):
    while True:
        message = consumer.poll(timeout)
        if message is None:
            continue
        if message.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        yield message
    consumer.close()

def confluent_consumer():
    consumer.subscribe(['topic1'])
    for msg in consume(consumer, 1.0):
        print(msg)

def confluent_consumer_partition():
    consumer.assign([TopicPartition("topic1", 0)])
    for msg in consume(consumer, 1.0):
        print(msg)
