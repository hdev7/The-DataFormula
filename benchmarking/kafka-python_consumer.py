from kafka import KafkaConsumer, TopicPartition

size = 1000000

consumer1 = KafkaConsumer(bootstrap_servers='localhost:9092')
def kafka_python_consumer1():
    consumer1.subscribe(['topic1'])
    for msg in consumer1:
      print(msg)

consumer2 = KafkaConsumer(bootstrap_servers='localhost:9092')
def kafka_python_consumer2():
    consumer2.assign([TopicPartition('topic1', 1), TopicPartition('topic2', 1)])
    for msg in consumer2:
        print(msg)

consumer3 = KafkaConsumer(bootstrap_servers='localhost:9092')
def kafka_python_consumer3():
    partition = TopicPartition('topic3', 0)
    consumer3.assign([partition])
    last_offset = consumer3.end_offsets([partition])[partition]
    for msg in consumer3:
        if msg.offset == last_offset - 1:
            break
