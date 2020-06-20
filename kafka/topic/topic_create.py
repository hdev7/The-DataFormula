"""
@Project: Real time stream processing of formula 1 racing car telemetry
@author: hemanth
@info: Creates topics with the necessary num_partitions
"""
from confluent_kafka.admin import AdminClient, NewTopic
import time


class Create():
    def __init__(self):

        self.admin_client = AdminClient({'bootstrap.servers': 'ip-172-31-87-92.ec2.internal:9092,ip-172-31-92-117.ec2.internal:9092,ip-172-31-93-248.ec2.internal:9092'})
        self.topic_list = []
        self.topic_list.append(NewTopic(topic="speed", num_partitions=2,replication_factor=1))
        self.topic_list.append(NewTopic(topic="lap", num_partitions=3,replication_factor=1))
        self.topic_list.append(NewTopic(topic="telemetry", num_partitions=5,replication_factor=1))

    def create_topic(self):
        self.admin_client.create_topics(self.topic_list)
        time.sleep(3)


if __name__ == "__main__":
    create = Create()
    create.create_topic()
