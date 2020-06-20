"""
@Project: Real time stream processing of formula 1 racing car telemetry
@author: hemanth
@info: Generates speed data to kafka topic (car.id=1)
"""
import sys
import confluent_kafka
import csv
from json import dumps
import time
from confluent_kafka.admin import AdminClient, NewTopic

class Producer(object):

    def __init__(self):
        self.producer = confluent_kafka.Producer({'bootstrap.servers': 'ip-172-31-87-92.ec2.internal:9092,ip-172-31-92-117.ec2.internal:9092,ip-172-31-93-248.ec2.internal:9092',
                                                  'linger.ms':1, 'batch.num.messages':5000, 'acks':2, 'compression.type': None})
        self.kafka_topic = 'speed'


    def create_msg(self, id, speed):
        """
        Generates the message structure for speed topic
        :param id: id of the car (each team has 2 cars)
        :param speed: Speed of car in kilometres per hour
        :type id: int
        :type speed: int
        :returns: message in a key value pair
        :rtype: dict
        """
        msg = {}
        msg["id"] = id
        msg["speed"] = speed
        return msg

    def produce_msgs(self):
        try:
            while True:

                    with open('telemetry.txt') as file:
                        reader = csv.DictReader(file)
                        for i in reader:
                            client_write_start_time = time.perf_counter()
                            msg = dumps(self.create_msg('1', i['m_speed']))
                            msg = msg.encode('utf-8')
                            try:
                                self.producer.produce(self.kafka_topic, msg)
                                #using poll for asynchronous communication: improves speed of ingestion
                                self.producer.poll(0)
                            except BufferError as e:
                                print("Buffer full")
                                producer.produce(self.kafka_topic, msg)
                                producer.poll(0.1)
                            time.sleep(.001)  #use case requirement: wait for 1ms
                            client_write_end_time = time.perf_counter()
                            print("Client Library Write: {time}s".format(time=client_write_end_time - client_write_start_time))
                    #Wait until the bytes in kafka buffer gets flushed
                    self.producer.flush()
        except KeyboardInterrupt:
            sys.stderr.write('%% Aborted by user\n')




if __name__ == "__main__":
    prod = Producer()
    prod.produce_msgs()
