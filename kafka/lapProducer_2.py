"""
@Project: Real time stream processing of formula 1 racing car telemetry
@author: hemanth
@info: Generates lap data to kafka topic (car.id=2)
"""
import sys
import confluent_kafka
import csv
from json import dumps
import time


class Producer(object):

    def __init__(self):

        self.producer = confluent_kafka.Producer({'bootstrap.servers': 'ip-172-31-87-92.ec2.internal:9092,ip-172-31-92-117.ec2.internal:9092,ip-172-31-93-248.ec2.internal:9092',
                                                  'linger.ms':1, 'batch.num.messages':5000, 'acks':2, 'compression.type': None})
        self.kafka_topic = 'lap'


    def create_msg(self, id, m_lastLapTime, m_currentLapTime, m_bestLapTime, m_lapDistance, m_totalDistance, m_currentLapNum):
        """
        Generates the message structure for lap topic
        :param id: id of the car (each team has 2 cars)
        :param m_lastLapTime: Last lap time in seconds
        :param m_currentLapTime: Current time around the lap in seconds
        :param m_bestLapTime: Best lap time of the session in seconds
        :param m_lapDistance: Distance vehicle is around current lap in metres
        :param m_totalDistance: Total distance travelled in session in metres
        :param m_currentLapNum: Current lap number
        :type id: int
        :type m_lastLapTime: float
        :type m_currentLapTime: float
        :type m_bestLapTime: float
        :type m_lapDistance: float
        :type m_totalDistance: float
        :type m_currentLapNum: int
        :returns: message in a key value pair
        :rtype: dict
        """
        msg = {}
        msg["id"] = id
        msg["m_lastLapTime"] = m_lastLapTime
        msg['m_currentLapTime'] = m_currentLapTime
        msg['m_bestLapTime'] = m_bestLapTime
        msg['m_lapDistance'] = m_lapDistance
        msg['m_totalDistance'] = m_totalDistance
        msg['m_currentLapNum'] = m_currentLapNum
        return msg

    def produce_msgs(self):
        try:
            while True:

                    with open('lap.txt') as file:
                        reader = csv.DictReader(file)
                        for i in reader:
                            client_write_start_time = time.perf_counter()
                            msg = dumps(self.create_msg('2', i['m_lastLapTime'], i['m_currentLapTime'], i['m_bestLapTime'], i['m_lapDistance'], i['m_totalDistance'], i['m_currentLapNum']))
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
