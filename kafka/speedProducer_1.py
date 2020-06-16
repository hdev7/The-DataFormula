"""
@Project: Real time stream processing of formula 1 racing car telemetry
@author: hemanth
"""
import sys
import confluent_kafka
import csv
from json import dumps
import time

class Producer(object):

    def __init__(self):
        self.producer = confluent_kafka.Producer({'bootstrap.servers': 'ip-172-31-87-92.ec2.internal:9092,ip-172-31-92-117.ec2.internal:9092,ip-172-31-93-248.ec2.internal:9092',
                                                  'linger.ms': 100})
        self.kafka_topic = 'speed'

    def create_msg(self, id, speed):#, rpm, e_temp, b_temp, ts_temp, ti_temp, t_pressure):
        """
        Args:
            id: str,    //id of the car (each team has 2 cars)
            speed: int, // Speed of car in kilometres per hour
        :rtype: dict
        """
        msg = {}
        #msg['time'] = t
        #msg['millis'] = millis
        msg["id"] = id
        msg["speed"] = speed
        # msg["rpm"] = rpm
        # msg["e_temp"] = e_temp
        # msg["b_temp"] = b_temp
        # msg["ts_temp"] = ts_temp
        # msg["ti_temp"] = ti_temp
        # msg["t_pressure"] = t_pressure
        return msg

    def produce_msgs(self):
        try:
            while True:

                    with open('telemetry.txt') as file:
                        reader = csv.DictReader(file)
                        for i in reader:
                            #t = time.strftime('%Y-%m-%dT%H:%M:%S')
                            client_write_start_time = time.perf_counter()
                            #millis = "%.3d" % (time.time() % 1 * 1000)
                            msg = dumps(self.create_msg('1', i['m_speed']))
                            msg = msg.encode('utf-8')
                            self.producer.produce(self.kafka_topic, msg)
                            #flush is inside: synchronous, otherwise asynchronous
                            self.producer.flush()
                            client_write_end_time = time.perf_counter()
                            print("Client Library Write: {time}s".format(time=client_write_end_time - client_write_start_time))
                            # Wait for 100 milliseconds
                            time.sleep(.100)
        except KeyboardInterrupt:
            sys.stderr.write('%% Aborted by user\n')




if __name__ == "__main__":
    prod = Producer()
    prod.produce_msgs()
