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
        self.kafka_topic = 'lap'

    def create_msg(self, id, m_lastLapTime, m_currentLapTime, m_bestLapTime, m_lapDistance, m_totalDistance, m_currentLapNum):
        """
        Args:
            float       m_lastLapTime;	       	         // Last lap time in seconds
            float       m_currentLapTime;	             // Current time around the lap in seconds
            float       m_bestLapTime;		             // Best lap time of the session in seconds
            float       m_lapDistance;		            // Distance vehicle is around current lap in metres – could
            float       m_totalDistance;		        // Total distance travelled in session in metres – could
            int         m_currentLapNum;		        // Current lap number
        :rtype: dict
        """
        msg = {}
        #msg['time'] = t
        #msg['millis'] = millis
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
                            #t = time.strftime('%Y-%m-%dT%H:%M:%S')
                            client_write_start_time = time.perf_counter()
                            #millis = "%.3d" % (time.time() % 1 * 1000)
                            #msg = dumps(self.create_msg('1', i['m_speed']))
                            msg = dumps(self.create_msg('2', i['m_lastLapTime'], i['m_currentLapTime'], i['m_bestLapTime'], i['m_lapDistance'], i['m_totalDistance'], i['m_currentLapNum']))
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
