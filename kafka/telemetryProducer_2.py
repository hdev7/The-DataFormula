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
        self.kafka_topic = 'telemetry'

    def create_msg(self, id, m_rpm, m_throttle, m_steer, m_brake, m_clutch, m_gear, m_drs, e_temp, b_temp, ts_temp, ti_temp, t_pressure):
        """
        Args:
            str       id                  // id of the car (each team has 2 cars)
            int       rpm;                // Engine RPM
            float     throttle;           // Amount of throttle applied (0.0 to 1.0)
            float     m_steer;            // Steering (-1.0 (full lock left) to 1.0 (full lock right))
            float     m_brake;            // Amount of brake applied (0.0 to 1.0)
            int       m_clutch;           // Amount of clutch applied (0 to 100)
            int       m_gear;             // Gear selected (1-8, N=0, R=-1)
            int       m_drs;              // 0 = off, 1 = on
            int       e_temp;             // Engine temperature (celsius)
            int       b_temp;             // Brakes temperature (celsius)
            int       ts_temp;            // Tyres surface temperature (celsius)
            uint16    ti_tenp;            // Tyres inner temperature (celsius)
            float     t_pressure;         // Tyres pressure (PSI)
        :rtype: dict
        """
        msg = {}
        #msg['time'] = t
        #msg['millis'] = millis
        msg["id"] = id
        msg["m_rpm"] = m_rpm
        msg['m_throttle'] = m_throttle
        msg['m_steer'] = m_steer
        msg['m_brake'] = m_brake
        msg['m_clutch'] = m_clutch
        msg['m_gear'] = m_gear
        msg['m_drs'] = m_drs
        msg["e_temp"] = e_temp
        msg["b_temp"] = b_temp
        msg["ts_temp"] = ts_temp
        msg["ti_temp"] = ti_temp
        msg["t_pressure"] = t_pressure
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
                            #msg = dumps(self.create_msg('1', i['m_speed']))
                            msg = dumps(self.create_msg('2', i['m_engineRPM'], i['m_throttle'], i['m_steer'], i['m_brake'], i['m_clutch'], i['m_gear'], i['m_drs'], i['m_engineTemperature'], i['m_brakesTemperature_RL'], i['m_tyresSurfaceTemperature_RL'], i['m_tyresInnerTemperature_RL'], i['m_tyresPressure_RL']))
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
