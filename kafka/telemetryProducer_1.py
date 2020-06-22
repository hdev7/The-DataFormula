"""
@Project: Real time stream processing of formula 1 racing car telemetry
@author: hemanth
@info: Generates telemetry data to kafka topic (car.id=1)
"""
import sys
import confluent_kafka
import csv
from json import dumps
import time
from confluent_kafka.admin import AdminClient, NewTopic
import config

class Producer(object):

    def __init__(self):
        self.producer = confluent_kafka.Producer({'bootstrap.servers': config.kafka_servers,
                                                  'linger.ms':1, 'batch.num.messages':5000, 'acks':2, 'compression.type': None})
        self.kafka_topic = 'telemetry'

    def create_msg(self, id, m_rpm, m_throttle, m_steer, m_brake, m_clutch, m_gear, m_drs, e_temp, b_temp, ts_temp, ti_temp, t_pressure):
        """
        Generates the message structure for telemetry topic
        :param id: id of the car (each team has 2 cars)
        :param rpm: Engine RPM
        :param throttle: Amount of throttle applied (0.0 to 1.0)
        :param m_steer: Steering (-1.0 (full lock left) to 1.0 (full lock right))
        :param m_brake: Amount of brake applied (0.0 to 1.0)
        :param m_clutch: Amount of clutch applied (0 to 100)
        :param m_gear: Gear selected (1-8, N=0, R=-1)
        :param m_drs: 0 = off, 1 = on
        :param e_temp: Engine temperature (celsius)
        :param b_temp: Brakes temperature (celsius)
        :param ts_temp: Tyres surface temperature (celsius)
        :param ti_temp: Tyres inner temperature (celsius)
        :param t_pressure: Tyres pressure (PSI)
        :type id: int
        :type rpm: int
        :type throttle: float
        :type m_steer: float
        :type m_brake: float
        :type m_clutch: int
        :type m_gear: int
        :type m_drs: int
        :type e_temp: int
        :type b_temp: int
        :type ts_temp: int
        :type ti_tenp: int
        :type t_pressure: float
        :returns: message in a key value pair
        :rtype: dict
        """
        msg = {}
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
                            client_write_start_time = time.perf_counter()
                            msg = dumps(self.create_msg('1', i['m_engineRPM'], i['m_throttle'], i['m_steer'], i['m_brake'], i['m_clutch'], i['m_gear'], i['m_drs'], i['m_engineTemperature'], i['m_brakesTemperature_RL'], i['m_tyresSurfaceTemperature_RL'], i['m_tyresInnerTemperature_RL'], i['m_tyresPressure_RL']))
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
