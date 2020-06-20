"""
@Project: Real time stream processing of formula 1 racing car telemetry
@author: hemanth
@info: Consumers bytes from AVGSPEED topic and ingest data into influxdb
"""
import sys
import json
from confluent_kafka import Consumer, KafkaError
from influxdb import InfluxDBClient
import time

def consume():

    client = InfluxDBClient(host='ip-172-31-95-138.ec2.internal', port=8086, username='admin', password='admin', database='test')

    c = Consumer({
        'bootstrap.servers': 'ip-172-31-87-92.ec2.internal:9092,ip-172-31-92-117.ec2.internal:9092,ip-172-31-93-248.ec2.internal:9092',
        'group.id': 'influxgroup_lap',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True,
        'fetch.min.bytes': 1000000
    })

    c.subscribe(['AVGSPEED'])

    try:
        while True:
            client_write_start_time = time.perf_counter()
            msg = c.poll(0.1)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                print('Received message: {}'.format(msg.value().decode('utf-8')))
            data = json.loads(msg.value())
            json_body = [
            {
                "measurement": "AVGSPEED", #this table will be automatically created
                "tags": {"id" : data["id"]},
                "fields": data
            }
            ]

            client.write_points(json_body) #writes data into influxdb

            client_write_end_time = time.perf_counter()
            print("Client Library Write: {time}s".format(time=client_write_end_time - client_write_start_time))
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')
    finally:# Close down consumer to commit final offsets.
        c.close()

if __name__ == "__main__":
    consume()
