"""
@Project: Real time stream processing of formula 1 racing car telemetry
@author: hemanth
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
        'group.id': 'influxgroup_telemetry',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True,
        'auto.commit.interval.ms': 100
    })

    c.subscribe(['telemetry'])

    try:
        while True:
            #print('Waiting for message..')
            msg = c.poll(0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue#print('Received message: {}'.format(msg.value().decode('utf-8')))
            data = json.loads(msg.value())
            json_body = [
            {
                "measurement": "telemetry", #this table will be automatically created
                "tags": {"id" : data["id"]},
                "fields": data
            }
            ]
            client_write_start_time = time.perf_counter()

            client.write_points(json_body)

            client_write_end_time = time.perf_counter()
            print("Client Library Write: {time}s".format(time=client_write_end_time - client_write_start_time))
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')
    finally:# Close down consumer to commit final offsets.
        c.close()

if __name__ == "__main__":
    consume()
