"""
@Project: Real time stream processing of formula 1 racing car telemetry
@author: hemanth
@info: Consumers bytes from lap topic and ingest data into influxdb
"""
import sys
import json
from confluent_kafka import Consumer, KafkaError
from influxdb import InfluxDBClient
import time
import config

def consume():

    client = InfluxDBClient(host=config.influxdb_servers, port=config.influxdb_port, username=config.infuxdb_user, password=config.influxdb_pass, database=config.influxdb_database)

    c = Consumer({
        'bootstrap.servers': config.kafka_servers,
        'group.id': 'influxgroup_lap',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True,
        'fetch.min.bytes': 1000000
    })

    c.subscribe(['lap'])

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
                "measurement": "lap", #this table will be automatically created
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
