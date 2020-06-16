#!/bin/bash

for f in influxdb_consumers/*.py; do
  python3 "$f" &
done
