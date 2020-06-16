#!/bin/bash

for f in kafka/*.py; do
  python3 "$f" &
done
