#!/bin/bash
for f in kafka/topic/*.py; do
  python3 "$f" &
done

for f in kafka/*.py; do
  python3 "$f" &
done
