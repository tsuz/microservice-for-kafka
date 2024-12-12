#!/bin/bash

set -x

BOOTSTRAP_SERVER=localhost:9092
TOPIC=users-20241201-1624
CONSUMER_GROUP=perf-test

kafka-consumer-groups \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --list
  
