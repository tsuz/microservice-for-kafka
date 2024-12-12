#!/bin/bash

set -x

BOOTSTRAP_SERVER=localhost:9092
TOPIC=users-20241201-1624
CONSUMER_GROUP=perf-test

kafka-console-consumer \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --topic $TOPIC \
  --group $CONSUMER_GROUP \
  --property "print.key=true" \
  --property "print.partition=true" \
  --property "print.offset=true" \
  --property "print.timestamp=true" \
  --from-beginning

