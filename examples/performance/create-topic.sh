#!/bin/bash

TOPIC=users-20241201-1624

kafka-topics \
    --bootstrap-server localhost:9092 \
    --create \
    --topic $TOPIC \
    --partitions 12 \
    --config cleanup.policy=compact