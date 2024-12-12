#!/bin/bash

TOPIC=users-20241201-1624

kafka-topics \
    --bootstrap-server localhost:9092 \
    --describe \
    --topic $TOPIC
