#!/bin/bash

set -x

# Configuration variables
BOOTSTRAP_SERVER=localhost:9092
TOPIC=users-20241201-1624
PAYLOAD_FILE=/Users/takutosuzuki/Documents/kafka-as-a-microservice/cmds/users.jsonl.18gb
NUM_ROWS=10000000  # Number of rows to process

# Check if input file exists
if [ ! -f "$PAYLOAD_FILE" ]; then
    echo "Error: Input file $PAYLOAD_FILE does not exist"
    exit 1
fi

# Function to check if required commands are available
check_requirements() {
    for cmd in kafka-console-producer head; do
        if ! command -v $cmd &> /dev/null; then
            echo "Error: $cmd could not be found"
            exit 1
        fi
    done
}

# Function to handle cleanup on script termination
cleanup() {
    echo "Cleaning up..."
    jobs -p | xargs -r kill
    exit 0
}

# Set up cleanup trap
trap cleanup SIGINT SIGTERM EXIT

# Check prerequisites
check_requirements

# Stream the first 10 million rows to Kafka
head -n "$NUM_ROWS" "$PAYLOAD_FILE" | \
kafka-console-producer \
    --broker-list "$BOOTSTRAP_SERVER" \
    --topic "$TOPIC" \
    --property "parse.key=true" \
    --property "key.separator=;" \
    --producer-property batch.size=2000000 \
    --producer-property linger.ms=5 \
    --producer-property max.request.size=2000000 \
    --producer-property compression.type=lz4

# Check if producer completed successfully
if [ $? -eq 0 ]; then
    echo "Successfully processed first $NUM_ROWS rows"
else
    echo "Error processing file"
    exit 1
fi

echo "Processing completed successfully"