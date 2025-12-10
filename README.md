
# Microservice for Kafka

Streamline data delivery from Kafka topics to your clients with a flexible, last-mile microservice solution using OpenAPI specification.

[![CircleCI](https://dl.circleci.com/status-badge/img/circleci/HzVo2vtpApgYVjMrhXoMPg/XFo6qMvVan1cv4RiYzoGk2/tree/main.svg?style=svg)](https://dl.circleci.com/status-badge/redirect/circleci/HzVo2vtpApgYVjMrhXoMPg/XFo6qMvVan1cv4RiYzoGk2/tree/main)


# Motivation

Key motivations for this project:

1. Cost Efficiency: Traditional microservices often incur significant costs due to the need for individual Redis instances and data stores for each service. Expect multitude of cost savings.
2. Language Flexibility: While Java supports GlobalKTable or KTable, many other programming languages lack this functionality, limiting options for developers.
3. Reduced Development Overhead: Building microservices typically involves substantial effort in implementing monitoring, logging, and business logic for each service.
4. Simplified Architecture: This project aims to provide a streamlined solution that addresses these challenges, potentially reducing both development time and operational costs. A very high level architecture looks like below:

![with and without this library](https://github.com/user-attachments/assets/9ecdcc34-5884-4aeb-bfef-e5680e9cc66f)

# Examples

## Get all items

**Config**

```yaml

kafka:
  application.id: kafka-streams-101
  bootstrap.servers: localhost:9092
  
  # Schema Registry Properties
  schema.registry.url: http://localhost:8081
  basic.auth.credentials.source: USER_INFO
  basic.auth.user.info: username:password

  metrics.recording.level: DEBUG


paths:
  /flights:
    get:
      description: Return all flights
      kafka:
        topic: flight-location
        query:
          method: all
        serializer:
          key: string
          value: avro
      responses:
        '200':
          description: A list of flights
          content:
            application/json:
              schema:
                type: array
```



## Get item by ID

**Config**

```yaml
kafka:
  application.id: kafka-streams-101
  bootstrap.servers: localhost:9092

  # Schema Registry Properties
  schema.registry.url: http://localhost:8081
  basic.auth.credentials.source: USER_INFO
  basic.auth.user.info: username:password

  metrics.recording.level: DEBUG

paths:
  /flights/{flightId}:
    parameters:
    - name: flightId
      in: path
      description: the flight identifier
    get:
      kafka:
        topic: flight-location
        serializer:
          key: string
          value: avro
        query:
          method: get
          key: ${parameters.flightId}
      responses:
        '200':
          description:
          content:
            application/json:
              schema:
                type: object
```



# Performance Benchmarks

We ran some load tests to see how the system performs with real-world data volumes and access patterns.

## Setup

- 10M unique keys in input topic (~0.9GB total message size)
- Endpoint: `/user/:userId` doing random lookups across the 10M keys
- 20 concurrent threads making requests for over a minute
- The hardware runs a single REST API server on MacBook Air with Apple M1 chip
- RocksDB state store size is 50MB
- The client runs on the same machine as the server (very little network latency)

## Results

- The library handled around 7,000 requests/second total
- The RocksDB `.get()` operations averaged 0.12ms per each lookup
- The latency from the client's point of view was 3 ms (50%), 5ms (95%), 8ms (99%)

The results will look better if all messages fit within memory.

Also, the results will vary based on hardware, query method, and disk type.



# Features

**API Endpoint Type**

| Endpoint type | Status |
|--|--|
| List all items | ✅
| Get single item | ✅


**Data Type (Key)**
| Data Type | Status |
|--|--|
| String | ✅
| Avro | 
| Int | 


**Data Type (Value)**
| Data Type | Status |
|--|--|
| String (JSON) | ✅
| JSON SR | ✅
| Avro | ✅
| Protobuf | ✅



## How to Run

### 1. Create a configuration file

```sh
git clone git@github.com:tsuz/microservice-for-kafka.git
cd microservice-for-kafka
mkdir -p configuration
vim configuration/config.yaml
```

Paste this to **config.yaml**
```sh
---
kafka:
  application.id: kafka-streams-101
  bootstrap.servers: kafka:29092
  metrics.recording.level: DEBUG

paths:
  /flights:
    get:
      description: Return all flights
      kafka:
        topic: flight-location
        query:
          method: all
        serializer:
          key: string
          value: string
      responses:
        '200':
          description: A list of flights
          content:
            application/json:
              schema:
                type: array
  /flights/{flightId}:
    parameters:
    - name: flightId
      in: path
      description: the flight identifier
    get:
      kafka:
        topic: flight-location
        serializer:
          key: string
          value: string
        query:
          method: get
          key: ${parameters.flightId}
      responses:
        '200':
          description:
          content:
            application/json:
              schema:
                type: object

```

### 2a. Run using Docker

```sh
docker-compose up -d
```

### 2b. Run locally

#### Build

```sh
./gradlew shadowJar
```

#### Run

```sh
java  -jar build/libs/kafka-as-a-microservice-standalone-*.jar configuration/config.yaml
```

### 3. Write Messages

Produce to `flight-location` topic:

```sh
kafka-console-producer \
  --property "key.separator=;" \
  --property "parse.key=true" \
  --bootstrap-server localhost:9092 \
  --topic flight-location

# produce below messages

NH1;{ "latitude": 37.7749, "longitude": -122.4194, "bearing": 135.5 }
NH2;{ "latitude": 37.7749, "longitude": -122.4194, "bearing": 134.3 }
```

### 4. Get Messages via REST


**REST Endpoint Output**

To get all,

```sh
curl localhost:7001/flights | jq

[
  {"latitude": 37.7749, "longitude": -122.4194, "bearing": 135.5},
  {"latitude": 37.7749, "longitude": -122.4194, "bearing": 134.3}
]
```

To get specific key,

```sh
curl localhost:7001/flights/NH1 | jq

{"latitude": 37.7749, "longitude": -122.4194, "bearing": 135.5 }
```

### 5. Enable Metric Monitoring

**Docker**

Metrics is enabled by default. To disable this, comment out line 34 in docker-compose file

```sh
JAVA_OPTS: "-javaagent:/app/monitoring/jmx_prometheus_javaagent-1.0.1.jar=0.0.0.0:7777:/app/monitoring/kafka_streams.yml"
```

To override metrics exporter behavior or prometheus agent, place your assets under `monitoring` folder

```sh
- ./monitoring:/app/monitoring
```

Then run again

```sh
docker-compose up
```

Verify by accessing 
```sh
curl localhost:7777/metrics
```

**Local**

Download these assets under monitoring

```sh
cd monitoring

# Download Kafka Streams JMX exporter config
curl -O https://raw.githubusercontent.com/confluentinc/jmx-monitoring-stacks/refs/heads/main/shared-assets/jmx-exporter/kafka_streams.yml

# Download Prometheus Agent
curl -O https://repo.maven.apache.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/jmx_prometheus_javaagent-1.0.1.jar
```

After downloading the assets, run with these options

```sh
java -javaagent:monitoring/jmx_prometheus_javaagent-1.0.1.jar=127.0.0.1:7777:monitoring/kafka_streams.yml -jar build/libs/kafka-as-a-microservice-standalone-*.jar configuration/config.yaml
```

The metrics can be retrieved by using the method below:

```sh
curl localhost:7777/metrics
```

The metrics can then be ingested by

- Prometheus
- Open Telemetry

## Test

Run below command to run unit tests.

```
gradle test
```
