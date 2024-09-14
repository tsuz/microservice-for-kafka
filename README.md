# Kafka as a Microservice

Streamline data delivery from Kafka topics to your clients with a flexible, last-mile microservice solution.

## Motivation

Key motivations for this project:

1. Cost Efficiency: Traditional microservices often incur significant costs due to the need for individual Redis instances and data stores for each service. Expect multitude of cost savings.
2. Language Flexibility: While Java supports GlobalKTable or KTable, many other programming languages lack this functionality, limiting options for developers.
3. Reduced Development Overhead: Building microservices typically involves substantial effort in implementing monitoring, logging, and business logic for each service.
4. Simplified Architecture: This project aims to provide a streamlined solution that addresses these challenges, potentially reducing both development time and operational costs. A very high level architecture looks like below:

![with and without this library](https://github.com/user-attachments/assets/9ecdcc34-5884-4aeb-bfef-e5680e9cc66f)

## Features

**API Endpoint Type**

| Endpoint type | Status |
|--|--|
| List all items | ✅
| Get single item | ✅


**Data Store Types**

| Store | Comment | Status 
|--|--|--|
| latest | Maintain latest value for key | ✅
| appendValue | Appends value for key | ✅

**Data Type (Key)**
| Data Type | Status |
|--|--|
| String | ✅
| Avro | ✅
| Int | 


**Data Type (Value)**
| Data Type | Status |
|--|--|
| String (JSON) | ✅
| String | 
| JSON Schema | 
| Avro Schema | ✅


## Example

### List all endpoint

**Config**

```yaml

kafka:
  bootstrap.servers:
  application.id: kafka-streams-101
  bootstrap.servers: localhost:9092

  # Serializer
  key.serializer: org.apache.kafka.common.serialization.StringSerializer
  value.serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
  
  # Schema Registry Properties
  schema.registry.url: http://localhost:8081
  basic.auth.credentials.source: USER_INFO
  basic.auth.user.info: username:password

  metrics.recording.level: DEBUG

endpoints:
- name: flights
  action: listAll
  datastore: latest # stores latest value for key
  description: `/flights` returns all latest loctions of flights
  topic: flight-location
  keySerializer: string
  valueSerializer: avro

```

**Input**

Produce to `flight-location` topic:

```sh
kafka-console-producer \
  --property "key.separator=;" \
  --property "parse.key=true" \
  --bootstrap-server localhost:9092 \
  --topic flight-location

> NH1;{ "latitude": 37.7749, "longitude": -122.4194, "bearing": 135.5 }
> NH2;{ "latitude": 37.7749, "longitude": -122.4194, "bearing": 134.3 }
```

**Output**

This generates below endpoint:

```
Request: GET /flights
Response: 
  [
	{"latitude": 37.7749, "longitude": -122.4194, "bearing": 135.5 },
	{"latitude": 37.7749, "longitude": -122.4194, "bearing": 134.3 }
  ]
```





## How to Run

### 1. Create a configuration file

```sh
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
 
endpoints:
- name: flights
  action: listAll
  datastore: latest # stores latest value for key
  description: # `/flights` returns all latest flights
  topic: flight-location
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
