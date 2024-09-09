# Kafka as a Microservice

Streamline data delivery from Kafka topics to your clients with a flexible, last-mile microservice solution.

## Motivation

Key motivations for this project:

1. Cost Efficiency: Traditional microservices often incur significant costs due to the need for individual Redis instances and data stores for each service. Expect multitude of cost savings.
2. Language Flexibility: While Java supports GlobalKTable or KTable, many other programming languages lack this functionality, limiting options for developers.
3. Reduced Development Overhead: Building microservices typically involves substantial effort in implementing monitoring, logging, and business logic for each service.
4. Simplified Architecture: This project aims to provide a streamlined solution that addresses these challenges, potentially reducing both development time and operational costs.


## Features

- [ ] Endpoints
  - [ ] GET all keys
  - [ ] Get single key
  - [ ] Get single key between
  - [ ] Get single key between

## Example

**Config**

```yaml

---
kafka:
  default:
    bootstrap.servers:
      
endpoints:
- name: flights
  action: getAll
  datastore: latest # stores latest value for key
  description: `/flights` returns all latest loctions of flights
  topic: flight-location


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

