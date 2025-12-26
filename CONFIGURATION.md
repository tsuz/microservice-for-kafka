# Configurations

Given an example below:

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
        mergeKey: true
      avro:
        includeType: true
      responses:
        '200':
          description: A list of flights
          content:
            application/json:
              schema:
                type: array


```

## Kafka Object

These properties are passed through into Kafka Streams configurations.

## Paths

Path contains such hiearchy:

```yaml
paths:
  /<pathName>:
    <method>:
      Config
```

where

- `paths` is a static value
- `/<pathName>` is the custom defined REST API path. It must start with a slash.
- `<method>` is the action method of the API. Only `get` is currently supported.
- `Config` is the config that defines how the data is retrieved for this endpoint.

## Config Object

| top-level field | description |
|--|--|
| description | comment that describes this endpoint. This object does not have an effect on the functionality of this API. |
| kafka | message level config such as topics, query method and serializers |
| avro | if using avro, this is additional avro setting | 
| responses | response object that conforms to the OpenAPI. This object does not have an effect on the functionality of this API. | 

### Kafka Object

#### mergeKey

Default: false

If set to true, then it will merge the key into the value and send it back in the response.

Suppose

```json
Key: {"productId": 1}
Value: {"productName": "foo"}
```

then if `mergeKey=true` then it will return

```
{
  "productId": 1,
  "productName": "foo"
}
```

if `mergeKey=false` then it will return

```
{
  "productName": "foo"
}
```

### Avro Object

#### includeType

Default: false

If set to true, then it will return the type tag within the JSON. For example:

```json
{
    "remQuantity": {
        "int": 6
    },
    "storeId": {
        "string": "001"
    },
    "updatedAt": {
        "long": 1766431139805
    }
}
```

whereas if false, then it returns:

```json
{
    "remQuantity": 6,
    "storeId": "001",
    "updatedAt": 1766431139805
}
```
