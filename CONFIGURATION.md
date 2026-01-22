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

### description

Default: ""

Comment that describes this endpoint. This object does not have an effect on the functionality of this API. 


### Kafka

Default: Object

Message level config such as topics, query method and serializers

```
kafka:  
  topic: inventory_20251222_1748
  query:
    method: all
  serializer:
    key: avro
    value: avro
  mergeKey: true
  keyField: productId
```

#### mergeKey

Default: false

If set to true, then it will merge the key into the value and send it back in the response.

This is only supported for Avro.

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

#### keyField

Default: null

`keyField` is required if key serializer is avro.

If key is an avro, then it requires a key field to look up the value. For example, if `keyField=productId` and the path is `/products/{id}`, then it looks up using key `{"productId": "{id}"}`. 

#### keyFields

Default: []

The list of multiple key fields used for looking up avro keys.

Either `keyField` or `keyFields` is required if key serializer is avro, and both cannot be set simultaneously for each path.


```sh
  /aggregation/5min/{productId}/{windowStart}:
    parameters:
    - name: productId
      in: path
      description: the product id
    - name: windowStart
      in: path
      description: the start window of product purchase
    get:
      kafka:
        topic: orders_5min_realtime
        serializer:
          key: avro
          value: avro
        keyFields:
          product_id: ${parameters.productId}
          bucket_start: ${parameters.windowStart}
```


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

### responses

response object that conforms to the OpenAPI. This object does not have an effect on the functionality of this API.


