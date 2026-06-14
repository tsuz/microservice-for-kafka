# Conversation store — range example

Fetches an **inclusive `[from, to]` window** of a conversation's messages with the `range` query
method — the building block for pagination or replaying a slice of a transcript. Shares the
single-topic data model from [`examples/conversation-store`](../conversation-store).

| Lookup | Endpoint | Query method |
|--|--|--|
| One conversation's metadata | `GET /conversations/{id}` | `get` |
| A window of messages | `GET /conversations/{id}/messages/{from}/{to}` | `range` on `message:{id}:{from}` → `message:{id}:{to}` |

## Key design

Messages are keyed `message:<id>:<paddedOffset>` in a `conversation-store` topic (string keys, JSON
string values). Offsets are **zero-padded** so they sort numerically (RocksDB orders keys by
serialized bytes; unpadded, `10` would sort before `2`). Keys must be `string` — Avro's binary
encoding is not order-preserving, so `range` rejects Avro keys.

## Run it

1. Start Kafka and create the topic:

   ```sh
   docker compose up -d kafka
   docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
     --create --topic conversation-store --partitions 6 --replication-factor 1
   ```

2. Produce a conversation with several messages (note the padded offsets, with a gap at 3–9):

   ```sh
   printf '%s\n' \
     'conversation:conv1;{"name":"Trip planning","state":"open"}' \
     'message:conv1:0000000001;{"role":"user","text":"hello"}' \
     'message:conv1:0000000002;{"role":"assistant","text":"how are you?"}' \
     'message:conv1:0000000010;{"role":"user","text":"much later"}' \
     'message:conv1:0000000011;{"role":"assistant","text":"a brand new reply"}' \
   | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
       --bootstrap-server localhost:9092 --topic conversation-store \
       --property "parse.key=true" --property "key.separator=;"
   ```

3. Build and run the service against this config:

   ```sh
   ./gradlew shadowJar
   java -jar build/libs/kafka-as-a-microservice-standalone-*.jar examples/conversation-store-range/config.yaml
   ```

## Query it

```sh
# Inclusive window [2, 10] — note offset 10 is included, and padding keeps 2 before 10
curl -s "localhost:7001/conversations/conv1/messages/0000000002/0000000010" | jq
# [ {"role":"assistant","text":"how are you?"},
#   {"role":"user","text":"much later"} ]

# Wide window [0, 99] — the whole transcript, in order
curl -s "localhost:7001/conversations/conv1/messages/0000000000/0000000099" | jq -c '[.[].text]'
# ["hello","how are you?","much later","a brand new reply"]

# A window that falls in a gap returns an empty array
curl -s "localhost:7001/conversations/conv1/messages/0000000003/0000000009" | jq -c
# []
```

Both bounds are inclusive. To paginate, advance `from` past the last offset you received.
