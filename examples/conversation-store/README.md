# Conversation store example

Serves chat-style conversations and their messages straight from a single Kafka topic, using the
`prefix` and `get` query methods. No database, no per-conversation index record.

| Lookup | Endpoint | Query method |
|--|--|--|
| List all conversations | `GET /conversations` | `prefix` on `conversation:` |
| One conversation's metadata | `GET /conversations/{id}` | `get` on `conversation:{id}` |
| A conversation's messages (ordered) | `GET /conversations/{id}/messages` | `prefix` on `message:{id}:` |

## Key design

A single topic `conversation-store` (string keys, JSON string values) holds two record types,
separated by key prefix so a `conversation:` scan never returns messages:

```
conversation:conv1            {"name":"Trip planning","state":"open"}
message:conv1:0000000001      {"role":"user","text":"hello"}
message:conv1:0000000002      {"role":"assistant","text":"how are you?"}
```

Offsets are **zero-padded** so they sort numerically (RocksDB orders keys by serialized bytes;
unpadded, `10` would sort before `2`). Keys must be `string` — Avro's binary encoding is not
prefix-preserving.

## Run it

1. Start Kafka and create the topic:

   ```sh
   docker compose up -d kafka
   docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
     --create --topic conversation-store --partitions 6 --replication-factor 1
   ```

2. Produce a couple of conversations and some messages:

   ```sh
   printf '%s\n' \
     'conversation:conv1;{"name":"Trip planning","state":"open"}' \
     'conversation:conv2;{"name":"Bug triage","state":"closed"}' \
     'message:conv1:0000000001;{"role":"user","text":"hello"}' \
     'message:conv1:0000000002;{"role":"assistant","text":"how are you?"}' \
     'message:conv1:0000000010;{"role":"user","text":"much later"}' \
     'message:conv2:0000000001;{"role":"user","text":"different conversation"}' \
   | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
       --bootstrap-server localhost:9092 --topic conversation-store \
       --property "parse.key=true" --property "key.separator=;"
   ```

3. Build and run the service against this config:

   ```sh
   ./gradlew shadowJar
   java -jar build/libs/kafka-as-a-microservice-standalone-*.jar examples/conversation-store/config.yaml
   ```

## Query it

```sh
# 1. List all conversations (messages are excluded — prefix isolation)
curl -s localhost:7001/conversations | jq
# [ {"name":"Trip planning","state":"open"}, {"name":"Bug triage","state":"closed"} ]

# 2. One conversation's metadata
curl -s localhost:7001/conversations/conv1 | jq
# {"name":"Trip planning","state":"open"}

# 3. A conversation's messages, in order (padding keeps offset 2 before offset 10)
curl -s localhost:7001/conversations/conv1/messages | jq
# [ {"role":"user","text":"hello"},
#   {"role":"assistant","text":"how are you?"},
#   {"role":"user","text":"much later"} ]
```

New messages produced to the topic appear in the results within a second or two — the GlobalKTable
updates live.

> **Tip:** set `includeKey: true` under `kafka:` on any endpoint to have each object carry its own
> key under a `"key"` field (e.g. `"key":"message:conv1:0000000001"`).
