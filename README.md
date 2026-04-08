# Kafka Examples

A hands-on collection of Apache Kafka examples covering producer/consumer basics, multi-broker clusters, Kafka Streams aggregation (Java), and real-time stream processing with QuixStreams (Python).

---

## Project structure

```
kafka-examples/
│
├── producer-consumer/               # Single broker — Python basics
│   ├── producer.py
│   └── consumer.py
│
├── producer-consumer-three-brokers/ # Multi-broker cluster — Python
│   ├── producer-3Brokers.py
│   └── consumer-3Brokers.py
│
├── event-streams/                   # Kafka Streams aggregation — Java
│   ├── TransactionProducer.java
│   └── RevenueTracker.java
│
└── simple-event-streams/            # Stream processing — Python + QuixStreams
    ├── transactionsProducer.py
    └── fraud_detector.py
```

---

## Prerequisites

| Requirement | Version |
|---|---|
| Docker & Docker Compose | any recent |
| Python | 3.8+ |
| Java (JDK) | 17+ |
| Maven | 3.8+ |

Install Python dependencies:

```bash
pip install kafka-python quixstreams
```

---

## Kafka setup (Docker)

### Single broker

```yaml
# docker-compose.yml
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:latest
    ports:
      - "9092:9092"
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

### Three-broker cluster

```yaml
# docker-compose-3brokers.yml
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka1:
    image: confluentinc/cp-kafka:latest
    ports: ["9093:9093"]
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9093

  kafka2:
    image: confluentinc/cp-kafka:latest
    ports: ["9094:9094"]
    environment:
      KAFKA_BROKER_ID: 2
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9094

  kafka3:
    image: confluentinc/cp-kafka:latest
    ports: ["9095:9095"]
    environment:
      KAFKA_BROKER_ID: 3
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9095
```

Start the cluster:

```bash
docker-compose up -d
```

---

## Examples

---

### 1 — Producer / Consumer (single broker)

> **Folder:** `producer-consumer/`

A minimal Python example. The producer sends a JSON message every 2 seconds to the `test-events` topic. The consumer reads from the earliest offset and deserializes each message manually.

**How it works:**

```
Producer                         Broker                    Consumer
  │                                │                           │
  │── serialize to JSON ──────────>│                           │
  │   {"id":1, "message":"..."}    │── append to log ──────────│
  │                                │   offset 0               │
  │                                │                          poll()
  │                                │<──── fetch from offset 0 ─│
  │                                │──── return batch ────────>│
  │                                │                      deserialize
  │                                │                      print message
  │                                │                      commit offset 1
```

**Create the topic:**

```bash
docker exec -it <kafka-container> kafka-topics \
  --create --topic test-events \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
```

**Run:**

```bash
# Terminal 1 — start consumer first so it's ready
python consumer.py

# Terminal 2
python producer.py
```

**Expected output:**

```
# producer.py
Starting Producer... Press Ctrl+C to stop.
Sent: {'id': 1, 'message': 'Hello from Python to Docker Kafka!', 'timestamp': 1718000000.0}

# consumer.py
Consumer started... Waiting for messages.
Received JSON: Hello from Python to Docker Kafka! (ID: 1)
```

**Key configuration:**

| Setting | Value | Purpose |
|---|---|---|
| `bootstrap_servers` | `localhost:9092` | Entry point to the cluster |
| `value_serializer` | `json.dumps + encode` | Serialize dict → bytes |
| `auto_offset_reset` | `earliest` | Read from the beginning of the log |
| `group_id` | `track-group-01` | Consumer group — tracks offset per group |

---

### 2 — Producer / Consumer (three brokers)

> **Folder:** `producer-consumer-three-brokers/`

The same producer/consumer pattern but connected to a three-broker cluster using topic `test-three-brokers`. Demonstrates Kafka's fault tolerance — if one broker goes down, the client automatically fails over to the others listed in `bootstrap_servers`.

**Create the topic with replication:**

```bash
docker exec -it <kafka1-container> kafka-topics \
  --create --topic test-three-brokers \
  --bootstrap-server localhost:9093 \
  --partitions 3 --replication-factor 3
```

**Run:**

```bash
# Terminal 1
python consumer-3Brokers.py

# Terminal 2
python producer-3Brokers.py
```

**What changes vs single broker:**

| Setting | Single broker | Three brokers |
|---|---|---|
| `bootstrap_servers` | `['localhost:9092']` | `['localhost:9093','localhost:9094','localhost:9095']` |
| Topic | `test-events` | `test-three-brokers` |
| Replication factor | 1 | 3 |

> `bootstrap_servers` is just the initial connection list — Kafka will discover all brokers from any one of them. You don't need to list all brokers, but listing all three makes startup more resilient.

---

### 3 — Event Streams: merchant revenue tracker (Java)

> **Folder:** `event-streams/`

A Java example using the native Kafka Streams API. `TransactionProducer` sends random transactions keyed by merchant name. `RevenueTracker` consumes the stream, counts transactions per merchant using a `KTable`, prints live updates, and sinks the result to the `merchant-totals` topic.

**Architecture:**

```
TransactionProducer
  │
  │── {"merchant":"Starbucks", "amount":120} ──> [transactions topic]
  │                                                       │
  │                                               RevenueTracker
  │                                               (Kafka Streams)
  │                                                       │
  │                                              groupByKey() → count()
  │                                              KTable: Starbucks → 5
  │                                                       │
  │                                               [merchant-totals topic]
```

**Create topics:**

```bash
kafka-topics --create --topic transactions \
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

kafka-topics --create --topic merchant-totals \
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

**Build and run:**

```bash
cd event-streams
mvn clean package

# Terminal 1 — start the stream processor
mvn exec:java -Dexec.mainClass="com.kafka.example.Kafka.RevenueTracker"

# Terminal 2 — start the producer
mvn exec:java -Dexec.mainClass="com.producer.consumer.example.ProducerConsumer.TransactionProducer"
```

**Expected output (`RevenueTracker`):**

```
🚀 Kafka Streams App starting...
🚀 Kafka Streams App Started! Current State: RUNNING
📊 [LIVE UPDATE] Merchant: Starbucks | Total Transactions: 1
📊 [LIVE UPDATE] Merchant: Jumia     | Total Transactions: 1
📊 [LIVE UPDATE] Merchant: Starbucks | Total Transactions: 2
```

**How the stream topology works:**

| Step | Operation | What it does |
|---|---|---|
| 1 | `builder.stream("transactions")` | Subscribe to input topic as a `KStream` |
| 2 | `groupByKey()` | Group records by the merchant key |
| 3 | `count(Materialized.as(...))` | Stateful count — stored in local RocksDB |
| 4 | `toStream().foreach(...)` | Side-effect: print each update |
| 5 | `toStream().to("merchant-totals")` | Sink aggregated result to output topic |

> The state store (`merchant-inventory-store`) is backed by a Kafka changelog topic automatically — it survives restarts without any extra configuration.

---

### 4 — Simple Event Streams: fraud detector (Python + QuixStreams)

> **Folder:** `simple-event-streams/`

A Python equivalent of the Java streaming example. `transactionsProducer.py` sends random transactions keyed by merchant. `fraud_detector.py` uses **QuixStreams** — a Python Kafka Streams-like library — to filter for transactions over $5,000 and print fraud alerts.

**Architecture:**

```
transactionsProducer.py
  │
  │── {"merchant":"Zara", "amount":7200} ──> [transactions topic]
  │                                                  │
  │                                          fraud_detector.py
  │                                          (QuixStreams SDF)
  │                                                  │
  │                                          filter: amount > 5000
  │                                                  │
  │                                          🚨 ALERT printed
```

**Run:**

```bash
# Terminal 1
python fraud_detector.py

# Terminal 2
python transactionsProducer.py
```

**Expected output (`fraud_detector.py`):**

```
🚨 ALERT: Potential Fraud detected for user_id!
   Details: $7200 spent at Zara
```

**How the streaming dataframe (SDF) works:**

```python
sdf = app.dataframe(topic)          # Subscribe to topic → streaming dataframe
fraud_sdf = sdf[sdf["amount"] > 5000]  # Filter — only rows where amount > 5000
fraud_sdf = fraud_sdf.update(report_fraud)  # Apply side-effect function per row
app.run(sdf)                        # Start the event loop
```

> Note: `fraud_detector.py` filters for `amount > 5000`, but `transactionsProducer.py` only generates amounts between $10–$500. To trigger alerts during testing, temporarily lower the threshold to e.g. `sdf["amount"] > 100`.

---

## Concepts illustrated

| Concept | Where it appears |
|---|---|
| Producer serialization (JSON → bytes) | All producers |
| Consumer offset management | `consumer.py`, `consumer-3Brokers.py` |
| Consumer groups | All consumers (`group_id` / `consumer_group`) |
| Multi-broker fault tolerance | `producer-consumer-three-brokers/` |
| Partition key routing | `transactionsProducer.py`, `TransactionProducer.java` |
| Stateful stream aggregation (KTable) | `RevenueTracker.java` |
| Stream filtering | `fraud_detector.py` |
| RocksDB local state + changelog backup | `RevenueTracker.java` |
| Sink to output topic | `RevenueTracker.java` → `merchant-totals` |

---

## Troubleshooting

**`NoBrokersAvailable` error**
- Check Docker is running: `docker ps`
- Verify the port mapping matches `bootstrap_servers` in your script

**Consumer receives no messages**
- Confirm the topic exists: `kafka-topics --list --bootstrap-server localhost:9092`
- Check `auto_offset_reset='earliest'` is set if the producer ran before the consumer started

**Kafka Streams app stays in `REBALANCING` state**
- Another instance with the same `application.id` may be running — stop it and restart

**QuixStreams `fraud_detector.py` shows no alerts**
- The max amount from the producer is $500 — lower the filter threshold to test: `sdf["amount"] > 100`