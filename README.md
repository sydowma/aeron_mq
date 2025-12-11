# Aeron MQ

A high-performance, Kafka-compatible message queue built on [Aeron](https://github.com/real-logic/aeron).

## Overview

Aeron MQ leverages Aeron's low-latency messaging capabilities to provide a Kafka-compatible message broker with superior performance characteristics:

- **Zero-copy message handling** via Agrona DirectBuffer
- **Off-heap memory management** to minimize GC pressure
- **Raft-based consensus** via Aeron Cluster for fault tolerance
- **Persistent storage** via Aeron Archive
- **Native Kafka protocol** support - works with existing Kafka clients

## Architecture

```
┌─────────────────┐     ┌─────────────────────────────────────────┐
│  Kafka Client   │     │              Aeron MQ Node              │
│  (Producer/     │────▶│  ┌─────────┐  ┌────────┐  ┌─────────┐  │
│   Consumer)     │TCP  │  │   TCP   │  │Request │  │  Topic  │  │
└─────────────────┘9092 │  │ Gateway │─▶│Handler │─▶│ Manager │  │
                        │  │ (Netty) │  │        │  │         │  │
                        │  └─────────┘  └────────┘  └────┬────┘  │
                        │                                │       │
                        │  ┌─────────────────────────────▼────┐  │
                        │  │         Aeron Archive            │  │
                        │  │    (Persistent Storage)          │  │
                        │  └──────────────────────────────────┘  │
                        │                                        │
                        │  ┌──────────────────────────────────┐  │
                        │  │        Aeron Cluster             │  │
                        │  │   (Raft Consensus Protocol)      │  │
                        │  └──────────────────────────────────┘  │
                        └────────────────────────────────────────┘
```

## Supported Kafka APIs

| API | Version | Description |
|-----|---------|-------------|
| ApiVersions | 0-3 | Discover supported API versions |
| Metadata | 0-12 | Get cluster and topic metadata |
| Produce | 0-9 | Produce messages to topics |
| Fetch | 0-13 | Consume messages from topics |
| ListOffsets | 0-7 | Get offset positions |
| FindCoordinator | 0-4 | Locate group coordinator |
| JoinGroup | 0-9 | Join consumer group |
| SyncGroup | 0-5 | Synchronize group assignment |
| Heartbeat | 0-4 | Keep group membership alive |
| LeaveGroup | 0-5 | Leave consumer group |
| OffsetCommit | 0-8 | Commit consumer offsets |
| OffsetFetch | 0-8 | Fetch committed offsets |

## Quick Start

### Prerequisites

- Java 25+
- Maven 3.9+
- Docker & Docker Compose (for containerized deployment)

### Run Single Node

```bash
# Build the project
mvn clean package -DskipTests

# Run the server
java --enable-preview -jar aeron-mq-server/target/aeron-mq-server-1.0.0-SNAPSHOT.jar
```

### Docker Single Node

```bash
docker-compose -f docker-compose.single.yml up -d
```

### Docker 3-Node Cluster

```bash
docker-compose up -d
```

### Test with Kafka CLI

```bash
# Create a topic (auto-created by default)
# Produce messages
kafka-console-producer.sh --broker-list localhost:9092 --topic test

# Consume messages
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning
```

### Test with Java Client

```java
// Producer
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
producer.send(new ProducerRecord<>("test", "key", "value"));
producer.close();

// Consumer
props.put("group.id", "my-group");
props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
consumer.subscribe(Collections.singletonList("test"));
ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_ID` | `0` | Unique broker ID |
| `HOST` | `0.0.0.0` | Bind address |
| `KAFKA_PORT` | `9092` | Kafka protocol port |
| `DATA_DIR` | `data` | Data directory for persistence |
| `AERON_DIR` | `/dev/shm/aeron-mq` | Aeron media driver directory |
| `AUTO_CREATE_TOPICS` | `true` | Auto-create topics on produce |
| `CLUSTER_MEMBERS` | - | Cluster member list (see below) |
| `JAVA_OPTS` | - | JVM options |

### Cluster Members Format

```
NODE_ID,HOSTNAME,BASE_PORT|NODE_ID,HOSTNAME,BASE_PORT|...
```

Example:
```
0,aeron-mq-1,20000|1,aeron-mq-2,20000|2,aeron-mq-3,20000
```

## Building

```bash
# Full build with tests
mvn clean verify

# Build without tests
mvn clean package -DskipTests

# Build Docker image
docker build -t aeron-mq:latest .
```

## Module Structure

```
aeron-mq/
├── aeron-mq-protocol/    # Kafka binary protocol encoder/decoder
├── aeron-mq-broker/      # Broker core logic, storage, clustering
├── aeron-mq-gateway/     # Netty TCP gateway
└── aeron-mq-server/      # Server bootstrap and main entry point
```

## Performance Tuning

### JVM Options

```bash
JAVA_OPTS="-Xms2g -Xmx4g \
  -XX:+UseG1GC \
  -XX:MaxGCPauseMillis=10 \
  -XX:+UseStringDeduplication \
  -Dagrona.disable.bounds.checks=true"
```

### System Tuning (Linux)

```bash
# Increase max open files
ulimit -n 65536

# Use huge pages for shared memory
echo 1024 > /proc/sys/vm/nr_hugepages

# Disable CPU frequency scaling
for cpu in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
  echo performance > $cpu
done
```

### Aeron Tuning

For ultra-low latency:

1. **Use shared memory** (`/dev/shm`) for Aeron directory
2. **Pin threads** to specific CPU cores
3. **Use busy-spin idle strategy** instead of sleeping
4. **Disable bounds checking** in production

## Why Aeron MQ?

| Feature | Kafka | Aeron MQ |
|---------|-------|----------|
| Latency | ~1-5ms | ~10-100μs |
| Message Passing | TCP | UDP/IPC |
| Buffer Management | JVM Heap | Off-Heap |
| Consensus | ZooKeeper/KRaft | Aeron Cluster (Raft) |
| Zero-Copy | Partial | Full |

## Limitations

Current implementation limitations:

- No transaction support
- No ACL/authentication
- Limited partition reassignment
- No log compaction
- Simplified consumer group rebalancing

## License

Apache License 2.0

## Acknowledgments

- [Aeron](https://github.com/real-logic/aeron) - High-performance messaging
- [Agrona](https://github.com/real-logic/agrona) - High-performance data structures
- [Netty](https://netty.io/) - Asynchronous event-driven network framework

