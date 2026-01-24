# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run Commands

```bash
# Build without tests
mvn clean package -DskipTests

# Full build with tests
mvn clean verify

# Run single test
mvn test -Dtest=ClassName#methodName

# Run the server (requires --enable-preview for Java 25)
java --enable-preview -jar aeron-mq-server/target/aeron-mq-server-1.0.0-SNAPSHOT.jar

# Run single node via Docker
docker-compose -f docker-compose.single.yml up -d

# Run 3-node cluster via Docker
docker-compose up -d

# Build Docker image
docker build -t aeron-mq:latest .
```

## High-Level Architecture

Aeron MQ is a high-performance, Kafka-compatible message queue built on Aeron's low-latency messaging framework.

### Module Structure

```
aeron-mq/
├── aeron-mq-protocol/    # Kafka binary protocol encoder/decoder
├── aeron-mq-broker/      # Core broker logic, storage, clustering
├── aeron-mq-gateway/     # Netty TCP gateway for client connections
└── aeron-mq-server/      # Server bootstrap and main entry point
```

### Component Flow

```
Kafka Client (TCP:9092)
       │
       ▼
TcpGateway (Netty) → KafkaRequestHandler
       │
       ▼
RequestHandler → TopicManager → PartitionLog
       │                    │
       │                    ▼
       │              Aeron Archive (persistent storage)
       │
       ▼
ConsumerGroupCoordinator
```

### Key Architecture Patterns

**Zero-Copy Design**: The system uses Agrona DirectBuffer throughout for zero-copy message handling. Messages are appended to and read from Aeron streams without copying into JVM heap.

**Offset-to-Position Mapping**: Each Kafka offset maps to an Aeron position. Critical invariant: one Kafka record-batch = one Aeron message (no fragmentation). This is enforced in `PartitionLog.append()` by checking against `maxPayloadLength()`. If a batch exceeds this limit, it's rejected to prevent offset indexing ambiguity.

**Deterministic Session IDs**: `PartitionLog` computes deterministic session IDs from (topic, partition) hash to allow extending recordings after restart. The stream ID is `STREAM_ID_BASE + partition`.

**Index Rebuilding**: On startup, `PartitionLog.rebuildIndexFromRecording()` replays the Aeron recording to rebuild offset->position mappings. This ensures offsets don't reset on restart.

**Thread Safety**: `PartitionLog.append()` is synchronized because `ExclusivePublication` is not thread-safe. Each partition has its own `PartitionLog` instance with its own publication.

**Network Threading**: Netty uses separate thread pools:
- IO threads: Protocol encode/decode (fast, non-blocking)
- Business thread pool: RequestHandler calls (may block on Aeron Archive replay)

This prevents broker backpressure from stalling Netty event loops.

### Core Classes

**aeron-mq-broker**:
- `PartitionLog`: Manages a single partition's persistent storage via Aeron Archive. Handles append, read, offset indexing.
- `TopicManager`: Manages topic metadata and creates `PartitionLog` instances.
- `RequestHandler`: Processes Kafka protocol requests, delegates to TopicManager and ConsumerGroupCoordinator.
- `ConsumerGroupCoordinator`: Manages consumer group state, offset commits, and rebalancing.
- `BrokerConfig`: Configuration record (loaded from environment variables).

**aeron-mq-gateway**:
- `TcpGateway`: Netty-based TCP server. Uses epoll on Linux for performance.
- `KafkaFrameDecoder/Encoder`: Netty pipeline handlers for Kafka protocol framing.
- `KafkaRequestHandler`: Netty handler that deserializes requests and calls `RequestHandler`.

**aeron-mq-server**:
- `AeronMqServer`: Main entry point. Starts MediaDriver, Archive, TopicManager, RequestHandler, and TcpGateway in order.

### Supported Kafka APIs

The broker supports a limited set of Kafka APIs. When adding support for new APIs:
1. Add the `ApiKey` enum in `aeron-mq-protocol`
2. Implement request/response classes in the protocol module
3. Add a handler method in `RequestHandler` using Java pattern matching on `KafkaRequest`
4. Add the API version to `SUPPORTED_VERSIONS` in `RequestHandler`

Currently supported: ApiVersions, Metadata, Produce, Fetch, ListOffsets, FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup, OffsetCommit, OffsetFetch.

### Configuration

Configuration is via environment variables (see `AeronMqServer.main()`):
- `NODE_ID`: Broker ID (default: 0)
- `HOST`: Bind address (default: 0.0.0.0)
- `KAFKA_PORT`: Kafka protocol port (default: 9092)
- `DATA_DIR`: Persistent storage directory (default: "data")
- `AERON_DIR`: Aeron media driver directory (default: `/dev/shm/aeron-mq-{NODE_ID}`)
- `AUTO_CREATE_TOPICS`: Auto-create topics on produce (default: true)
- `CLUSTER_MEMBERS`: Cluster members in format `ID,HOST,PORT|ID,HOST,PORT|...`

For production, set `AERON_DIR` to a shared memory location (`/dev/shm`) for best performance.

### Storage Implementation Details

**Aeron Archive Channels**: Uses IPC channels (`aeron:ipc?term-length=64k|alias=...`) for local persistence.

**Recording Lifecycle**:
1. New topic: `archive.startRecording()` creates a new recording
2. Existing topic: `archive.extendRecording()` resumes the previous recording
3. Deterministic session IDs ensure recordings map to the same partition across restarts

**Replay for Fetch**: Fetch requests use `archive.startReplay()` to read specific offset ranges. Each replay gets a unique stream ID from `REPLAY_STREAM_ID` counter to avoid conflicts.

**Offset Index**: Three parallel arrays store per-offset metadata:
- `positionsByOffset`: Aeron start position for each offset
- `payloadLengthsByOffset`: Actual data length
- `spansByOffset`: Aligned frame length (for position calculations)

### Testing

Currently no tests exist. JUnit 5 is configured via Maven Surefire with `--enable-preview` flag. Tests should be placed in `src/test/java` following Maven conventions.

### Common Modifications

**Add new Kafka API support**: Update `aeron-mq-protocol` module first, then add handler in `RequestHandler`.

**Change storage behavior**: Modify `PartitionLog` - this class encapsulates all Aeron Archive interaction.

**Add clustering features**: `ClusterNode` class exists but cluster coordination via Aeron Cluster is not fully implemented. Currently runs in single-node mode.

**Performance tuning**: Adjust Aeron channel parameters (term-length, MTU) in `PartitionLog.CHANNEL_TEMPLATE`. Use shared memory for `AERON_DIR`. Pin threads to CPU cores for ultra-low latency.

### Limitations

Current implementation is a prototype with these limitations:
- No transaction support
- No ACL/authentication
- Limited partition reassignment
- No log compaction
- Simplified consumer group rebalancing
- Single-writer per partition (synchronized)
- No fault-tolerance/leader election (cluster infrastructure exists but not active)
