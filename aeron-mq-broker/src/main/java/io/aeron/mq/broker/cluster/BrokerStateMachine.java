package io.aeron.mq.broker.cluster;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import io.aeron.mq.broker.ConsumerGroupCoordinator;
import io.aeron.mq.broker.RequestHandler;
import io.aeron.mq.broker.TopicManager;
import io.aeron.mq.broker.TopicPartition;
import io.aeron.mq.protocol.*;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Aeron Cluster state machine for the broker.
 * Replicates state across cluster nodes using Raft consensus.
 */
public final class BrokerStateMachine implements ClusteredService {

    private static final Logger LOG = LoggerFactory.getLogger(BrokerStateMachine.class);

    // Command types for state machine
    private static final byte CMD_PRODUCE = 1;
    private static final byte CMD_OFFSET_COMMIT = 2;
    private static final byte CMD_JOIN_GROUP = 3;
    private static final byte CMD_SYNC_GROUP = 4;
    private static final byte CMD_LEAVE_GROUP = 5;
    private static final byte CMD_HEARTBEAT = 6;
    private static final byte CMD_CREATE_TOPIC = 7;
    private static final byte CMD_DELETE_TOPIC = 8;

    private Cluster cluster;
    private IdleStrategy idleStrategy;

    // In-memory state (replicated across cluster)
    private final ConcurrentHashMap<TopicPartition, PartitionState> partitions = new ConcurrentHashMap<>();
    private final ConsumerGroupCoordinator groupCoordinator = new ConsumerGroupCoordinator();

    // Encoding/decoding buffers
    private final ExpandableArrayBuffer responseBuffer = new ExpandableArrayBuffer(1024 * 1024);
    private final KafkaRequestDecoder requestDecoder = new KafkaRequestDecoder();
    private final KafkaResponseEncoder responseEncoder = new KafkaResponseEncoder();

    @Override
    public void onStart(Cluster cluster, Image snapshotImage) {
        this.cluster = cluster;
        this.idleStrategy = cluster.idleStrategy();

        LOG.info("BrokerStateMachine started, role={}, memberId={}",
                cluster.role(), cluster.memberId());

        if (snapshotImage != null) {
            loadSnapshot(snapshotImage);
        }
    }

    @Override
    public void onSessionOpen(ClientSession session, long timestamp) {
        LOG.debug("Client session opened: sessionId={}", session.id());
    }

    @Override
    public void onSessionClose(ClientSession session, long timestamp, CloseReason closeReason) {
        LOG.debug("Client session closed: sessionId={}, reason={}", session.id(), closeReason);
    }

    @Override
    public void onSessionMessage(ClientSession session, long timestamp, DirectBuffer buffer,
                                  int offset, int length, Header header) {
        // Parse command type
        byte commandType = buffer.getByte(offset);
        offset += 1;
        length -= 1;

        switch (commandType) {
            case CMD_PRODUCE -> handleProduceCommand(session, buffer, offset, length);
            case CMD_OFFSET_COMMIT -> handleOffsetCommitCommand(session, buffer, offset, length);
            case CMD_JOIN_GROUP -> handleJoinGroupCommand(session, buffer, offset, length);
            case CMD_SYNC_GROUP -> handleSyncGroupCommand(session, buffer, offset, length);
            case CMD_LEAVE_GROUP -> handleLeaveGroupCommand(session, buffer, offset, length);
            case CMD_HEARTBEAT -> handleHeartbeatCommand(session, buffer, offset, length);
            case CMD_CREATE_TOPIC -> handleCreateTopicCommand(session, buffer, offset, length);
            case CMD_DELETE_TOPIC -> handleDeleteTopicCommand(session, buffer, offset, length);
            default -> LOG.warn("Unknown command type: {}", commandType);
        }
    }

    @Override
    public void onTimerEvent(long correlationId, long timestamp) {
        // Handle heartbeat timeouts, session expirations, etc.
    }

    @Override
    public void onTakeSnapshot(ExclusivePublication snapshotPublication) {
        LOG.info("Taking snapshot");

        MutableDirectBuffer snapshotBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(64 * 1024 * 1024));
        int offset = 0;

        // Write partition count
        snapshotBuffer.putInt(offset, partitions.size());
        offset += 4;

        // Write each partition's state
        for (var entry : partitions.entrySet()) {
            TopicPartition tp = entry.getKey();
            PartitionState state = entry.getValue();

            // Write topic name
            byte[] topicBytes = tp.topic().getBytes();
            snapshotBuffer.putInt(offset, topicBytes.length);
            offset += 4;
            snapshotBuffer.putBytes(offset, topicBytes);
            offset += topicBytes.length;

            // Write partition index
            snapshotBuffer.putInt(offset, tp.partition());
            offset += 4;

            // Write state
            snapshotBuffer.putLong(offset, state.nextOffset);
            offset += 8;
            snapshotBuffer.putLong(offset, state.logStartOffset);
            offset += 8;
            snapshotBuffer.putInt(offset, state.leaderEpoch);
            offset += 4;
        }

        // Offer snapshot
        while (snapshotPublication.offer(snapshotBuffer, 0, offset) < 0) {
            idleStrategy.idle();
        }

        LOG.info("Snapshot taken, size={} bytes", offset);
    }

    @Override
    public void onRoleChange(Cluster.Role newRole) {
        LOG.info("Role changed to: {}", newRole);
    }

    @Override
    public void onTerminate(Cluster cluster) {
        LOG.info("Cluster terminating");
    }

    private void loadSnapshot(Image snapshotImage) {
        LOG.info("Loading snapshot");

        UnsafeBuffer buffer = new UnsafeBuffer();

        while (true) {
            int fragments = snapshotImage.poll((buf, offset, length, header) -> {
                buffer.wrap(buf, offset, length);

                int pos = 0;
                int partitionCount = buffer.getInt(pos);
                pos += 4;

                for (int i = 0; i < partitionCount; i++) {
                    // Read topic name
                    int topicLen = buffer.getInt(pos);
                    pos += 4;
                    byte[] topicBytes = new byte[topicLen];
                    buffer.getBytes(pos, topicBytes);
                    pos += topicLen;
                    String topic = new String(topicBytes);

                    // Read partition index
                    int partition = buffer.getInt(pos);
                    pos += 4;

                    // Read state
                    long nextOffset = buffer.getLong(pos);
                    pos += 8;
                    long logStartOffset = buffer.getLong(pos);
                    pos += 8;
                    int leaderEpoch = buffer.getInt(pos);
                    pos += 4;

                    TopicPartition tp = new TopicPartition(topic, partition);
                    partitions.put(tp, new PartitionState(nextOffset, logStartOffset, leaderEpoch));
                }

                LOG.info("Loaded {} partitions from snapshot", partitionCount);
            }, 1);

            if (fragments == 0) {
                if (snapshotImage.isClosed()) {
                    break;
                }
                idleStrategy.idle();
            }
        }
    }

    private void handleProduceCommand(ClientSession session, DirectBuffer buffer, int offset, int length) {
        // Decode produce request
        requestDecoder.wrap(buffer, offset, length);
        RequestHeader header = requestDecoder.decodeHeader();
        ProduceRequest request = (ProduceRequest) requestDecoder.decodeRequest(header);

        // Apply to state
        var responses = new java.util.ArrayList<ProduceResponse.TopicProduceResponse>();

        for (ProduceRequest.TopicProduceData topicData : request.topicData()) {
            var partitionResponses = new java.util.ArrayList<ProduceResponse.PartitionProduceResponse>();

            for (ProduceRequest.PartitionProduceData partitionData : topicData.partitionData()) {
                TopicPartition tp = new TopicPartition(topicData.name(), partitionData.index());

                PartitionState state = partitions.computeIfAbsent(tp,
                        k -> new PartitionState(0, 0, 0));

                long baseOffset = state.nextOffset;
                state.nextOffset++;

                partitionResponses.add(new ProduceResponse.PartitionProduceResponse(
                        partitionData.index(),
                        Errors.NONE.code(),
                        baseOffset,
                        System.currentTimeMillis(),
                        state.logStartOffset
                ));
            }

            responses.add(new ProduceResponse.TopicProduceResponse(topicData.name(), partitionResponses));
        }

        // Send response
        ProduceResponse response = new ProduceResponse(responses, 0);
        sendResponse(session, header.correlationId(), response, header.apiVersion());
    }

    private void handleOffsetCommitCommand(ClientSession session, DirectBuffer buffer, int offset, int length) {
        requestDecoder.wrap(buffer, offset, length);
        RequestHeader header = requestDecoder.decodeHeader();
        OffsetCommitRequest request = (OffsetCommitRequest) requestDecoder.decodeRequest(header);

        OffsetCommitResponse response = groupCoordinator.handleOffsetCommit(request);
        sendResponse(session, header.correlationId(), response, header.apiVersion());
    }

    private void handleJoinGroupCommand(ClientSession session, DirectBuffer buffer, int offset, int length) {
        requestDecoder.wrap(buffer, offset, length);
        RequestHeader header = requestDecoder.decodeHeader();
        JoinGroupRequest request = (JoinGroupRequest) requestDecoder.decodeRequest(header);

        JoinGroupResponse response = groupCoordinator.handleJoinGroup(request);
        sendResponse(session, header.correlationId(), response, header.apiVersion());
    }

    private void handleSyncGroupCommand(ClientSession session, DirectBuffer buffer, int offset, int length) {
        requestDecoder.wrap(buffer, offset, length);
        RequestHeader header = requestDecoder.decodeHeader();
        SyncGroupRequest request = (SyncGroupRequest) requestDecoder.decodeRequest(header);

        SyncGroupResponse response = groupCoordinator.handleSyncGroup(request);
        sendResponse(session, header.correlationId(), response, header.apiVersion());
    }

    private void handleLeaveGroupCommand(ClientSession session, DirectBuffer buffer, int offset, int length) {
        requestDecoder.wrap(buffer, offset, length);
        RequestHeader header = requestDecoder.decodeHeader();
        LeaveGroupRequest request = (LeaveGroupRequest) requestDecoder.decodeRequest(header);

        LeaveGroupResponse response = groupCoordinator.handleLeaveGroup(request);
        sendResponse(session, header.correlationId(), response, header.apiVersion());
    }

    private void handleHeartbeatCommand(ClientSession session, DirectBuffer buffer, int offset, int length) {
        requestDecoder.wrap(buffer, offset, length);
        RequestHeader header = requestDecoder.decodeHeader();
        HeartbeatRequest request = (HeartbeatRequest) requestDecoder.decodeRequest(header);

        HeartbeatResponse response = groupCoordinator.handleHeartbeat(request);
        sendResponse(session, header.correlationId(), response, header.apiVersion());
    }

    private void handleCreateTopicCommand(ClientSession session, DirectBuffer buffer, int offset, int length) {
        // Read topic name and partition count
        int topicLen = buffer.getInt(offset);
        offset += 4;
        byte[] topicBytes = new byte[topicLen];
        buffer.getBytes(offset, topicBytes);
        offset += topicLen;
        String topic = new String(topicBytes);

        int partitionCount = buffer.getInt(offset);

        // Create partitions
        for (int i = 0; i < partitionCount; i++) {
            TopicPartition tp = new TopicPartition(topic, i);
            partitions.putIfAbsent(tp, new PartitionState(0, 0, 0));
        }

        LOG.info("Created topic {} with {} partitions", topic, partitionCount);
    }

    private void handleDeleteTopicCommand(ClientSession session, DirectBuffer buffer, int offset, int length) {
        // Read topic name
        int topicLen = buffer.getInt(offset);
        offset += 4;
        byte[] topicBytes = new byte[topicLen];
        buffer.getBytes(offset, topicBytes);
        String topic = new String(topicBytes);

        // Remove all partitions for this topic
        partitions.keySet().removeIf(tp -> tp.topic().equals(topic));

        LOG.info("Deleted topic {}", topic);
    }

    private void sendResponse(ClientSession session, int correlationId, KafkaResponse response, short apiVersion) {
        responseEncoder.wrap(responseBuffer, 0);
        responseEncoder.encodeHeader(correlationId);
        responseEncoder.encodeResponse(response, apiVersion);

        int length = responseEncoder.encodedLength();

        while (session.offer(responseBuffer, 0, length) < 0) {
            idleStrategy.idle();
        }
    }

    /**
     * Internal state for a partition.
     */
    private static class PartitionState {
        long nextOffset;
        long logStartOffset;
        int leaderEpoch;

        PartitionState(long nextOffset, long logStartOffset, int leaderEpoch) {
            this.nextOffset = nextOffset;
            this.logStartOffset = logStartOffset;
            this.leaderEpoch = leaderEpoch;
        }
    }
}

