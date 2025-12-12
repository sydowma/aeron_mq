package io.aeron.mq.protocol;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * High-performance decoder for Kafka binary protocol.
 * Uses Agrona DirectBuffer for zero-copy parsing.
 */
public final class KafkaRequestDecoder {

    private DirectBuffer buffer;
    private int offset;
    private int limit;

    public KafkaRequestDecoder() {
        this.buffer = new UnsafeBuffer();
    }

    /**
     * Wrap a ByteBuffer for decoding.
     */
    public KafkaRequestDecoder wrap(ByteBuffer byteBuffer) {
        if (buffer instanceof UnsafeBuffer unsafeBuffer) {
            unsafeBuffer.wrap(byteBuffer);
        }
        this.offset = byteBuffer.position();
        this.limit = byteBuffer.limit();
        return this;
    }

    /**
     * Wrap a DirectBuffer for decoding.
     */
    public KafkaRequestDecoder wrap(DirectBuffer buffer, int offset, int length) {
        this.buffer = buffer;
        this.offset = offset;
        this.limit = offset + length;
        return this;
    }

    /**
     * Decode request header from current position.
     */
    public RequestHeader decodeHeader() {
        short apiKey = buffer.getShort(offset, java.nio.ByteOrder.BIG_ENDIAN);
        offset += 2;

        short apiVersion = buffer.getShort(offset, java.nio.ByteOrder.BIG_ENDIAN);
        offset += 2;

        int correlationId = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
        offset += 4;

        String clientId = decodeNullableString();

        return new RequestHeader(apiKey, apiVersion, correlationId, clientId);
    }

    /**
     * Decode a Kafka request based on the API key.
     */
    public KafkaRequest decodeRequest(RequestHeader header) {
        ApiKeys apiKey = ApiKeys.forId(header.apiKey());
        if (apiKey == null) {
            throw new IllegalArgumentException("Unknown API key: " + header.apiKey());
        }

        return switch (apiKey) {
            case API_VERSIONS -> decodeApiVersionsRequest(header.apiVersion());
            case METADATA -> decodeMetadataRequest(header.apiVersion());
            case PRODUCE -> decodeProduceRequest(header.apiVersion());
            case FETCH -> decodeFetchRequest(header.apiVersion());
            case LIST_OFFSETS -> decodeListOffsetsRequest(header.apiVersion());
            case FIND_COORDINATOR -> decodeFindCoordinatorRequest(header.apiVersion());
            case JOIN_GROUP -> decodeJoinGroupRequest(header.apiVersion());
            case SYNC_GROUP -> decodeSyncGroupRequest(header.apiVersion());
            case HEARTBEAT -> decodeHeartbeatRequest(header.apiVersion());
            case LEAVE_GROUP -> decodeLeaveGroupRequest(header.apiVersion());
            case OFFSET_COMMIT -> decodeOffsetCommitRequest(header.apiVersion());
            case OFFSET_FETCH -> decodeOffsetFetchRequest(header.apiVersion());
        };
    }

    private ApiVersionsRequest decodeApiVersionsRequest(short apiVersion) {
        String clientSoftwareName = null;
        String clientSoftwareVersion = null;

        if (apiVersion >= 3) {
            // Tagged fields in v3+
            skipTaggedFields();
            clientSoftwareName = decodeCompactString();
            clientSoftwareVersion = decodeCompactString();
            skipTaggedFields();
        }

        return new ApiVersionsRequest(apiVersion, clientSoftwareName, clientSoftwareVersion);
    }

    private MetadataRequest decodeMetadataRequest(short apiVersion) {
        List<String> topics;
        boolean allowAutoTopicCreation = true;

        if (apiVersion >= 9) {
            // Flexible version with compact arrays
            skipTaggedFields();
            topics = decodeCompactStringArray();
            if (apiVersion >= 4) {
                allowAutoTopicCreation = buffer.getByte(offset++) != 0;
            }
            skipTaggedFields();
        } else {
            int topicCount = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
            offset += 4;

            if (topicCount < 0) {
                topics = null; // null means all topics
            } else {
                topics = new ArrayList<>(topicCount);
                for (int i = 0; i < topicCount; i++) {
                    topics.add(decodeString());
                }
            }

            if (apiVersion >= 4) {
                allowAutoTopicCreation = buffer.getByte(offset++) != 0;
            }
        }

        return new MetadataRequest(apiVersion, topics, allowAutoTopicCreation);
    }

    private ProduceRequest decodeProduceRequest(short apiVersion) {
        String transactionalId = null;

        if (apiVersion >= 9) {
            skipTaggedFields();
        }

        if (apiVersion >= 3) {
            transactionalId = apiVersion >= 9 ? decodeCompactNullableString() : decodeNullableString();
        }

        short acks = buffer.getShort(offset, java.nio.ByteOrder.BIG_ENDIAN);
        offset += 2;

        int timeoutMs = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
        offset += 4;

        List<ProduceRequest.TopicProduceData> topicData;

        if (apiVersion >= 9) {
            int topicCount = decodeUnsignedVarint() - 1;
            topicData = new ArrayList<>(topicCount);

            for (int i = 0; i < topicCount; i++) {
                String topicName = decodeCompactString();
                int partitionCount = decodeUnsignedVarint() - 1;
                List<ProduceRequest.PartitionProduceData> partitionData = new ArrayList<>(partitionCount);

                for (int j = 0; j < partitionCount; j++) {
                    int partitionIndex = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
                    offset += 4;

                    byte[] records = decodeCompactBytes();
                    skipTaggedFields();

                    partitionData.add(new ProduceRequest.PartitionProduceData(partitionIndex, records));
                }

                skipTaggedFields();
                topicData.add(new ProduceRequest.TopicProduceData(topicName, partitionData));
            }

            skipTaggedFields();
        } else {
            int topicCount = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
            offset += 4;
            topicData = new ArrayList<>(topicCount);

            for (int i = 0; i < topicCount; i++) {
                String topicName = decodeString();
                int partitionCount = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
                offset += 4;
                List<ProduceRequest.PartitionProduceData> partitionData = new ArrayList<>(partitionCount);

                for (int j = 0; j < partitionCount; j++) {
                    int partitionIndex = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
                    offset += 4;

                    byte[] records = decodeBytes();
                    partitionData.add(new ProduceRequest.PartitionProduceData(partitionIndex, records));
                }

                topicData.add(new ProduceRequest.TopicProduceData(topicName, partitionData));
            }
        }

        return new ProduceRequest(apiVersion, transactionalId, acks, timeoutMs, topicData);
    }

    private FetchRequest decodeFetchRequest(short apiVersion) {
        if (apiVersion >= 12) {
            skipTaggedFields();
        }

        int replicaId = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
        offset += 4;

        int maxWaitMs = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
        offset += 4;

        int minBytes = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
        offset += 4;

        int maxBytes = 0x7fffffff;
        if (apiVersion >= 3) {
            maxBytes = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
            offset += 4;
        }

        byte isolationLevel = 0;
        if (apiVersion >= 4) {
            isolationLevel = buffer.getByte(offset++);
        }

        int sessionId = 0;
        int sessionEpoch = -1;
        if (apiVersion >= 7) {
            sessionId = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
            offset += 4;
            sessionEpoch = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
            offset += 4;
        }

        List<FetchRequest.FetchTopic> topics;
        if (apiVersion >= 12) {
            int topicCount = decodeUnsignedVarint() - 1;
            topics = new ArrayList<>(topicCount);

            for (int i = 0; i < topicCount; i++) {
                String topicName = decodeCompactString();
                int partitionCount = decodeUnsignedVarint() - 1;
                List<FetchRequest.FetchPartition> partitions = new ArrayList<>(partitionCount);

                for (int j = 0; j < partitionCount; j++) {
                    int partition = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
                    offset += 4;

                    int currentLeaderEpoch = -1;
                    if (apiVersion >= 9) {
                        currentLeaderEpoch = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
                        offset += 4;
                    }

                    long fetchOffset = buffer.getLong(offset, java.nio.ByteOrder.BIG_ENDIAN);
                    offset += 8;

                    int lastFetchedEpoch = -1;
                    if (apiVersion >= 12) {
                        lastFetchedEpoch = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
                        offset += 4;
                    }

                    long logStartOffset = -1;
                    if (apiVersion >= 5) {
                        logStartOffset = buffer.getLong(offset, java.nio.ByteOrder.BIG_ENDIAN);
                        offset += 8;
                    }

                    int partitionMaxBytes = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
                    offset += 4;

                    skipTaggedFields();
                    partitions.add(new FetchRequest.FetchPartition(
                            partition, currentLeaderEpoch, fetchOffset,
                            lastFetchedEpoch, logStartOffset, partitionMaxBytes));
                }

                skipTaggedFields();
                topics.add(new FetchRequest.FetchTopic(topicName, partitions));
            }

            skipTaggedFields();
        } else {
            int topicCount = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
            offset += 4;
            topics = new ArrayList<>(topicCount);

            for (int i = 0; i < topicCount; i++) {
                String topicName = decodeString();
                int partitionCount = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
                offset += 4;
                List<FetchRequest.FetchPartition> partitions = new ArrayList<>(partitionCount);

                for (int j = 0; j < partitionCount; j++) {
                    int partition = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
                    offset += 4;

                    int currentLeaderEpoch = -1;
                    if (apiVersion >= 9) {
                        currentLeaderEpoch = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
                        offset += 4;
                    }

                    long fetchOffset = buffer.getLong(offset, java.nio.ByteOrder.BIG_ENDIAN);
                    offset += 8;

                    long logStartOffset = -1;
                    if (apiVersion >= 5) {
                        logStartOffset = buffer.getLong(offset, java.nio.ByteOrder.BIG_ENDIAN);
                        offset += 8;
                    }

                    int partitionMaxBytes = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
                    offset += 4;

                    partitions.add(new FetchRequest.FetchPartition(
                            partition, currentLeaderEpoch, fetchOffset,
                            -1, logStartOffset, partitionMaxBytes));
                }

                topics.add(new FetchRequest.FetchTopic(topicName, partitions));
            }
        }

        return new FetchRequest(apiVersion, replicaId, maxWaitMs, minBytes, maxBytes,
                isolationLevel, sessionId, sessionEpoch, topics);
    }

    private ListOffsetsRequest decodeListOffsetsRequest(short apiVersion) {
        if (apiVersion >= 6) {
            skipTaggedFields();
        }

        int replicaId = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
        offset += 4;

        byte isolationLevel = 0;
        if (apiVersion >= 2) {
            isolationLevel = buffer.getByte(offset++);
        }

        List<ListOffsetsRequest.ListOffsetsTopic> topics;
        if (apiVersion >= 6) {
            int topicCount = decodeUnsignedVarint() - 1;
            topics = new ArrayList<>(topicCount);

            for (int i = 0; i < topicCount; i++) {
                String topicName = decodeCompactString();
                int partitionCount = decodeUnsignedVarint() - 1;
                List<ListOffsetsRequest.ListOffsetsPartition> partitions = new ArrayList<>(partitionCount);

                for (int j = 0; j < partitionCount; j++) {
                    int partitionIndex = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
                    offset += 4;

                    int currentLeaderEpoch = -1;
                    if (apiVersion >= 4) {
                        currentLeaderEpoch = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
                        offset += 4;
                    }

                    long timestamp = buffer.getLong(offset, java.nio.ByteOrder.BIG_ENDIAN);
                    offset += 8;

                    skipTaggedFields();
                    partitions.add(new ListOffsetsRequest.ListOffsetsPartition(
                            partitionIndex, currentLeaderEpoch, timestamp));
                }

                skipTaggedFields();
                topics.add(new ListOffsetsRequest.ListOffsetsTopic(topicName, partitions));
            }

            skipTaggedFields();
        } else {
            int topicCount = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
            offset += 4;
            topics = new ArrayList<>(topicCount);

            for (int i = 0; i < topicCount; i++) {
                String topicName = decodeString();
                int partitionCount = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
                offset += 4;
                List<ListOffsetsRequest.ListOffsetsPartition> partitions = new ArrayList<>(partitionCount);

                for (int j = 0; j < partitionCount; j++) {
                    int partitionIndex = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
                    offset += 4;

                    int currentLeaderEpoch = -1;
                    if (apiVersion >= 4) {
                        currentLeaderEpoch = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
                        offset += 4;
                    }

                    long timestamp = buffer.getLong(offset, java.nio.ByteOrder.BIG_ENDIAN);
                    offset += 8;

                    partitions.add(new ListOffsetsRequest.ListOffsetsPartition(
                            partitionIndex, currentLeaderEpoch, timestamp));
                }

                topics.add(new ListOffsetsRequest.ListOffsetsTopic(topicName, partitions));
            }
        }

        return new ListOffsetsRequest(apiVersion, replicaId, isolationLevel, topics);
    }

    private FindCoordinatorRequest decodeFindCoordinatorRequest(short apiVersion) {
        if (apiVersion >= 4) {
            skipTaggedFields();
        }

        String key = apiVersion >= 4 ? decodeCompactString() : decodeString();
        byte keyType = 0;
        List<String> coordinatorKeys = null;

        if (apiVersion >= 1) {
            keyType = buffer.getByte(offset++);
        }

        if (apiVersion >= 4) {
            coordinatorKeys = decodeCompactStringArray();
            skipTaggedFields();
        }

        return new FindCoordinatorRequest(apiVersion, key, keyType, coordinatorKeys);
    }

    private JoinGroupRequest decodeJoinGroupRequest(short apiVersion) {
        if (apiVersion >= 6) {
            skipTaggedFields();
        }

        String groupId = apiVersion >= 6 ? decodeCompactString() : decodeString();
        int sessionTimeoutMs = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
        offset += 4;

        int rebalanceTimeoutMs = sessionTimeoutMs;
        if (apiVersion >= 1) {
            rebalanceTimeoutMs = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
            offset += 4;
        }

        String memberId = apiVersion >= 6 ? decodeCompactString() : decodeString();

        String groupInstanceId = null;
        if (apiVersion >= 5) {
            groupInstanceId = apiVersion >= 6 ? decodeCompactNullableString() : decodeNullableString();
        }

        String protocolType = apiVersion >= 6 ? decodeCompactString() : decodeString();

        List<JoinGroupRequest.JoinGroupProtocol> protocols;
        if (apiVersion >= 6) {
            int protocolCount = decodeUnsignedVarint() - 1;
            protocols = new ArrayList<>(protocolCount);

            for (int i = 0; i < protocolCount; i++) {
                String name = decodeCompactString();
                byte[] metadata = decodeCompactBytes();
                skipTaggedFields();
                protocols.add(new JoinGroupRequest.JoinGroupProtocol(name, metadata));
            }
        } else {
            int protocolCount = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
            offset += 4;
            protocols = new ArrayList<>(protocolCount);

            for (int i = 0; i < protocolCount; i++) {
                String name = decodeString();
                byte[] metadata = decodeBytes();
                protocols.add(new JoinGroupRequest.JoinGroupProtocol(name, metadata));
            }
        }

        String reason = null;
        if (apiVersion >= 8) {
            reason = decodeCompactNullableString();
            skipTaggedFields();
        } else if (apiVersion >= 6) {
            skipTaggedFields();
        }

        return new JoinGroupRequest(apiVersion, groupId, sessionTimeoutMs, rebalanceTimeoutMs,
                memberId, groupInstanceId, protocolType, protocols, reason);
    }

    private SyncGroupRequest decodeSyncGroupRequest(short apiVersion) {
        if (apiVersion >= 4) {
            skipTaggedFields();
        }

        String groupId = apiVersion >= 4 ? decodeCompactString() : decodeString();
        int generationId = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
        offset += 4;
        String memberId = apiVersion >= 4 ? decodeCompactString() : decodeString();

        String groupInstanceId = null;
        if (apiVersion >= 3) {
            groupInstanceId = apiVersion >= 4 ? decodeCompactNullableString() : decodeNullableString();
        }

        String protocolType = null;
        String protocolName = null;
        if (apiVersion >= 5) {
            protocolType = decodeCompactNullableString();
            protocolName = decodeCompactNullableString();
        }

        List<SyncGroupRequest.SyncGroupAssignment> assignments;
        if (apiVersion >= 4) {
            int assignmentCount = decodeUnsignedVarint() - 1;
            assignments = new ArrayList<>(assignmentCount);

            for (int i = 0; i < assignmentCount; i++) {
                String memberIdAssign = decodeCompactString();
                byte[] assignment = decodeCompactBytes();
                skipTaggedFields();
                assignments.add(new SyncGroupRequest.SyncGroupAssignment(memberIdAssign, assignment));
            }

            skipTaggedFields();
        } else {
            int assignmentCount = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
            offset += 4;
            assignments = new ArrayList<>(assignmentCount);

            for (int i = 0; i < assignmentCount; i++) {
                String memberIdAssign = decodeString();
                byte[] assignment = decodeBytes();
                assignments.add(new SyncGroupRequest.SyncGroupAssignment(memberIdAssign, assignment));
            }
        }

        return new SyncGroupRequest(apiVersion, groupId, generationId, memberId,
                groupInstanceId, protocolType, protocolName, assignments);
    }

    private HeartbeatRequest decodeHeartbeatRequest(short apiVersion) {
        if (apiVersion >= 4) {
            skipTaggedFields();
        }

        String groupId = apiVersion >= 4 ? decodeCompactString() : decodeString();
        int generationId = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
        offset += 4;
        String memberId = apiVersion >= 4 ? decodeCompactString() : decodeString();

        String groupInstanceId = null;
        if (apiVersion >= 3) {
            groupInstanceId = apiVersion >= 4 ? decodeCompactNullableString() : decodeNullableString();
        }

        if (apiVersion >= 4) {
            skipTaggedFields();
        }

        return new HeartbeatRequest(apiVersion, groupId, generationId, memberId, groupInstanceId);
    }

    private LeaveGroupRequest decodeLeaveGroupRequest(short apiVersion) {
        if (apiVersion >= 4) {
            skipTaggedFields();
        }

        String groupId = apiVersion >= 4 ? decodeCompactString() : decodeString();

        String memberId = null;
        List<LeaveGroupRequest.MemberIdentity> members = null;

        if (apiVersion < 3) {
            memberId = apiVersion >= 4 ? decodeCompactString() : decodeString();
        } else {
            if (apiVersion >= 4) {
                int memberCount = decodeUnsignedVarint() - 1;
                members = new ArrayList<>(memberCount);

                for (int i = 0; i < memberCount; i++) {
                    String memberIdItem = decodeCompactString();
                    String groupInstanceId = decodeCompactNullableString();

                    String reason = null;
                    if (apiVersion >= 5) {
                        reason = decodeCompactNullableString();
                    }

                    skipTaggedFields();
                    members.add(new LeaveGroupRequest.MemberIdentity(memberIdItem, groupInstanceId, reason));
                }
            } else {
                int memberCount = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
                offset += 4;
                members = new ArrayList<>(memberCount);

                for (int i = 0; i < memberCount; i++) {
                    String memberIdItem = decodeString();
                    String groupInstanceId = decodeNullableString();
                    members.add(new LeaveGroupRequest.MemberIdentity(memberIdItem, groupInstanceId, null));
                }
            }
        }

        if (apiVersion >= 4) {
            skipTaggedFields();
        }

        return new LeaveGroupRequest(apiVersion, groupId, memberId, members);
    }

    private OffsetCommitRequest decodeOffsetCommitRequest(short apiVersion) {
        if (apiVersion >= 8) {
            skipTaggedFields();
        }

        String groupId = apiVersion >= 8 ? decodeCompactString() : decodeString();

        int generationIdOrMemberEpoch = -1;
        String memberId = "";
        String groupInstanceId = null;

        if (apiVersion >= 1) {
            generationIdOrMemberEpoch = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
            offset += 4;
            memberId = apiVersion >= 8 ? decodeCompactString() : decodeString();

            if (apiVersion >= 7) {
                groupInstanceId = apiVersion >= 8 ? decodeCompactNullableString() : decodeNullableString();
            }
        }

        List<OffsetCommitRequest.OffsetCommitTopic> topics;
        if (apiVersion >= 8) {
            int topicCount = decodeUnsignedVarint() - 1;
            topics = new ArrayList<>(topicCount);

            for (int i = 0; i < topicCount; i++) {
                String topicName = decodeCompactString();
                int partitionCount = decodeUnsignedVarint() - 1;
                List<OffsetCommitRequest.OffsetCommitPartition> partitions = new ArrayList<>(partitionCount);

                for (int j = 0; j < partitionCount; j++) {
                    int partitionIndex = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
                    offset += 4;
                    long committedOffset = buffer.getLong(offset, java.nio.ByteOrder.BIG_ENDIAN);
                    offset += 8;

                    int committedLeaderEpoch = -1;
                    if (apiVersion >= 6) {
                        committedLeaderEpoch = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
                        offset += 4;
                    }

                    String committedMetadata = decodeCompactNullableString();
                    skipTaggedFields();
                    partitions.add(new OffsetCommitRequest.OffsetCommitPartition(
                            partitionIndex, committedOffset, committedLeaderEpoch, -1, committedMetadata));
                }

                skipTaggedFields();
                topics.add(new OffsetCommitRequest.OffsetCommitTopic(topicName, partitions));
            }

            skipTaggedFields();
        } else {
            int topicCount = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
            offset += 4;
            topics = new ArrayList<>(topicCount);

            for (int i = 0; i < topicCount; i++) {
                String topicName = decodeString();
                int partitionCount = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
                offset += 4;
                List<OffsetCommitRequest.OffsetCommitPartition> partitions = new ArrayList<>(partitionCount);

                for (int j = 0; j < partitionCount; j++) {
                    int partitionIndex = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
                    offset += 4;
                    long committedOffset = buffer.getLong(offset, java.nio.ByteOrder.BIG_ENDIAN);
                    offset += 8;

                    long commitTimestamp = -1;
                    if (apiVersion == 1) {
                        commitTimestamp = buffer.getLong(offset, java.nio.ByteOrder.BIG_ENDIAN);
                        offset += 8;
                    }

                    int committedLeaderEpoch = -1;
                    if (apiVersion >= 6) {
                        committedLeaderEpoch = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
                        offset += 4;
                    }

                    String committedMetadata = decodeNullableString();
                    partitions.add(new OffsetCommitRequest.OffsetCommitPartition(
                            partitionIndex, committedOffset, committedLeaderEpoch, commitTimestamp, committedMetadata));
                }

                topics.add(new OffsetCommitRequest.OffsetCommitTopic(topicName, partitions));
            }
        }

        return new OffsetCommitRequest(apiVersion, groupId, generationIdOrMemberEpoch,
                memberId, groupInstanceId, topics);
    }

    private OffsetFetchRequest decodeOffsetFetchRequest(short apiVersion) {
        if (apiVersion >= 6) {
            skipTaggedFields();
        }

        String groupId = null;
        List<OffsetFetchRequest.OffsetFetchTopic> topics = null;
        List<OffsetFetchRequest.OffsetFetchGroup> groups = null;
        boolean requireStable = false;

        if (apiVersion < 8) {
            groupId = apiVersion >= 6 ? decodeCompactString() : decodeString();

            if (apiVersion >= 6) {
                int topicCount = decodeUnsignedVarint() - 1;
                if (topicCount >= 0) {
                    topics = new ArrayList<>(topicCount);
                    for (int i = 0; i < topicCount; i++) {
                        String topicName = decodeCompactString();
                        int partitionCount = decodeUnsignedVarint() - 1;
                        List<Integer> partitionIndexes = new ArrayList<>(partitionCount);
                        for (int j = 0; j < partitionCount; j++) {
                            partitionIndexes.add(buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN));
                            offset += 4;
                        }
                        skipTaggedFields();
                        topics.add(new OffsetFetchRequest.OffsetFetchTopic(topicName, partitionIndexes));
                    }
                }
            } else {
                int topicCount = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
                offset += 4;
                if (topicCount >= 0) {
                    topics = new ArrayList<>(topicCount);
                    for (int i = 0; i < topicCount; i++) {
                        String topicName = decodeString();
                        int partitionCount = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
                        offset += 4;
                        List<Integer> partitionIndexes = new ArrayList<>(partitionCount);
                        for (int j = 0; j < partitionCount; j++) {
                            partitionIndexes.add(buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN));
                            offset += 4;
                        }
                        topics.add(new OffsetFetchRequest.OffsetFetchTopic(topicName, partitionIndexes));
                    }
                }
            }

            if (apiVersion >= 7) {
                requireStable = buffer.getByte(offset++) != 0;
            }
        } else {
            // v8+ uses groups array
            int groupCount = decodeUnsignedVarint() - 1;
            groups = new ArrayList<>(groupCount);

            for (int i = 0; i < groupCount; i++) {
                String gId = decodeCompactString();
                String memberId = null;
                int memberEpoch = -1;
                List<OffsetFetchRequest.OffsetFetchTopic> gTopics = null;

                if (apiVersion >= 9) {
                    memberId = decodeCompactNullableString();
                    memberEpoch = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
                    offset += 4;
                }

                int topicCount = decodeUnsignedVarint() - 1;
                if (topicCount >= 0) {
                    gTopics = new ArrayList<>(topicCount);
                    for (int j = 0; j < topicCount; j++) {
                        String topicName = decodeCompactString();
                        int partitionCount = decodeUnsignedVarint() - 1;
                        List<Integer> partitionIndexes = new ArrayList<>(partitionCount);
                        for (int k = 0; k < partitionCount; k++) {
                            partitionIndexes.add(buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN));
                            offset += 4;
                        }
                        skipTaggedFields();
                        gTopics.add(new OffsetFetchRequest.OffsetFetchTopic(topicName, partitionIndexes));
                    }
                }

                skipTaggedFields();
                groups.add(new OffsetFetchRequest.OffsetFetchGroup(gId, memberId, memberEpoch, gTopics));
            }

            requireStable = buffer.getByte(offset++) != 0;
        }

        if (apiVersion >= 6) {
            skipTaggedFields();
        }

        return new OffsetFetchRequest(apiVersion, groupId, topics, groups, requireStable);
    }

    // Primitive decoders

    private String decodeString() {
        short length = buffer.getShort(offset, java.nio.ByteOrder.BIG_ENDIAN);
        offset += 2;
        if (length < 0) {
            return null;
        }
        byte[] bytes = new byte[length];
        buffer.getBytes(offset, bytes);
        offset += length;
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private String decodeNullableString() {
        short length = buffer.getShort(offset, java.nio.ByteOrder.BIG_ENDIAN);
        offset += 2;
        if (length < 0) {
            return null;
        }
        byte[] bytes = new byte[length];
        buffer.getBytes(offset, bytes);
        offset += length;
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private String decodeCompactString() {
        int length = decodeUnsignedVarint() - 1;
        if (length < 0) {
            return null;
        }
        byte[] bytes = new byte[length];
        buffer.getBytes(offset, bytes);
        offset += length;
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private String decodeCompactNullableString() {
        int length = decodeUnsignedVarint() - 1;
        if (length < 0) {
            return null;
        }
        byte[] bytes = new byte[length];
        buffer.getBytes(offset, bytes);
        offset += length;
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private List<String> decodeCompactStringArray() {
        int count = decodeUnsignedVarint() - 1;
        if (count < 0) {
            return null;
        }
        List<String> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            result.add(decodeCompactString());
        }
        return result;
    }

    private byte[] decodeBytes() {
        int length = buffer.getInt(offset, java.nio.ByteOrder.BIG_ENDIAN);
        offset += 4;
        if (length < 0) {
            return null;
        }
        byte[] bytes = new byte[length];
        buffer.getBytes(offset, bytes);
        offset += length;
        return bytes;
    }

    private byte[] decodeCompactBytes() {
        int length = decodeUnsignedVarint() - 1;
        if (length < 0) {
            return null;
        }
        byte[] bytes = new byte[length];
        buffer.getBytes(offset, bytes);
        offset += length;
        return bytes;
    }

    private int decodeUnsignedVarint() {
        int value = 0;
        int shift = 0;
        byte b;
        do {
            b = buffer.getByte(offset++);
            value |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return value;
    }

    private void skipTaggedFields() {
        int numTaggedFields = decodeUnsignedVarint();
        for (int i = 0; i < numTaggedFields; i++) {
            decodeUnsignedVarint(); // tag
            int size = decodeUnsignedVarint();
            offset += size;
        }
    }
}


