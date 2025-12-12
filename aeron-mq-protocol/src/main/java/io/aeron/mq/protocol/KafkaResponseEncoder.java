package io.aeron.mq.protocol;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * High-performance encoder for Kafka binary protocol responses.
 * Uses Agrona MutableDirectBuffer for zero-copy encoding.
 */
public final class KafkaResponseEncoder {

    private MutableDirectBuffer buffer;
    private int offset;
    private int startOffset;

    public KafkaResponseEncoder() {
        this.buffer = new UnsafeBuffer();
    }

    /**
     * Wrap a ByteBuffer for encoding.
     */
    public KafkaResponseEncoder wrap(ByteBuffer byteBuffer) {
        if (buffer instanceof UnsafeBuffer unsafeBuffer) {
            unsafeBuffer.wrap(byteBuffer);
        }
        this.offset = byteBuffer.position();
        this.startOffset = this.offset;
        return this;
    }

    /**
     * Wrap a MutableDirectBuffer for encoding.
     */
    public KafkaResponseEncoder wrap(MutableDirectBuffer buffer, int offset) {
        this.buffer = buffer;
        this.offset = offset;
        this.startOffset = offset;
        return this;
    }

    /**
     * Get current write position.
     */
    public int position() {
        return offset;
    }

    /**
     * Get number of bytes written.
     */
    public int encodedLength() {
        return offset - startOffset;
    }

    /**
     * Encode response header.
     */
    public KafkaResponseEncoder encodeHeader(int correlationId) {
        buffer.putInt(offset, correlationId, ByteOrder.BIG_ENDIAN);
        offset += 4;
        return this;
    }

    /**
     * Encode a response based on its type.
     */
    public KafkaResponseEncoder encodeResponse(KafkaResponse response, short apiVersion) {
        return switch (response) {
            case ApiVersionsResponse r -> encodeApiVersionsResponse(r, apiVersion);
            case MetadataResponse r -> encodeMetadataResponse(r, apiVersion);
            case ProduceResponse r -> encodeProduceResponse(r, apiVersion);
            case FetchResponse r -> encodeFetchResponse(r, apiVersion);
            case ListOffsetsResponse r -> encodeListOffsetsResponse(r, apiVersion);
            case FindCoordinatorResponse r -> encodeFindCoordinatorResponse(r, apiVersion);
            case JoinGroupResponse r -> encodeJoinGroupResponse(r, apiVersion);
            case SyncGroupResponse r -> encodeSyncGroupResponse(r, apiVersion);
            case HeartbeatResponse r -> encodeHeartbeatResponse(r, apiVersion);
            case LeaveGroupResponse r -> encodeLeaveGroupResponse(r, apiVersion);
            case OffsetCommitResponse r -> encodeOffsetCommitResponse(r, apiVersion);
            case OffsetFetchResponse r -> encodeOffsetFetchResponse(r, apiVersion);
        };
    }

    private KafkaResponseEncoder encodeApiVersionsResponse(ApiVersionsResponse response, short apiVersion) {
        buffer.putShort(offset, response.errorCode(), ByteOrder.BIG_ENDIAN);
        offset += 2;

        if (apiVersion >= 3) {
            // Compact array
            encodeUnsignedVarint(response.apiVersions().size() + 1);
            for (ApiVersionsResponse.ApiVersion av : response.apiVersions()) {
                buffer.putShort(offset, av.apiKey(), ByteOrder.BIG_ENDIAN);
                offset += 2;
                buffer.putShort(offset, av.minVersion(), ByteOrder.BIG_ENDIAN);
                offset += 2;
                buffer.putShort(offset, av.maxVersion(), ByteOrder.BIG_ENDIAN);
                offset += 2;
                encodeUnsignedVarint(0); // tagged fields
            }
            buffer.putInt(offset, response.throttleTimeMs(), ByteOrder.BIG_ENDIAN);
            offset += 4;
            encodeUnsignedVarint(0); // tagged fields
        } else {
            buffer.putInt(offset, response.apiVersions().size(), ByteOrder.BIG_ENDIAN);
            offset += 4;

            for (ApiVersionsResponse.ApiVersion av : response.apiVersions()) {
                buffer.putShort(offset, av.apiKey(), ByteOrder.BIG_ENDIAN);
                offset += 2;
                buffer.putShort(offset, av.minVersion(), ByteOrder.BIG_ENDIAN);
                offset += 2;
                buffer.putShort(offset, av.maxVersion(), ByteOrder.BIG_ENDIAN);
                offset += 2;
            }

            if (apiVersion >= 1) {
                buffer.putInt(offset, response.throttleTimeMs(), ByteOrder.BIG_ENDIAN);
                offset += 4;
            }
        }

        return this;
    }

    private KafkaResponseEncoder encodeMetadataResponse(MetadataResponse response, short apiVersion) {
        if (apiVersion >= 9) {
            encodeUnsignedVarint(0); // tagged fields header
        }

        if (apiVersion >= 3) {
            buffer.putInt(offset, response.throttleTimeMs(), ByteOrder.BIG_ENDIAN);
            offset += 4;
        }

        // Brokers
        if (apiVersion >= 9) {
            encodeUnsignedVarint(response.brokers().size() + 1);
            for (MetadataResponse.BrokerMetadata broker : response.brokers()) {
                buffer.putInt(offset, broker.nodeId(), ByteOrder.BIG_ENDIAN);
                offset += 4;
                encodeCompactString(broker.host());
                buffer.putInt(offset, broker.port(), ByteOrder.BIG_ENDIAN);
                offset += 4;
                if (apiVersion >= 1) {
                    encodeCompactNullableString(broker.rack());
                }
                encodeUnsignedVarint(0); // tagged fields
            }
        } else {
            buffer.putInt(offset, response.brokers().size(), ByteOrder.BIG_ENDIAN);
            offset += 4;
            for (MetadataResponse.BrokerMetadata broker : response.brokers()) {
                buffer.putInt(offset, broker.nodeId(), ByteOrder.BIG_ENDIAN);
                offset += 4;
                encodeString(broker.host());
                buffer.putInt(offset, broker.port(), ByteOrder.BIG_ENDIAN);
                offset += 4;
                if (apiVersion >= 1) {
                    encodeNullableString(broker.rack());
                }
            }
        }

        // Cluster ID
        if (apiVersion >= 2) {
            if (apiVersion >= 9) {
                encodeCompactNullableString(response.clusterId());
            } else {
                encodeNullableString(response.clusterId());
            }
        }

        // Controller ID
        if (apiVersion >= 1) {
            buffer.putInt(offset, response.controllerId(), ByteOrder.BIG_ENDIAN);
            offset += 4;
        }

        // Topics
        if (apiVersion >= 9) {
            encodeUnsignedVarint(response.topics().size() + 1);
            for (MetadataResponse.TopicMetadata topic : response.topics()) {
                buffer.putShort(offset, topic.errorCode(), ByteOrder.BIG_ENDIAN);
                offset += 2;
                encodeCompactString(topic.name());
                if (apiVersion >= 1) {
                    buffer.putByte(offset++, topic.isInternal() ? (byte) 1 : (byte) 0);
                }

                encodeUnsignedVarint(topic.partitions().size() + 1);
                for (MetadataResponse.PartitionMetadata partition : topic.partitions()) {
                    buffer.putShort(offset, partition.errorCode(), ByteOrder.BIG_ENDIAN);
                    offset += 2;
                    buffer.putInt(offset, partition.partitionIndex(), ByteOrder.BIG_ENDIAN);
                    offset += 4;
                    buffer.putInt(offset, partition.leaderId(), ByteOrder.BIG_ENDIAN);
                    offset += 4;

                    if (apiVersion >= 7) {
                        buffer.putInt(offset, partition.leaderEpoch(), ByteOrder.BIG_ENDIAN);
                        offset += 4;
                    }

                    encodeUnsignedVarint(partition.replicaNodes().size() + 1);
                    for (int nodeId : partition.replicaNodes()) {
                        buffer.putInt(offset, nodeId, ByteOrder.BIG_ENDIAN);
                        offset += 4;
                    }

                    encodeUnsignedVarint(partition.isrNodes().size() + 1);
                    for (int nodeId : partition.isrNodes()) {
                        buffer.putInt(offset, nodeId, ByteOrder.BIG_ENDIAN);
                        offset += 4;
                    }

                    encodeUnsignedVarint(0); // tagged fields
                }
                encodeUnsignedVarint(0); // tagged fields
            }
            encodeUnsignedVarint(0); // tagged fields
        } else {
            buffer.putInt(offset, response.topics().size(), ByteOrder.BIG_ENDIAN);
            offset += 4;
            for (MetadataResponse.TopicMetadata topic : response.topics()) {
                buffer.putShort(offset, topic.errorCode(), ByteOrder.BIG_ENDIAN);
                offset += 2;
                encodeString(topic.name());
                if (apiVersion >= 1) {
                    buffer.putByte(offset++, topic.isInternal() ? (byte) 1 : (byte) 0);
                }

                buffer.putInt(offset, topic.partitions().size(), ByteOrder.BIG_ENDIAN);
                offset += 4;
                for (MetadataResponse.PartitionMetadata partition : topic.partitions()) {
                    buffer.putShort(offset, partition.errorCode(), ByteOrder.BIG_ENDIAN);
                    offset += 2;
                    buffer.putInt(offset, partition.partitionIndex(), ByteOrder.BIG_ENDIAN);
                    offset += 4;
                    buffer.putInt(offset, partition.leaderId(), ByteOrder.BIG_ENDIAN);
                    offset += 4;

                    if (apiVersion >= 7) {
                        buffer.putInt(offset, partition.leaderEpoch(), ByteOrder.BIG_ENDIAN);
                        offset += 4;
                    }

                    buffer.putInt(offset, partition.replicaNodes().size(), ByteOrder.BIG_ENDIAN);
                    offset += 4;
                    for (int nodeId : partition.replicaNodes()) {
                        buffer.putInt(offset, nodeId, ByteOrder.BIG_ENDIAN);
                        offset += 4;
                    }

                    buffer.putInt(offset, partition.isrNodes().size(), ByteOrder.BIG_ENDIAN);
                    offset += 4;
                    for (int nodeId : partition.isrNodes()) {
                        buffer.putInt(offset, nodeId, ByteOrder.BIG_ENDIAN);
                        offset += 4;
                    }
                }
            }
        }

        return this;
    }

    private KafkaResponseEncoder encodeProduceResponse(ProduceResponse response, short apiVersion) {
        if (apiVersion >= 9) {
            encodeUnsignedVarint(0); // tagged fields header
            encodeUnsignedVarint(response.responses().size() + 1);

            for (ProduceResponse.TopicProduceResponse topic : response.responses()) {
                encodeCompactString(topic.name());
                encodeUnsignedVarint(topic.partitionResponses().size() + 1);

                for (ProduceResponse.PartitionProduceResponse partition : topic.partitionResponses()) {
                    buffer.putInt(offset, partition.index(), ByteOrder.BIG_ENDIAN);
                    offset += 4;
                    buffer.putShort(offset, partition.errorCode(), ByteOrder.BIG_ENDIAN);
                    offset += 2;
                    buffer.putLong(offset, partition.baseOffset(), ByteOrder.BIG_ENDIAN);
                    offset += 8;

                    if (apiVersion >= 2) {
                        buffer.putLong(offset, partition.logAppendTimeMs(), ByteOrder.BIG_ENDIAN);
                        offset += 8;
                    }

                    if (apiVersion >= 5) {
                        buffer.putLong(offset, partition.logStartOffset(), ByteOrder.BIG_ENDIAN);
                        offset += 8;
                    }

                    encodeUnsignedVarint(0); // tagged fields
                }
                encodeUnsignedVarint(0); // tagged fields
            }

            buffer.putInt(offset, response.throttleTimeMs(), ByteOrder.BIG_ENDIAN);
            offset += 4;
            encodeUnsignedVarint(0); // tagged fields
        } else {
            buffer.putInt(offset, response.responses().size(), ByteOrder.BIG_ENDIAN);
            offset += 4;

            for (ProduceResponse.TopicProduceResponse topic : response.responses()) {
                encodeString(topic.name());
                buffer.putInt(offset, topic.partitionResponses().size(), ByteOrder.BIG_ENDIAN);
                offset += 4;

                for (ProduceResponse.PartitionProduceResponse partition : topic.partitionResponses()) {
                    buffer.putInt(offset, partition.index(), ByteOrder.BIG_ENDIAN);
                    offset += 4;
                    buffer.putShort(offset, partition.errorCode(), ByteOrder.BIG_ENDIAN);
                    offset += 2;
                    buffer.putLong(offset, partition.baseOffset(), ByteOrder.BIG_ENDIAN);
                    offset += 8;

                    if (apiVersion >= 2) {
                        buffer.putLong(offset, partition.logAppendTimeMs(), ByteOrder.BIG_ENDIAN);
                        offset += 8;
                    }

                    if (apiVersion >= 5) {
                        buffer.putLong(offset, partition.logStartOffset(), ByteOrder.BIG_ENDIAN);
                        offset += 8;
                    }
                }
            }

            if (apiVersion >= 1) {
                buffer.putInt(offset, response.throttleTimeMs(), ByteOrder.BIG_ENDIAN);
                offset += 4;
            }
        }

        return this;
    }

    private KafkaResponseEncoder encodeFetchResponse(FetchResponse response, short apiVersion) {
        if (apiVersion >= 12) {
            encodeUnsignedVarint(0); // tagged fields header
        }

        if (apiVersion >= 1) {
            buffer.putInt(offset, response.throttleTimeMs(), ByteOrder.BIG_ENDIAN);
            offset += 4;
        }

        if (apiVersion >= 7) {
            buffer.putShort(offset, response.errorCode(), ByteOrder.BIG_ENDIAN);
            offset += 2;
            buffer.putInt(offset, response.sessionId(), ByteOrder.BIG_ENDIAN);
            offset += 4;
        }

        if (apiVersion >= 12) {
            encodeUnsignedVarint(response.responses().size() + 1);

            for (FetchResponse.FetchableTopicResponse topic : response.responses()) {
                encodeCompactString(topic.topic());
                encodeUnsignedVarint(topic.partitions().size() + 1);

                for (FetchResponse.PartitionData partition : topic.partitions()) {
                    buffer.putInt(offset, partition.partitionIndex(), ByteOrder.BIG_ENDIAN);
                    offset += 4;
                    buffer.putShort(offset, partition.errorCode(), ByteOrder.BIG_ENDIAN);
                    offset += 2;
                    buffer.putLong(offset, partition.highWatermark(), ByteOrder.BIG_ENDIAN);
                    offset += 8;

                    if (apiVersion >= 4) {
                        buffer.putLong(offset, partition.lastStableOffset(), ByteOrder.BIG_ENDIAN);
                        offset += 8;
                    }

                    if (apiVersion >= 5) {
                        buffer.putLong(offset, partition.logStartOffset(), ByteOrder.BIG_ENDIAN);
                        offset += 8;
                    }

                    if (apiVersion >= 4) {
                        List<FetchResponse.AbortedTransaction> aborted = partition.abortedTransactions();
                        if (aborted == null) {
                            encodeUnsignedVarint(0);
                        } else {
                            encodeUnsignedVarint(aborted.size() + 1);
                            for (FetchResponse.AbortedTransaction tx : aborted) {
                                buffer.putLong(offset, tx.producerId(), ByteOrder.BIG_ENDIAN);
                                offset += 8;
                                buffer.putLong(offset, tx.firstOffset(), ByteOrder.BIG_ENDIAN);
                                offset += 8;
                                encodeUnsignedVarint(0); // tagged fields
                            }
                        }
                    }

                    if (apiVersion >= 11) {
                        buffer.putInt(offset, partition.preferredReadReplica(), ByteOrder.BIG_ENDIAN);
                        offset += 4;
                    }

                    encodeCompactBytes(partition.records());
                    encodeUnsignedVarint(0); // tagged fields
                }
                encodeUnsignedVarint(0); // tagged fields
            }
            encodeUnsignedVarint(0); // tagged fields
        } else {
            buffer.putInt(offset, response.responses().size(), ByteOrder.BIG_ENDIAN);
            offset += 4;

            for (FetchResponse.FetchableTopicResponse topic : response.responses()) {
                encodeString(topic.topic());
                buffer.putInt(offset, topic.partitions().size(), ByteOrder.BIG_ENDIAN);
                offset += 4;

                for (FetchResponse.PartitionData partition : topic.partitions()) {
                    buffer.putInt(offset, partition.partitionIndex(), ByteOrder.BIG_ENDIAN);
                    offset += 4;
                    buffer.putShort(offset, partition.errorCode(), ByteOrder.BIG_ENDIAN);
                    offset += 2;
                    buffer.putLong(offset, partition.highWatermark(), ByteOrder.BIG_ENDIAN);
                    offset += 8;

                    if (apiVersion >= 4) {
                        buffer.putLong(offset, partition.lastStableOffset(), ByteOrder.BIG_ENDIAN);
                        offset += 8;
                    }

                    if (apiVersion >= 5) {
                        buffer.putLong(offset, partition.logStartOffset(), ByteOrder.BIG_ENDIAN);
                        offset += 8;
                    }

                    if (apiVersion >= 4) {
                        List<FetchResponse.AbortedTransaction> aborted = partition.abortedTransactions();
                        if (aborted == null) {
                            buffer.putInt(offset, -1, ByteOrder.BIG_ENDIAN);
                            offset += 4;
                        } else {
                            buffer.putInt(offset, aborted.size(), ByteOrder.BIG_ENDIAN);
                            offset += 4;
                            for (FetchResponse.AbortedTransaction tx : aborted) {
                                buffer.putLong(offset, tx.producerId(), ByteOrder.BIG_ENDIAN);
                                offset += 8;
                                buffer.putLong(offset, tx.firstOffset(), ByteOrder.BIG_ENDIAN);
                                offset += 8;
                            }
                        }
                    }

                    if (apiVersion >= 11) {
                        buffer.putInt(offset, partition.preferredReadReplica(), ByteOrder.BIG_ENDIAN);
                        offset += 4;
                    }

                    encodeBytes(partition.records());
                }
            }
        }

        return this;
    }

    private KafkaResponseEncoder encodeListOffsetsResponse(ListOffsetsResponse response, short apiVersion) {
        if (apiVersion >= 6) {
            encodeUnsignedVarint(0); // tagged fields header
        }

        if (apiVersion >= 2) {
            buffer.putInt(offset, response.throttleTimeMs(), ByteOrder.BIG_ENDIAN);
            offset += 4;
        }

        if (apiVersion >= 6) {
            encodeUnsignedVarint(response.topics().size() + 1);
            for (ListOffsetsResponse.ListOffsetsTopicResponse topic : response.topics()) {
                encodeCompactString(topic.name());
                encodeUnsignedVarint(topic.partitions().size() + 1);

                for (ListOffsetsResponse.ListOffsetsPartitionResponse partition : topic.partitions()) {
                    buffer.putInt(offset, partition.partitionIndex(), ByteOrder.BIG_ENDIAN);
                    offset += 4;
                    buffer.putShort(offset, partition.errorCode(), ByteOrder.BIG_ENDIAN);
                    offset += 2;
                    buffer.putLong(offset, partition.timestamp(), ByteOrder.BIG_ENDIAN);
                    offset += 8;
                    buffer.putLong(offset, partition.offset(), ByteOrder.BIG_ENDIAN);
                    offset += 8;

                    if (apiVersion >= 4) {
                        buffer.putInt(offset, partition.leaderEpoch(), ByteOrder.BIG_ENDIAN);
                        offset += 4;
                    }

                    encodeUnsignedVarint(0); // tagged fields
                }
                encodeUnsignedVarint(0); // tagged fields
            }
            encodeUnsignedVarint(0); // tagged fields
        } else {
            buffer.putInt(offset, response.topics().size(), ByteOrder.BIG_ENDIAN);
            offset += 4;

            for (ListOffsetsResponse.ListOffsetsTopicResponse topic : response.topics()) {
                encodeString(topic.name());
                buffer.putInt(offset, topic.partitions().size(), ByteOrder.BIG_ENDIAN);
                offset += 4;

                for (ListOffsetsResponse.ListOffsetsPartitionResponse partition : topic.partitions()) {
                    buffer.putInt(offset, partition.partitionIndex(), ByteOrder.BIG_ENDIAN);
                    offset += 4;
                    buffer.putShort(offset, partition.errorCode(), ByteOrder.BIG_ENDIAN);
                    offset += 2;
                    buffer.putLong(offset, partition.timestamp(), ByteOrder.BIG_ENDIAN);
                    offset += 8;
                    buffer.putLong(offset, partition.offset(), ByteOrder.BIG_ENDIAN);
                    offset += 8;

                    if (apiVersion >= 4) {
                        buffer.putInt(offset, partition.leaderEpoch(), ByteOrder.BIG_ENDIAN);
                        offset += 4;
                    }
                }
            }
        }

        return this;
    }

    private KafkaResponseEncoder encodeFindCoordinatorResponse(FindCoordinatorResponse response, short apiVersion) {
        if (apiVersion >= 4) {
            encodeUnsignedVarint(0); // tagged fields header
        }

        if (apiVersion >= 1) {
            buffer.putInt(offset, response.throttleTimeMs(), ByteOrder.BIG_ENDIAN);
            offset += 4;
        }

        if (apiVersion < 4) {
            buffer.putShort(offset, response.errorCode(), ByteOrder.BIG_ENDIAN);
            offset += 2;

            if (apiVersion >= 1) {
                if (apiVersion >= 3) {
                    encodeCompactNullableString(response.errorMessage());
                } else {
                    encodeNullableString(response.errorMessage());
                }
            }

            buffer.putInt(offset, response.nodeId(), ByteOrder.BIG_ENDIAN);
            offset += 4;
            if (apiVersion >= 3) {
                encodeCompactString(response.host());
            } else {
                encodeString(response.host());
            }
            buffer.putInt(offset, response.port(), ByteOrder.BIG_ENDIAN);
            offset += 4;
        } else {
            // v4+ uses coordinators array
            encodeUnsignedVarint(response.coordinators().size() + 1);
            for (FindCoordinatorResponse.Coordinator coord : response.coordinators()) {
                encodeCompactString(coord.key());
                buffer.putInt(offset, coord.nodeId(), ByteOrder.BIG_ENDIAN);
                offset += 4;
                encodeCompactString(coord.host());
                buffer.putInt(offset, coord.port(), ByteOrder.BIG_ENDIAN);
                offset += 4;
                buffer.putShort(offset, coord.errorCode(), ByteOrder.BIG_ENDIAN);
                offset += 2;
                encodeCompactNullableString(coord.errorMessage());
                encodeUnsignedVarint(0); // tagged fields
            }
        }

        if (apiVersion >= 3) {
            encodeUnsignedVarint(0); // tagged fields
        }

        return this;
    }

    private KafkaResponseEncoder encodeJoinGroupResponse(JoinGroupResponse response, short apiVersion) {
        if (apiVersion >= 6) {
            encodeUnsignedVarint(0); // tagged fields header
        }

        if (apiVersion >= 2) {
            buffer.putInt(offset, response.throttleTimeMs(), ByteOrder.BIG_ENDIAN);
            offset += 4;
        }

        buffer.putShort(offset, response.errorCode(), ByteOrder.BIG_ENDIAN);
        offset += 2;
        buffer.putInt(offset, response.generationId(), ByteOrder.BIG_ENDIAN);
        offset += 4;

        if (apiVersion >= 7) {
            encodeCompactNullableString(response.protocolType());
            encodeCompactNullableString(response.protocolName());
        } else if (apiVersion >= 6) {
            encodeCompactString(response.protocolName() != null ? response.protocolName() : "");
        } else {
            encodeString(response.protocolName() != null ? response.protocolName() : "");
        }

        if (apiVersion >= 6) {
            encodeCompactString(response.leader());
        } else {
            encodeString(response.leader());
        }

        if (apiVersion >= 9) {
            buffer.putByte(offset++, response.skipAssignment() ? (byte) 1 : (byte) 0);
        }

        if (apiVersion >= 6) {
            encodeCompactString(response.memberId());
            encodeUnsignedVarint(response.members().size() + 1);
            for (JoinGroupResponse.JoinGroupMember member : response.members()) {
                encodeCompactString(member.memberId());
                if (apiVersion >= 5) {
                    encodeCompactNullableString(member.groupInstanceId());
                }
                encodeCompactBytes(member.metadata());
                encodeUnsignedVarint(0); // tagged fields
            }
            encodeUnsignedVarint(0); // tagged fields
        } else {
            encodeString(response.memberId());
            buffer.putInt(offset, response.members().size(), ByteOrder.BIG_ENDIAN);
            offset += 4;
            for (JoinGroupResponse.JoinGroupMember member : response.members()) {
                encodeString(member.memberId());
                if (apiVersion >= 5) {
                    encodeNullableString(member.groupInstanceId());
                }
                encodeBytes(member.metadata());
            }
        }

        return this;
    }

    private KafkaResponseEncoder encodeSyncGroupResponse(SyncGroupResponse response, short apiVersion) {
        if (apiVersion >= 4) {
            encodeUnsignedVarint(0); // tagged fields header
        }

        if (apiVersion >= 1) {
            buffer.putInt(offset, response.throttleTimeMs(), ByteOrder.BIG_ENDIAN);
            offset += 4;
        }

        buffer.putShort(offset, response.errorCode(), ByteOrder.BIG_ENDIAN);
        offset += 2;

        if (apiVersion >= 5) {
            encodeCompactNullableString(response.protocolType());
            encodeCompactNullableString(response.protocolName());
        }

        if (apiVersion >= 4) {
            encodeCompactBytes(response.assignment());
            encodeUnsignedVarint(0); // tagged fields
        } else {
            encodeBytes(response.assignment());
        }

        return this;
    }

    private KafkaResponseEncoder encodeHeartbeatResponse(HeartbeatResponse response, short apiVersion) {
        if (apiVersion >= 4) {
            encodeUnsignedVarint(0); // tagged fields header
        }

        if (apiVersion >= 1) {
            buffer.putInt(offset, response.throttleTimeMs(), ByteOrder.BIG_ENDIAN);
            offset += 4;
        }

        buffer.putShort(offset, response.errorCode(), ByteOrder.BIG_ENDIAN);
        offset += 2;

        if (apiVersion >= 4) {
            encodeUnsignedVarint(0); // tagged fields
        }

        return this;
    }

    private KafkaResponseEncoder encodeLeaveGroupResponse(LeaveGroupResponse response, short apiVersion) {
        if (apiVersion >= 4) {
            encodeUnsignedVarint(0); // tagged fields header
        }

        if (apiVersion >= 1) {
            buffer.putInt(offset, response.throttleTimeMs(), ByteOrder.BIG_ENDIAN);
            offset += 4;
        }

        buffer.putShort(offset, response.errorCode(), ByteOrder.BIG_ENDIAN);
        offset += 2;

        if (apiVersion >= 3 && response.members() != null) {
            if (apiVersion >= 4) {
                encodeUnsignedVarint(response.members().size() + 1);
                for (LeaveGroupResponse.MemberResponse member : response.members()) {
                    encodeCompactString(member.memberId());
                    encodeCompactNullableString(member.groupInstanceId());
                    buffer.putShort(offset, member.errorCode(), ByteOrder.BIG_ENDIAN);
                    offset += 2;
                    encodeUnsignedVarint(0); // tagged fields
                }
            } else {
                buffer.putInt(offset, response.members().size(), ByteOrder.BIG_ENDIAN);
                offset += 4;
                for (LeaveGroupResponse.MemberResponse member : response.members()) {
                    encodeString(member.memberId());
                    encodeNullableString(member.groupInstanceId());
                    buffer.putShort(offset, member.errorCode(), ByteOrder.BIG_ENDIAN);
                    offset += 2;
                }
            }
        }

        if (apiVersion >= 4) {
            encodeUnsignedVarint(0); // tagged fields
        }

        return this;
    }

    private KafkaResponseEncoder encodeOffsetCommitResponse(OffsetCommitResponse response, short apiVersion) {
        if (apiVersion >= 8) {
            encodeUnsignedVarint(0); // tagged fields header
        }

        if (apiVersion >= 3) {
            buffer.putInt(offset, response.throttleTimeMs(), ByteOrder.BIG_ENDIAN);
            offset += 4;
        }

        if (apiVersion >= 8) {
            encodeUnsignedVarint(response.topics().size() + 1);
            for (OffsetCommitResponse.OffsetCommitTopicResponse topic : response.topics()) {
                encodeCompactString(topic.name());
                encodeUnsignedVarint(topic.partitions().size() + 1);
                for (OffsetCommitResponse.OffsetCommitPartitionResponse partition : topic.partitions()) {
                    buffer.putInt(offset, partition.partitionIndex(), ByteOrder.BIG_ENDIAN);
                    offset += 4;
                    buffer.putShort(offset, partition.errorCode(), ByteOrder.BIG_ENDIAN);
                    offset += 2;
                    encodeUnsignedVarint(0); // tagged fields
                }
                encodeUnsignedVarint(0); // tagged fields
            }
            encodeUnsignedVarint(0); // tagged fields
        } else {
            buffer.putInt(offset, response.topics().size(), ByteOrder.BIG_ENDIAN);
            offset += 4;
            for (OffsetCommitResponse.OffsetCommitTopicResponse topic : response.topics()) {
                encodeString(topic.name());
                buffer.putInt(offset, topic.partitions().size(), ByteOrder.BIG_ENDIAN);
                offset += 4;
                for (OffsetCommitResponse.OffsetCommitPartitionResponse partition : topic.partitions()) {
                    buffer.putInt(offset, partition.partitionIndex(), ByteOrder.BIG_ENDIAN);
                    offset += 4;
                    buffer.putShort(offset, partition.errorCode(), ByteOrder.BIG_ENDIAN);
                    offset += 2;
                }
            }
        }

        return this;
    }

    private KafkaResponseEncoder encodeOffsetFetchResponse(OffsetFetchResponse response, short apiVersion) {
        if (apiVersion >= 6) {
            encodeUnsignedVarint(0); // tagged fields header
        }

        if (apiVersion >= 3) {
            buffer.putInt(offset, response.throttleTimeMs(), ByteOrder.BIG_ENDIAN);
            offset += 4;
        }

        if (apiVersion < 8) {
            // v0-v7 format
            if (apiVersion >= 6) {
                List<OffsetFetchResponse.OffsetFetchTopicResponse> topics = response.topics();
                encodeUnsignedVarint(topics != null ? topics.size() + 1 : 1);
                if (topics != null) {
                    for (OffsetFetchResponse.OffsetFetchTopicResponse topic : topics) {
                        encodeCompactString(topic.name());
                        encodeUnsignedVarint(topic.partitions().size() + 1);
                        for (OffsetFetchResponse.OffsetFetchPartitionResponse partition : topic.partitions()) {
                            buffer.putInt(offset, partition.partitionIndex(), ByteOrder.BIG_ENDIAN);
                            offset += 4;
                            buffer.putLong(offset, partition.committedOffset(), ByteOrder.BIG_ENDIAN);
                            offset += 8;
                            if (apiVersion >= 5) {
                                buffer.putInt(offset, partition.committedLeaderEpoch(), ByteOrder.BIG_ENDIAN);
                                offset += 4;
                            }
                            encodeCompactNullableString(partition.metadata());
                            buffer.putShort(offset, partition.errorCode(), ByteOrder.BIG_ENDIAN);
                            offset += 2;
                            encodeUnsignedVarint(0); // tagged fields
                        }
                        encodeUnsignedVarint(0); // tagged fields
                    }
                }
            } else {
                List<OffsetFetchResponse.OffsetFetchTopicResponse> topics = response.topics();
                buffer.putInt(offset, topics != null ? topics.size() : 0, ByteOrder.BIG_ENDIAN);
                offset += 4;
                if (topics != null) {
                    for (OffsetFetchResponse.OffsetFetchTopicResponse topic : topics) {
                        encodeString(topic.name());
                        buffer.putInt(offset, topic.partitions().size(), ByteOrder.BIG_ENDIAN);
                        offset += 4;
                        for (OffsetFetchResponse.OffsetFetchPartitionResponse partition : topic.partitions()) {
                            buffer.putInt(offset, partition.partitionIndex(), ByteOrder.BIG_ENDIAN);
                            offset += 4;
                            buffer.putLong(offset, partition.committedOffset(), ByteOrder.BIG_ENDIAN);
                            offset += 8;
                            if (apiVersion >= 5) {
                                buffer.putInt(offset, partition.committedLeaderEpoch(), ByteOrder.BIG_ENDIAN);
                                offset += 4;
                            }
                            encodeNullableString(partition.metadata());
                            buffer.putShort(offset, partition.errorCode(), ByteOrder.BIG_ENDIAN);
                            offset += 2;
                        }
                    }
                }
            }

            if (apiVersion >= 2) {
                buffer.putShort(offset, response.errorCode(), ByteOrder.BIG_ENDIAN);
                offset += 2;
            }
        } else {
            // v8+ format with groups array
            encodeUnsignedVarint(response.groups() != null ? response.groups().size() + 1 : 1);
            if (response.groups() != null) {
                for (OffsetFetchResponse.OffsetFetchGroupResponse group : response.groups()) {
                    encodeCompactString(group.groupId());
                    encodeUnsignedVarint(group.topics() != null ? group.topics().size() + 1 : 1);
                    if (group.topics() != null) {
                        for (OffsetFetchResponse.OffsetFetchTopicResponse topic : group.topics()) {
                            encodeCompactString(topic.name());
                            encodeUnsignedVarint(topic.partitions().size() + 1);
                            for (OffsetFetchResponse.OffsetFetchPartitionResponse partition : topic.partitions()) {
                                buffer.putInt(offset, partition.partitionIndex(), ByteOrder.BIG_ENDIAN);
                                offset += 4;
                                buffer.putLong(offset, partition.committedOffset(), ByteOrder.BIG_ENDIAN);
                                offset += 8;
                                buffer.putInt(offset, partition.committedLeaderEpoch(), ByteOrder.BIG_ENDIAN);
                                offset += 4;
                                encodeCompactNullableString(partition.metadata());
                                buffer.putShort(offset, partition.errorCode(), ByteOrder.BIG_ENDIAN);
                                offset += 2;
                                encodeUnsignedVarint(0); // tagged fields
                            }
                            encodeUnsignedVarint(0); // tagged fields
                        }
                    }
                    buffer.putShort(offset, group.errorCode(), ByteOrder.BIG_ENDIAN);
                    offset += 2;
                    encodeUnsignedVarint(0); // tagged fields
                }
            }
        }

        if (apiVersion >= 6) {
            encodeUnsignedVarint(0); // tagged fields
        }

        return this;
    }

    // Primitive encoders

    private void encodeString(String value) {
        if (value == null) {
            buffer.putShort(offset, (short) -1, ByteOrder.BIG_ENDIAN);
            offset += 2;
        } else {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            buffer.putShort(offset, (short) bytes.length, ByteOrder.BIG_ENDIAN);
            offset += 2;
            buffer.putBytes(offset, bytes);
            offset += bytes.length;
        }
    }

    private void encodeNullableString(String value) {
        if (value == null) {
            buffer.putShort(offset, (short) -1, ByteOrder.BIG_ENDIAN);
            offset += 2;
        } else {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            buffer.putShort(offset, (short) bytes.length, ByteOrder.BIG_ENDIAN);
            offset += 2;
            buffer.putBytes(offset, bytes);
            offset += bytes.length;
        }
    }

    private void encodeCompactString(String value) {
        if (value == null) {
            encodeUnsignedVarint(0);
        } else {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            encodeUnsignedVarint(bytes.length + 1);
            buffer.putBytes(offset, bytes);
            offset += bytes.length;
        }
    }

    private void encodeCompactNullableString(String value) {
        if (value == null) {
            encodeUnsignedVarint(0);
        } else {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            encodeUnsignedVarint(bytes.length + 1);
            buffer.putBytes(offset, bytes);
            offset += bytes.length;
        }
    }

    private void encodeBytes(byte[] value) {
        if (value == null) {
            buffer.putInt(offset, -1, ByteOrder.BIG_ENDIAN);
            offset += 4;
        } else {
            buffer.putInt(offset, value.length, ByteOrder.BIG_ENDIAN);
            offset += 4;
            buffer.putBytes(offset, value);
            offset += value.length;
        }
    }

    private void encodeCompactBytes(byte[] value) {
        if (value == null) {
            encodeUnsignedVarint(0);
        } else {
            encodeUnsignedVarint(value.length + 1);
            buffer.putBytes(offset, value);
            offset += value.length;
        }
    }

    private void encodeUnsignedVarint(int value) {
        while ((value & ~0x7F) != 0) {
            buffer.putByte(offset++, (byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        buffer.putByte(offset++, (byte) value);
    }
}


