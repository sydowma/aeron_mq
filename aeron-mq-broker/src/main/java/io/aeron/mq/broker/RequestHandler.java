package io.aeron.mq.broker;

import io.aeron.mq.protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Handles Kafka protocol requests and generates responses.
 * This class is thread-safe and can be shared across multiple connections.
 */
public final class RequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RequestHandler.class);

    private static final String CLUSTER_ID = "aeron-mq-cluster";

    // Supported API versions
    private static final List<ApiVersionsResponse.ApiVersion> SUPPORTED_VERSIONS = List.of(
            new ApiVersionsResponse.ApiVersion(ApiKeys.API_VERSIONS.id(), (short) 0, (short) 3),
            // Keep max versions aligned with what our protocol codec + broker logic actually handles.
            // This prevents clients from negotiating a version we can't parse/produce correctly.
            new ApiVersionsResponse.ApiVersion(ApiKeys.METADATA.id(), (short) 0, (short) 9),
            new ApiVersionsResponse.ApiVersion(ApiKeys.PRODUCE.id(), (short) 0, (short) 9),
            new ApiVersionsResponse.ApiVersion(ApiKeys.FETCH.id(), (short) 0, (short) 12),
            new ApiVersionsResponse.ApiVersion(ApiKeys.LIST_OFFSETS.id(), (short) 0, (short) 6),
            new ApiVersionsResponse.ApiVersion(ApiKeys.FIND_COORDINATOR.id(), (short) 0, (short) 4),
            new ApiVersionsResponse.ApiVersion(ApiKeys.JOIN_GROUP.id(), (short) 0, (short) 9),
            new ApiVersionsResponse.ApiVersion(ApiKeys.SYNC_GROUP.id(), (short) 0, (short) 5),
            new ApiVersionsResponse.ApiVersion(ApiKeys.HEARTBEAT.id(), (short) 0, (short) 4),
            new ApiVersionsResponse.ApiVersion(ApiKeys.LEAVE_GROUP.id(), (short) 0, (short) 5),
            new ApiVersionsResponse.ApiVersion(ApiKeys.OFFSET_COMMIT.id(), (short) 0, (short) 8),
            new ApiVersionsResponse.ApiVersion(ApiKeys.OFFSET_FETCH.id(), (short) 0, (short) 9)
    );

    private final TopicManager topicManager;
    private final ConsumerGroupCoordinator groupCoordinator;
    private final BrokerConfig config;

    public RequestHandler(TopicManager topicManager, ConsumerGroupCoordinator groupCoordinator,
                          BrokerConfig config) {
        this.topicManager = topicManager;
        this.groupCoordinator = groupCoordinator;
        this.config = config;
    }

    /**
     * Handle a request and return the appropriate response.
     */
    public KafkaResponse handleRequest(KafkaRequest request) {
        return switch (request) {
            case ApiVersionsRequest r -> handleApiVersions(r);
            case MetadataRequest r -> handleMetadata(r);
            case ProduceRequest r -> handleProduce(r);
            case FetchRequest r -> handleFetch(r);
            case ListOffsetsRequest r -> handleListOffsets(r);
            case FindCoordinatorRequest r -> handleFindCoordinator(r);
            case JoinGroupRequest r -> handleJoinGroup(r);
            case SyncGroupRequest r -> handleSyncGroup(r);
            case HeartbeatRequest r -> handleHeartbeat(r);
            case LeaveGroupRequest r -> handleLeaveGroup(r);
            case OffsetCommitRequest r -> handleOffsetCommit(r);
            case OffsetFetchRequest r -> handleOffsetFetch(r);
        };
    }

    private ApiVersionsResponse handleApiVersions(ApiVersionsRequest request) {
        return new ApiVersionsResponse(Errors.NONE.code(), SUPPORTED_VERSIONS, 0);
    }

    private MetadataResponse handleMetadata(MetadataRequest request) {
        List<String> requestedTopics = request.topics();

        // If topics is null or empty, return all topics
        if (requestedTopics == null || requestedTopics.isEmpty()) {
            requestedTopics = new ArrayList<>(topicManager.getTopicNames());
        }

        // Auto-create topics if needed
        if (request.allowAutoTopicCreation()) {
            for (String topic : requestedTopics) {
                topicManager.getOrCreateTopic(topic);
            }
        }

        // Build broker list
        List<MetadataResponse.BrokerMetadata> brokers = List.of(
                new MetadataResponse.BrokerMetadata(
                        config.brokerId(),
                        config.host(),
                        config.kafkaPort(),
                        null // rack
                )
        );

        // Build topic metadata
        List<MetadataResponse.TopicMetadata> topics = new ArrayList<>();
        for (String topicName : requestedTopics) {
            TopicManager.TopicConfig topicConfig = topicManager.getTopic(topicName);
            if (topicConfig == null) {
                topics.add(new MetadataResponse.TopicMetadata(
                        Errors.UNKNOWN_TOPIC_OR_PARTITION.code(),
                        topicName,
                        false,
                        List.of()
                ));
            } else {
                List<MetadataResponse.PartitionMetadata> partitions = new ArrayList<>();
                for (int i = 0; i < topicConfig.partitionCount(); i++) {
                    PartitionLog log = topicManager.getPartitionLog(new TopicPartition(topicName, i));
                    partitions.add(new MetadataResponse.PartitionMetadata(
                            Errors.NONE.code(),
                            i,
                            config.brokerId(), // leader
                            log != null ? log.leaderEpoch() : 0,
                            List.of(config.brokerId()), // replicas
                            List.of(config.brokerId())  // ISR
                    ));
                }
                topics.add(new MetadataResponse.TopicMetadata(
                        Errors.NONE.code(),
                        topicName,
                        topicConfig.isInternal(),
                        partitions
                ));
            }
        }

        return new MetadataResponse(0, brokers, CLUSTER_ID, config.brokerId(), topics);
    }

    private ProduceResponse handleProduce(ProduceRequest request) {
        List<ProduceResponse.TopicProduceResponse> responses = new ArrayList<>();

        for (ProduceRequest.TopicProduceData topicData : request.topicData()) {
            List<ProduceResponse.PartitionProduceResponse> partitionResponses = new ArrayList<>();

            for (ProduceRequest.PartitionProduceData partitionData : topicData.partitionData()) {
                PartitionLog log = topicManager.getOrCreatePartitionLog(
                        topicData.name(), partitionData.index());

                if (log == null) {
                    partitionResponses.add(new ProduceResponse.PartitionProduceResponse(
                            partitionData.index(),
                            Errors.UNKNOWN_TOPIC_OR_PARTITION.code(),
                            -1L, -1L, -1L
                    ));
                } else {
                    final byte[] records = partitionData.records();
                    if (records != null && records.length > 0 && records.length > log.maxPayloadLength()) {
                        LOG.warn("Produce rejected due to oversized record batch: topic={}, partition={}, size={}, maxPayloadLength={}",
                                topicData.name(), partitionData.index(), records.length, log.maxPayloadLength());
                        partitionResponses.add(new ProduceResponse.PartitionProduceResponse(
                                partitionData.index(),
                                Errors.MESSAGE_TOO_LARGE.code(),
                                -1L, -1L, -1L
                        ));
                        continue;
                    }

                    long baseOffset = log.append(partitionData.records());
                    if (baseOffset < 0) {
                        partitionResponses.add(new ProduceResponse.PartitionProduceResponse(
                                partitionData.index(),
                                Errors.REQUEST_TIMED_OUT.code(),
                                -1L, -1L, -1L
                        ));
                    } else {
                        partitionResponses.add(new ProduceResponse.PartitionProduceResponse(
                                partitionData.index(),
                                Errors.NONE.code(),
                                baseOffset,
                                System.currentTimeMillis(),
                                log.logStartOffset()
                        ));
                    }
                }
            }

            responses.add(new ProduceResponse.TopicProduceResponse(topicData.name(), partitionResponses));
        }

        return new ProduceResponse(responses, 0);
    }

    private FetchResponse handleFetch(FetchRequest request) {
        List<FetchResponse.FetchableTopicResponse> responses = new ArrayList<>();

        for (FetchRequest.FetchTopic topic : request.topics()) {
            List<FetchResponse.PartitionData> partitions = new ArrayList<>();

            for (FetchRequest.FetchPartition partition : topic.partitions()) {
                PartitionLog log = topicManager.getPartitionLog(
                        new TopicPartition(topic.name(), partition.partition()));

                if (log == null) {
                    partitions.add(new FetchResponse.PartitionData(
                            partition.partition(),
                            Errors.UNKNOWN_TOPIC_OR_PARTITION.code(),
                            -1L, -1L, -1L,
                            null, -1, null
                    ));
                } else {
                    PartitionLog.ReadResult read = log.read(partition.fetchOffset(), partition.partitionMaxBytes());
                    byte[] records = read.records();
                    partitions.add(new FetchResponse.PartitionData(
                            partition.partition(),
                            read.errorCode(),
                            log.highWatermark(),
                            log.highWatermark(), // lastStableOffset
                            log.logStartOffset(),
                            null, // abortedTransactions
                            -1, // preferredReadReplica
                            records != null ? records : new byte[0]
                    ));
                }
            }

            responses.add(new FetchResponse.FetchableTopicResponse(topic.name(), partitions));
        }

        return new FetchResponse(0, Errors.NONE.code(), 0, responses);
    }

    private ListOffsetsResponse handleListOffsets(ListOffsetsRequest request) {
        List<ListOffsetsResponse.ListOffsetsTopicResponse> topics = new ArrayList<>();

        for (ListOffsetsRequest.ListOffsetsTopic topic : request.topics()) {
            List<ListOffsetsResponse.ListOffsetsPartitionResponse> partitions = new ArrayList<>();

            for (ListOffsetsRequest.ListOffsetsPartition partition : topic.partitions()) {
                PartitionLog log = topicManager.getPartitionLog(
                        new TopicPartition(topic.name(), partition.partitionIndex()));

                if (log == null) {
                    partitions.add(new ListOffsetsResponse.ListOffsetsPartitionResponse(
                            partition.partitionIndex(),
                            Errors.UNKNOWN_TOPIC_OR_PARTITION.code(),
                            -1L, -1L, -1
                    ));
                } else {
                    long offset;
                    if (partition.timestamp() == ListOffsetsRequest.EARLIEST_TIMESTAMP) {
                        offset = log.logStartOffset();
                    } else if (partition.timestamp() == ListOffsetsRequest.LATEST_TIMESTAMP) {
                        offset = log.highWatermark();
                    } else {
                        // Find by timestamp - simplified to latest
                        offset = log.highWatermark();
                    }

                    partitions.add(new ListOffsetsResponse.ListOffsetsPartitionResponse(
                            partition.partitionIndex(),
                            Errors.NONE.code(),
                            System.currentTimeMillis(),
                            offset,
                            log.leaderEpoch()
                    ));
                }
            }

            topics.add(new ListOffsetsResponse.ListOffsetsTopicResponse(topic.name(), partitions));
        }

        return new ListOffsetsResponse(0, topics);
    }

    private FindCoordinatorResponse handleFindCoordinator(FindCoordinatorRequest request) {
        // This broker is the coordinator for all groups (single node mode)
        if (request.apiVersion() >= 4 && request.coordinatorKeys() != null) {
            List<FindCoordinatorResponse.Coordinator> coordinators = new ArrayList<>();
            for (String key : request.coordinatorKeys()) {
                coordinators.add(new FindCoordinatorResponse.Coordinator(
                        key,
                        config.brokerId(),
                        config.host(),
                        config.kafkaPort(),
                        Errors.NONE.code(),
                        null
                ));
            }
            return new FindCoordinatorResponse(0, Errors.NONE.code(), null,
                    config.brokerId(), config.host(), config.kafkaPort(), coordinators);
        }

        return new FindCoordinatorResponse(
                0,
                Errors.NONE.code(),
                null,
                config.brokerId(),
                config.host(),
                config.kafkaPort(),
                null
        );
    }

    private JoinGroupResponse handleJoinGroup(JoinGroupRequest request) {
        return groupCoordinator.handleJoinGroup(request);
    }

    private SyncGroupResponse handleSyncGroup(SyncGroupRequest request) {
        return groupCoordinator.handleSyncGroup(request);
    }

    private HeartbeatResponse handleHeartbeat(HeartbeatRequest request) {
        return groupCoordinator.handleHeartbeat(request);
    }

    private LeaveGroupResponse handleLeaveGroup(LeaveGroupRequest request) {
        return groupCoordinator.handleLeaveGroup(request);
    }

    private OffsetCommitResponse handleOffsetCommit(OffsetCommitRequest request) {
        return groupCoordinator.handleOffsetCommit(request);
    }

    private OffsetFetchResponse handleOffsetFetch(OffsetFetchRequest request) {
        return groupCoordinator.handleOffsetFetch(request);
    }
}


