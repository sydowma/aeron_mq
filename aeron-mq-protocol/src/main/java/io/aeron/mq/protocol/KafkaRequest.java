package io.aeron.mq.protocol;

/**
 * Base interface for all Kafka requests.
 */
public sealed interface KafkaRequest permits
        ApiVersionsRequest,
        MetadataRequest,
        ProduceRequest,
        FetchRequest,
        ListOffsetsRequest,
        FindCoordinatorRequest,
        JoinGroupRequest,
        SyncGroupRequest,
        HeartbeatRequest,
        LeaveGroupRequest,
        OffsetCommitRequest,
        OffsetFetchRequest {

    /**
     * @return the API key for this request
     */
    ApiKeys apiKey();

    /**
     * @return the API version used for this request
     */
    short apiVersion();
}


