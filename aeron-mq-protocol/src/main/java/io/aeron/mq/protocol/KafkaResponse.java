package io.aeron.mq.protocol;

/**
 * Base interface for all Kafka responses.
 */
public sealed interface KafkaResponse permits
        ApiVersionsResponse,
        MetadataResponse,
        ProduceResponse,
        FetchResponse,
        ListOffsetsResponse,
        FindCoordinatorResponse,
        JoinGroupResponse,
        SyncGroupResponse,
        HeartbeatResponse,
        LeaveGroupResponse,
        OffsetCommitResponse,
        OffsetFetchResponse {

    /**
     * @return the API key for this response
     */
    ApiKeys apiKey();
}


