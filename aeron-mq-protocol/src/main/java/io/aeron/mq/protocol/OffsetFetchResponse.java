package io.aeron.mq.protocol;

import java.util.List;

/**
 * OffsetFetch response containing committed offsets.
 *
 * @param throttleTimeMs throttle time in milliseconds
 * @param topics list of topic responses (v0-v7)
 * @param errorCode error code (v2+)
 * @param groups list of group responses (v8+)
 */
public record OffsetFetchResponse(
        int throttleTimeMs,
        List<OffsetFetchTopicResponse> topics,
        short errorCode,
        List<OffsetFetchGroupResponse> groups
) implements KafkaResponse {

    @Override
    public ApiKeys apiKey() {
        return ApiKeys.OFFSET_FETCH;
    }

    /**
     * Topic response.
     *
     * @param name topic name
     * @param partitions list of partition responses
     */
    public record OffsetFetchTopicResponse(String name, List<OffsetFetchPartitionResponse> partitions) {}

    /**
     * Partition response.
     *
     * @param partitionIndex partition index
     * @param committedOffset committed offset (-1 if none)
     * @param committedLeaderEpoch committed leader epoch
     * @param metadata optional metadata
     * @param errorCode error code
     */
    public record OffsetFetchPartitionResponse(
            int partitionIndex,
            long committedOffset,
            int committedLeaderEpoch,
            String metadata,
            short errorCode
    ) {}

    /**
     * Group response (v8+).
     *
     * @param groupId group ID
     * @param topics list of topic responses
     * @param errorCode error code
     */
    public record OffsetFetchGroupResponse(
            String groupId,
            List<OffsetFetchTopicResponse> topics,
            short errorCode
    ) {}
}


