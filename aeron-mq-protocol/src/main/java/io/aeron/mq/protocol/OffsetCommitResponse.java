package io.aeron.mq.protocol;

import java.util.List;

/**
 * OffsetCommit response.
 *
 * @param throttleTimeMs throttle time in milliseconds
 * @param topics list of topic responses
 */
public record OffsetCommitResponse(
        int throttleTimeMs,
        List<OffsetCommitTopicResponse> topics
) implements KafkaResponse {

    @Override
    public ApiKeys apiKey() {
        return ApiKeys.OFFSET_COMMIT;
    }

    /**
     * Topic response.
     *
     * @param name topic name
     * @param partitions list of partition responses
     */
    public record OffsetCommitTopicResponse(String name, List<OffsetCommitPartitionResponse> partitions) {}

    /**
     * Partition response.
     *
     * @param partitionIndex partition index
     * @param errorCode error code
     */
    public record OffsetCommitPartitionResponse(int partitionIndex, short errorCode) {}
}


