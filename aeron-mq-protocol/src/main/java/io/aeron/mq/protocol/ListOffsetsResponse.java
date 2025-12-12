package io.aeron.mq.protocol;

import java.util.List;

/**
 * ListOffsets response containing offset positions.
 *
 * @param throttleTimeMs throttle time in milliseconds
 * @param topics list of topic responses
 */
public record ListOffsetsResponse(
        int throttleTimeMs,
        List<ListOffsetsTopicResponse> topics
) implements KafkaResponse {

    @Override
    public ApiKeys apiKey() {
        return ApiKeys.LIST_OFFSETS;
    }

    /**
     * List offsets topic response.
     *
     * @param name topic name
     * @param partitions list of partition responses
     */
    public record ListOffsetsTopicResponse(String name, List<ListOffsetsPartitionResponse> partitions) {}

    /**
     * List offsets partition response.
     *
     * @param partitionIndex partition index
     * @param errorCode error code
     * @param timestamp timestamp of the offset
     * @param offset the offset
     * @param leaderEpoch leader epoch
     */
    public record ListOffsetsPartitionResponse(
            int partitionIndex,
            short errorCode,
            long timestamp,
            long offset,
            int leaderEpoch
    ) {}
}


