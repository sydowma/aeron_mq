package io.aeron.mq.protocol;

import java.util.List;

/**
 * ListOffsets request to get offset positions.
 *
 * @param apiVersion the API version of this request
 * @param replicaId replica ID (-1 for clients)
 * @param isolationLevel isolation level
 * @param topics list of topics
 */
public record ListOffsetsRequest(
        short apiVersion,
        int replicaId,
        byte isolationLevel,
        List<ListOffsetsTopic> topics
) implements KafkaRequest {

    public static final long EARLIEST_TIMESTAMP = -2L;
    public static final long LATEST_TIMESTAMP = -1L;

    @Override
    public ApiKeys apiKey() {
        return ApiKeys.LIST_OFFSETS;
    }

    /**
     * List offsets topic data.
     *
     * @param name topic name
     * @param partitions list of partitions
     */
    public record ListOffsetsTopic(String name, List<ListOffsetsPartition> partitions) {}

    /**
     * List offsets partition data.
     *
     * @param partitionIndex partition index
     * @param currentLeaderEpoch current leader epoch
     * @param timestamp timestamp to query (-1 = latest, -2 = earliest)
     */
    public record ListOffsetsPartition(int partitionIndex, int currentLeaderEpoch, long timestamp) {}
}


