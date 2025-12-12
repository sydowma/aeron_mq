package io.aeron.mq.protocol;

import java.util.List;

/**
 * Fetch request to consume records from topics.
 *
 * @param apiVersion the API version of this request
 * @param replicaId the broker ID of the follower (-1 for consumers)
 * @param maxWaitMs maximum wait time in milliseconds
 * @param minBytes minimum bytes to return
 * @param maxBytes maximum bytes to return
 * @param isolationLevel isolation level (0 = read_uncommitted, 1 = read_committed)
 * @param sessionId fetch session ID
 * @param sessionEpoch fetch session epoch
 * @param topics list of topics to fetch
 */
public record FetchRequest(
        short apiVersion,
        int replicaId,
        int maxWaitMs,
        int minBytes,
        int maxBytes,
        byte isolationLevel,
        int sessionId,
        int sessionEpoch,
        List<FetchTopic> topics
) implements KafkaRequest {

    @Override
    public ApiKeys apiKey() {
        return ApiKeys.FETCH;
    }

    /**
     * Fetch topic data.
     *
     * @param name topic name
     * @param partitions list of partitions to fetch
     */
    public record FetchTopic(String name, List<FetchPartition> partitions) {}

    /**
     * Fetch partition data.
     *
     * @param partition partition index
     * @param currentLeaderEpoch current leader epoch
     * @param fetchOffset offset to start fetching from
     * @param lastFetchedEpoch last fetched epoch
     * @param logStartOffset log start offset
     * @param partitionMaxBytes maximum bytes to return for this partition
     */
    public record FetchPartition(
            int partition,
            int currentLeaderEpoch,
            long fetchOffset,
            int lastFetchedEpoch,
            long logStartOffset,
            int partitionMaxBytes
    ) {}
}


