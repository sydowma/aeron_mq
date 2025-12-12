package io.aeron.mq.protocol;

import java.util.List;

/**
 * Fetch response containing consumed records.
 *
 * @param throttleTimeMs throttle time in milliseconds
 * @param errorCode top-level error code
 * @param sessionId fetch session ID
 * @param responses list of topic responses
 */
public record FetchResponse(
        int throttleTimeMs,
        short errorCode,
        int sessionId,
        List<FetchableTopicResponse> responses
) implements KafkaResponse {

    @Override
    public ApiKeys apiKey() {
        return ApiKeys.FETCH;
    }

    /**
     * Fetchable topic response.
     *
     * @param topic topic name
     * @param partitions list of partition responses
     */
    public record FetchableTopicResponse(String topic, List<PartitionData> partitions) {}

    /**
     * Partition data in fetch response.
     *
     * @param partitionIndex partition index
     * @param errorCode error code
     * @param highWatermark high watermark offset
     * @param lastStableOffset last stable offset
     * @param logStartOffset log start offset
     * @param abortedTransactions list of aborted transactions
     * @param preferredReadReplica preferred read replica
     * @param records raw record batch bytes
     */
    public record PartitionData(
            int partitionIndex,
            short errorCode,
            long highWatermark,
            long lastStableOffset,
            long logStartOffset,
            List<AbortedTransaction> abortedTransactions,
            int preferredReadReplica,
            byte[] records
    ) {}

    /**
     * Aborted transaction information.
     *
     * @param producerId producer ID
     * @param firstOffset first offset of aborted transaction
     */
    public record AbortedTransaction(long producerId, long firstOffset) {}
}


