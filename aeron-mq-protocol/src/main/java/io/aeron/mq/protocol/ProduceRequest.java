package io.aeron.mq.protocol;

import java.util.List;

/**
 * Produce request to send records to topics.
 *
 * @param apiVersion the API version of this request
 * @param transactionalId optional transactional ID
 * @param acks required acknowledgments (-1, 0, 1)
 * @param timeoutMs request timeout in milliseconds
 * @param topicData list of topic data to produce
 */
public record ProduceRequest(
        short apiVersion,
        String transactionalId,
        short acks,
        int timeoutMs,
        List<TopicProduceData> topicData
) implements KafkaRequest {

    @Override
    public ApiKeys apiKey() {
        return ApiKeys.PRODUCE;
    }

    /**
     * Topic produce data.
     *
     * @param name topic name
     * @param partitionData list of partition data
     */
    public record TopicProduceData(String name, List<PartitionProduceData> partitionData) {}

    /**
     * Partition produce data.
     *
     * @param index partition index
     * @param records raw record batch bytes
     */
    public record PartitionProduceData(int index, byte[] records) {}
}


