package io.aeron.mq.protocol;

import java.util.List;

/**
 * Metadata response containing cluster and topic information.
 *
 * @param throttleTimeMs throttle time in milliseconds
 * @param brokers list of broker information
 * @param clusterId the cluster ID
 * @param controllerId the controller broker ID
 * @param topics list of topic metadata
 */
public record MetadataResponse(
        int throttleTimeMs,
        List<BrokerMetadata> brokers,
        String clusterId,
        int controllerId,
        List<TopicMetadata> topics
) implements KafkaResponse {

    @Override
    public ApiKeys apiKey() {
        return ApiKeys.METADATA;
    }

    /**
     * Broker metadata.
     *
     * @param nodeId the broker node ID
     * @param host the broker host
     * @param port the broker port
     * @param rack optional rack identifier
     */
    public record BrokerMetadata(int nodeId, String host, int port, String rack) {}

    /**
     * Topic metadata.
     *
     * @param errorCode error code for this topic
     * @param name topic name
     * @param isInternal whether this is an internal topic
     * @param partitions list of partition metadata
     */
    public record TopicMetadata(
            short errorCode,
            String name,
            boolean isInternal,
            List<PartitionMetadata> partitions
    ) {}

    /**
     * Partition metadata.
     *
     * @param errorCode error code for this partition
     * @param partitionIndex the partition index
     * @param leaderId the leader broker ID
     * @param leaderEpoch the leader epoch
     * @param replicaNodes list of replica node IDs
     * @param isrNodes list of in-sync replica node IDs
     */
    public record PartitionMetadata(
            short errorCode,
            int partitionIndex,
            int leaderId,
            int leaderEpoch,
            List<Integer> replicaNodes,
            List<Integer> isrNodes
    ) {}
}


