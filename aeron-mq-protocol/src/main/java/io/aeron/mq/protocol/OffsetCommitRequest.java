package io.aeron.mq.protocol;

import java.util.List;

/**
 * OffsetCommit request to commit consumer offsets.
 *
 * @param apiVersion the API version of this request
 * @param groupId the group ID
 * @param generationIdOrMemberEpoch generation ID or member epoch
 * @param memberId the member ID
 * @param groupInstanceId optional static group instance ID
 * @param topics list of topics with offsets to commit
 */
public record OffsetCommitRequest(
        short apiVersion,
        String groupId,
        int generationIdOrMemberEpoch,
        String memberId,
        String groupInstanceId,
        List<OffsetCommitTopic> topics
) implements KafkaRequest {

    @Override
    public ApiKeys apiKey() {
        return ApiKeys.OFFSET_COMMIT;
    }

    /**
     * Topic with offsets to commit.
     *
     * @param name topic name
     * @param partitions list of partitions with offsets
     */
    public record OffsetCommitTopic(String name, List<OffsetCommitPartition> partitions) {}

    /**
     * Partition with offset to commit.
     *
     * @param partitionIndex partition index
     * @param committedOffset offset to commit
     * @param committedLeaderEpoch committed leader epoch
     * @param commitTimestamp commit timestamp (deprecated)
     * @param committedMetadata optional metadata
     */
    public record OffsetCommitPartition(
            int partitionIndex,
            long committedOffset,
            int committedLeaderEpoch,
            long commitTimestamp,
            String committedMetadata
    ) {}
}


