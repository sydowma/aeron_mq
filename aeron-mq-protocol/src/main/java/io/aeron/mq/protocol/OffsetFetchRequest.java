package io.aeron.mq.protocol;

import java.util.List;

/**
 * OffsetFetch request to fetch committed offsets.
 *
 * @param apiVersion the API version of this request
 * @param groupId the group ID (v0-v7)
 * @param topics list of topics to fetch offsets for (null = all)
 * @param groups list of groups to fetch offsets for (v8+)
 * @param requireStable whether to require stable offsets
 */
public record OffsetFetchRequest(
        short apiVersion,
        String groupId,
        List<OffsetFetchTopic> topics,
        List<OffsetFetchGroup> groups,
        boolean requireStable
) implements KafkaRequest {

    @Override
    public ApiKeys apiKey() {
        return ApiKeys.OFFSET_FETCH;
    }

    /**
     * Topic to fetch offsets for.
     *
     * @param name topic name
     * @param partitionIndexes list of partition indexes
     */
    public record OffsetFetchTopic(String name, List<Integer> partitionIndexes) {}

    /**
     * Group to fetch offsets for (v8+).
     *
     * @param groupId group ID
     * @param memberId optional member ID
     * @param memberEpoch optional member epoch
     * @param topics list of topics (null = all)
     */
    public record OffsetFetchGroup(
            String groupId,
            String memberId,
            int memberEpoch,
            List<OffsetFetchTopic> topics
    ) {}
}


