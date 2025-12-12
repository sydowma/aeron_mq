package io.aeron.mq.protocol;

/**
 * Heartbeat request to keep group membership alive.
 *
 * @param apiVersion the API version of this request
 * @param groupId the group ID
 * @param generationId the generation ID
 * @param memberId the member ID
 * @param groupInstanceId optional static group instance ID
 */
public record HeartbeatRequest(
        short apiVersion,
        String groupId,
        int generationId,
        String memberId,
        String groupInstanceId
) implements KafkaRequest {

    @Override
    public ApiKeys apiKey() {
        return ApiKeys.HEARTBEAT;
    }
}


