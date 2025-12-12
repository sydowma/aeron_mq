package io.aeron.mq.protocol;

import java.util.List;

/**
 * JoinGroup request to join a consumer group.
 *
 * @param apiVersion the API version of this request
 * @param groupId the group ID
 * @param sessionTimeoutMs session timeout in milliseconds
 * @param rebalanceTimeoutMs rebalance timeout in milliseconds
 * @param memberId the member ID (empty string for new members)
 * @param groupInstanceId optional static group instance ID
 * @param protocolType protocol type (e.g., "consumer")
 * @param protocols list of supported protocols
 * @param reason optional join reason
 */
public record JoinGroupRequest(
        short apiVersion,
        String groupId,
        int sessionTimeoutMs,
        int rebalanceTimeoutMs,
        String memberId,
        String groupInstanceId,
        String protocolType,
        List<JoinGroupProtocol> protocols,
        String reason
) implements KafkaRequest {

    @Override
    public ApiKeys apiKey() {
        return ApiKeys.JOIN_GROUP;
    }

    /**
     * Protocol information.
     *
     * @param name protocol name
     * @param metadata protocol metadata bytes
     */
    public record JoinGroupProtocol(String name, byte[] metadata) {}
}


