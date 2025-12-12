package io.aeron.mq.protocol;

import java.util.List;

/**
 * JoinGroup response containing group membership information.
 *
 * @param throttleTimeMs throttle time in milliseconds
 * @param errorCode error code
 * @param generationId the generation ID
 * @param protocolType the selected protocol type
 * @param protocolName the selected protocol name
 * @param leader the leader member ID
 * @param skipAssignment whether to skip assignment
 * @param memberId the assigned member ID
 * @param members list of group members (only for leader)
 */
public record JoinGroupResponse(
        int throttleTimeMs,
        short errorCode,
        int generationId,
        String protocolType,
        String protocolName,
        String leader,
        boolean skipAssignment,
        String memberId,
        List<JoinGroupMember> members
) implements KafkaResponse {

    @Override
    public ApiKeys apiKey() {
        return ApiKeys.JOIN_GROUP;
    }

    /**
     * Group member information.
     *
     * @param memberId member ID
     * @param groupInstanceId optional static group instance ID
     * @param metadata protocol metadata bytes
     */
    public record JoinGroupMember(String memberId, String groupInstanceId, byte[] metadata) {}
}


