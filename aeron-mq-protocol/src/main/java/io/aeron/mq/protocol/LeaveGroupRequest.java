package io.aeron.mq.protocol;

import java.util.List;

/**
 * LeaveGroup request to leave a consumer group.
 *
 * @param apiVersion the API version of this request
 * @param groupId the group ID
 * @param memberId the member ID (v0-v2)
 * @param members list of members to remove (v3+)
 */
public record LeaveGroupRequest(
        short apiVersion,
        String groupId,
        String memberId,
        List<MemberIdentity> members
) implements KafkaRequest {

    @Override
    public ApiKeys apiKey() {
        return ApiKeys.LEAVE_GROUP;
    }

    /**
     * Member identity.
     *
     * @param memberId member ID
     * @param groupInstanceId optional static group instance ID
     * @param reason optional leave reason
     */
    public record MemberIdentity(String memberId, String groupInstanceId, String reason) {}
}


