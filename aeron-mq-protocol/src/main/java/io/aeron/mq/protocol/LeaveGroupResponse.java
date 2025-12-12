package io.aeron.mq.protocol;

import java.util.List;

/**
 * LeaveGroup response.
 *
 * @param throttleTimeMs throttle time in milliseconds
 * @param errorCode error code
 * @param members list of member responses (v3+)
 */
public record LeaveGroupResponse(
        int throttleTimeMs,
        short errorCode,
        List<MemberResponse> members
) implements KafkaResponse {

    @Override
    public ApiKeys apiKey() {
        return ApiKeys.LEAVE_GROUP;
    }

    /**
     * Member response.
     *
     * @param memberId member ID
     * @param groupInstanceId optional static group instance ID
     * @param errorCode error code
     */
    public record MemberResponse(String memberId, String groupInstanceId, short errorCode) {}
}


