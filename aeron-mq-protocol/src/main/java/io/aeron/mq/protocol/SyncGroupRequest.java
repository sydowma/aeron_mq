package io.aeron.mq.protocol;

import java.util.List;

/**
 * SyncGroup request to synchronize group assignment.
 *
 * @param apiVersion the API version of this request
 * @param groupId the group ID
 * @param generationId the generation ID
 * @param memberId the member ID
 * @param groupInstanceId optional static group instance ID
 * @param protocolType protocol type
 * @param protocolName protocol name
 * @param assignments list of member assignments (only from leader)
 */
public record SyncGroupRequest(
        short apiVersion,
        String groupId,
        int generationId,
        String memberId,
        String groupInstanceId,
        String protocolType,
        String protocolName,
        List<SyncGroupAssignment> assignments
) implements KafkaRequest {

    @Override
    public ApiKeys apiKey() {
        return ApiKeys.SYNC_GROUP;
    }

    /**
     * Member assignment.
     *
     * @param memberId member ID
     * @param assignment assignment bytes
     */
    public record SyncGroupAssignment(String memberId, byte[] assignment) {}
}


