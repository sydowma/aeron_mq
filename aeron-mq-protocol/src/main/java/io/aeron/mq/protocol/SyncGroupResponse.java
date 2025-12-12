package io.aeron.mq.protocol;

/**
 * SyncGroup response containing member assignment.
 *
 * @param throttleTimeMs throttle time in milliseconds
 * @param errorCode error code
 * @param protocolType protocol type
 * @param protocolName protocol name
 * @param assignment assignment bytes
 */
public record SyncGroupResponse(
        int throttleTimeMs,
        short errorCode,
        String protocolType,
        String protocolName,
        byte[] assignment
) implements KafkaResponse {

    @Override
    public ApiKeys apiKey() {
        return ApiKeys.SYNC_GROUP;
    }
}


