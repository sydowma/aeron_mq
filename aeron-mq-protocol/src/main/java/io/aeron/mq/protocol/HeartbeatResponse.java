package io.aeron.mq.protocol;

/**
 * Heartbeat response.
 *
 * @param throttleTimeMs throttle time in milliseconds
 * @param errorCode error code
 */
public record HeartbeatResponse(
        int throttleTimeMs,
        short errorCode
) implements KafkaResponse {

    @Override
    public ApiKeys apiKey() {
        return ApiKeys.HEARTBEAT;
    }
}


