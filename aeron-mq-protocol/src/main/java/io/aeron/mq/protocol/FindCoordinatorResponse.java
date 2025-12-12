package io.aeron.mq.protocol;

import java.util.List;

/**
 * FindCoordinator response containing coordinator information.
 *
 * @param throttleTimeMs throttle time in milliseconds
 * @param errorCode error code
 * @param errorMessage error message
 * @param nodeId coordinator node ID
 * @param host coordinator host
 * @param port coordinator port
 * @param coordinators list of coordinator responses (v4+)
 */
public record FindCoordinatorResponse(
        int throttleTimeMs,
        short errorCode,
        String errorMessage,
        int nodeId,
        String host,
        int port,
        List<Coordinator> coordinators
) implements KafkaResponse {

    @Override
    public ApiKeys apiKey() {
        return ApiKeys.FIND_COORDINATOR;
    }

    /**
     * Coordinator information.
     *
     * @param key the coordinator key
     * @param nodeId node ID
     * @param host host
     * @param port port
     * @param errorCode error code
     * @param errorMessage error message
     */
    public record Coordinator(
            String key,
            int nodeId,
            String host,
            int port,
            short errorCode,
            String errorMessage
    ) {}
}


