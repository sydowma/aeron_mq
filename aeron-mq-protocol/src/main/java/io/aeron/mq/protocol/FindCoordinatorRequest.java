package io.aeron.mq.protocol;

import java.util.List;

/**
 * FindCoordinator request to locate group coordinator.
 *
 * @param apiVersion the API version of this request
 * @param key the coordinator key (group ID for GROUP, transactional ID for TRANSACTION)
 * @param keyType coordinator type (0 = GROUP, 1 = TRANSACTION)
 * @param coordinatorKeys list of coordinator keys (v4+)
 */
public record FindCoordinatorRequest(
        short apiVersion,
        String key,
        byte keyType,
        List<String> coordinatorKeys
) implements KafkaRequest {

    public static final byte KEY_TYPE_GROUP = 0;
    public static final byte KEY_TYPE_TRANSACTION = 1;

    @Override
    public ApiKeys apiKey() {
        return ApiKeys.FIND_COORDINATOR;
    }
}


