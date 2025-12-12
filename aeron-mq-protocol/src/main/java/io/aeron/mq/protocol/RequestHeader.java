package io.aeron.mq.protocol;

/**
 * Kafka request header containing common request metadata.
 *
 * @param apiKey the API key
 * @param apiVersion the API version
 * @param correlationId the correlation ID for matching request/response
 * @param clientId the client identifier
 */
public record RequestHeader(
        short apiKey,
        short apiVersion,
        int correlationId,
        String clientId
) {
    public static final int MIN_HEADER_SIZE = 8; // apiKey(2) + apiVersion(2) + correlationId(4)
}


