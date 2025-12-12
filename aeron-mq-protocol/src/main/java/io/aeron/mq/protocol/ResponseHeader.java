package io.aeron.mq.protocol;

/**
 * Kafka response header containing common response metadata.
 *
 * @param correlationId the correlation ID matching the request
 */
public record ResponseHeader(int correlationId) {
    public static final int HEADER_SIZE = 4; // correlationId(4)
}


