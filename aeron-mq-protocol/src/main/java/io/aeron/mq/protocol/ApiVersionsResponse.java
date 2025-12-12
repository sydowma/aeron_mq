package io.aeron.mq.protocol;

import java.util.List;

/**
 * ApiVersions response containing supported API versions.
 *
 * @param errorCode error code (0 = success)
 * @param apiVersions list of supported API versions
 * @param throttleTimeMs throttle time in milliseconds
 */
public record ApiVersionsResponse(
        short errorCode,
        List<ApiVersion> apiVersions,
        int throttleTimeMs
) implements KafkaResponse {

    @Override
    public ApiKeys apiKey() {
        return ApiKeys.API_VERSIONS;
    }

    /**
     * Represents a single API version range.
     *
     * @param apiKey the API key
     * @param minVersion minimum supported version
     * @param maxVersion maximum supported version
     */
    public record ApiVersion(short apiKey, short minVersion, short maxVersion) {}
}


