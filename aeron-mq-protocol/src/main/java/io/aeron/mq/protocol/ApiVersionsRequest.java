package io.aeron.mq.protocol;

/**
 * ApiVersions request to discover supported API versions.
 *
 * @param apiVersion the API version of this request
 * @param clientSoftwareName optional client software name (v3+)
 * @param clientSoftwareVersion optional client software version (v3+)
 */
public record ApiVersionsRequest(
        short apiVersion,
        String clientSoftwareName,
        String clientSoftwareVersion
) implements KafkaRequest {

    @Override
    public ApiKeys apiKey() {
        return ApiKeys.API_VERSIONS;
    }
}


