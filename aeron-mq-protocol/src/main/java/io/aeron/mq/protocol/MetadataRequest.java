package io.aeron.mq.protocol;

import java.util.List;

/**
 * Metadata request to get cluster and topic information.
 *
 * @param apiVersion the API version of this request
 * @param topics list of topics to get metadata for (null = all topics)
 * @param allowAutoTopicCreation whether to auto-create topics
 */
public record MetadataRequest(
        short apiVersion,
        List<String> topics,
        boolean allowAutoTopicCreation
) implements KafkaRequest {

    @Override
    public ApiKeys apiKey() {
        return ApiKeys.METADATA;
    }
}


