package io.aeron.mq.protocol;

import java.util.List;

/**
 * Produce response containing acknowledgment for produced records.
 *
 * @param responses list of topic responses
 * @param throttleTimeMs throttle time in milliseconds
 */
public record ProduceResponse(
        List<TopicProduceResponse> responses,
        int throttleTimeMs
) implements KafkaResponse {

    @Override
    public ApiKeys apiKey() {
        return ApiKeys.PRODUCE;
    }

    /**
     * Topic produce response.
     *
     * @param name topic name
     * @param partitionResponses list of partition responses
     */
    public record TopicProduceResponse(String name, List<PartitionProduceResponse> partitionResponses) {}

    /**
     * Partition produce response.
     *
     * @param index partition index
     * @param errorCode error code
     * @param baseOffset the base offset of appended records
     * @param logAppendTimeMs log append time in milliseconds (-1 if not used)
     * @param logStartOffset the log start offset
     */
    public record PartitionProduceResponse(
            int index,
            short errorCode,
            long baseOffset,
            long logAppendTimeMs,
            long logStartOffset
    ) {}
}


