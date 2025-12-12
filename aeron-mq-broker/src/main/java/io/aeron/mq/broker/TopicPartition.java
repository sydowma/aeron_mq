package io.aeron.mq.broker;

/**
 * Represents a topic-partition pair.
 *
 * @param topic the topic name
 * @param partition the partition index
 */
public record TopicPartition(String topic, int partition) implements Comparable<TopicPartition> {

    @Override
    public int compareTo(TopicPartition other) {
        int cmp = topic.compareTo(other.topic);
        if (cmp != 0) {
            return cmp;
        }
        return Integer.compare(partition, other.partition);
    }

    @Override
    public String toString() {
        return topic + "-" + partition;
    }
}


