package io.aeron.mq.broker;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages topics and their partitions.
 * Thread-safe for concurrent access from multiple request handlers.
 */
public final class TopicManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TopicManager.class);

    private final AeronArchive archive;
    private final Aeron aeron;
    private final int brokerId;
    private final boolean autoCreateTopics;
    private final int defaultPartitionCount;
    private final int defaultReplicationFactor;

    // Serialize topic creation per topic name to avoid publishing topic metadata before logs exist.
    private final ConcurrentHashMap<String, Object> topicCreationLocks = new ConcurrentHashMap<>();

    // Topic name -> TopicConfig
    private final ConcurrentHashMap<String, TopicConfig> topics = new ConcurrentHashMap<>();

    // TopicPartition -> PartitionLog
    private final ConcurrentHashMap<TopicPartition, PartitionLog> partitionLogs = new ConcurrentHashMap<>();

    public TopicManager(
            AeronArchive archive,
            Aeron aeron,
            int brokerId,
            boolean autoCreateTopics,
            int defaultPartitionCount,
            int defaultReplicationFactor) {
        this.archive = archive;
        this.aeron = aeron;
        this.brokerId = brokerId;
        this.autoCreateTopics = autoCreateTopics;
        this.defaultPartitionCount = Math.max(1, defaultPartitionCount);
        this.defaultReplicationFactor = Math.max(1, defaultReplicationFactor);
    }

    /**
     * Create a new topic with specified configuration.
     *
     * @param topicName the topic name
     * @param partitionCount number of partitions
     * @param replicationFactor replication factor
     * @return true if created, false if already exists
     */
    public boolean createTopic(String topicName, int partitionCount, int replicationFactor) {
        if (topicName == null || topicName.isBlank()) {
            throw new IllegalArgumentException("topicName must be non-empty");
        }
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be > 0");
        }
        if (replicationFactor <= 0) {
            throw new IllegalArgumentException("replicationFactor must be > 0");
        }

        final Object lock = topicCreationLocks.computeIfAbsent(topicName, k -> new Object());
        synchronized (lock) {
            if (topics.containsKey(topicName)) {
                return false; // Already exists
            }

            TopicConfig config = new TopicConfig(topicName, partitionCount, replicationFactor, false);

            // Initialize partition logs first, then publish topic config.
            final List<TopicPartition> created = new ArrayList<>(partitionCount);
            try {
                for (int i = 0; i < partitionCount; i++) {
                    TopicPartition tp = new TopicPartition(topicName, i);
                    PartitionLog log = new PartitionLog(tp, archive, aeron);
                    log.initialize();
                    partitionLogs.put(tp, log);
                    created.add(tp);
                }
            } catch (Exception e) {
                // Rollback partially created logs.
                for (TopicPartition tp : created) {
                    PartitionLog log = partitionLogs.remove(tp);
                    if (log != null) {
                        log.close();
                    }
                }
                throw e;
            }

            topics.put(topicName, config);
            LOG.info("Created topic {} with {} partitions", topicName, partitionCount);
            return true;
        }
    }

    /**
     * Get or create a topic (if auto-creation is enabled).
     *
     * @param topicName the topic name
     * @return the topic config, or null if not found and auto-creation is disabled
     */
    public TopicConfig getOrCreateTopic(String topicName) {
        TopicConfig config = topics.get(topicName);
        if (config == null && autoCreateTopics) {
            createTopic(topicName, defaultPartitionCount, defaultReplicationFactor);
            config = topics.get(topicName);
        }
        return config;
    }

    /**
     * Get a topic configuration.
     *
     * @param topicName the topic name
     * @return the topic config, or null if not found
     */
    public TopicConfig getTopic(String topicName) {
        return topics.get(topicName);
    }

    /**
     * Get all topic names.
     */
    public Set<String> getTopicNames() {
        return Collections.unmodifiableSet(topics.keySet());
    }

    /**
     * Get all topic configurations.
     */
    public Collection<TopicConfig> getAllTopics() {
        return Collections.unmodifiableCollection(topics.values());
    }

    /**
     * Get the partition log for a topic-partition.
     *
     * @param tp the topic-partition
     * @return the partition log, or null if not found
     */
    public PartitionLog getPartitionLog(TopicPartition tp) {
        return partitionLogs.get(tp);
    }

    /**
     * Get the partition log, creating the topic if auto-creation is enabled.
     *
     * @param topicName the topic name
     * @param partition the partition index
     * @return the partition log, or null if not found
     */
    public PartitionLog getOrCreatePartitionLog(String topicName, int partition) {
        TopicPartition tp = new TopicPartition(topicName, partition);
        PartitionLog log = partitionLogs.get(tp);

        if (log == null && autoCreateTopics) {
            // Auto-create topic
            TopicConfig config = getOrCreateTopic(topicName);
            if (config != null && partition < config.partitionCount()) {
                log = partitionLogs.get(tp);
            }
        }

        return log;
    }

    /**
     * Get all partition logs for a topic.
     *
     * @param topicName the topic name
     * @return list of partition logs, or empty if topic not found
     */
    public List<PartitionLog> getPartitionLogs(String topicName) {
        TopicConfig config = topics.get(topicName);
        if (config == null) {
            return Collections.emptyList();
        }

        List<PartitionLog> logs = new ArrayList<>(config.partitionCount());
        for (int i = 0; i < config.partitionCount(); i++) {
            PartitionLog log = partitionLogs.get(new TopicPartition(topicName, i));
            if (log != null) {
                logs.add(log);
            }
        }
        return logs;
    }

    /**
     * Delete a topic and all its partitions.
     *
     * @param topicName the topic name
     * @return true if deleted, false if not found
     */
    public boolean deleteTopic(String topicName) {
        TopicConfig config = topics.remove(topicName);
        if (config == null) {
            return false;
        }

        for (int i = 0; i < config.partitionCount(); i++) {
            TopicPartition tp = new TopicPartition(topicName, i);
            PartitionLog log = partitionLogs.remove(tp);
            if (log != null) {
                log.close();
            }
        }

        LOG.info("Deleted topic {}", topicName);
        return true;
    }

    /**
     * Get metadata for all topics.
     */
    public List<TopicMetadata> getTopicsMetadata() {
        return getTopicsMetadata(null);
    }

    /**
     * Get metadata for specified topics.
     *
     * @param topicNames list of topic names, or null for all topics
     */
    public List<TopicMetadata> getTopicsMetadata(List<String> topicNames) {
        Collection<String> names = topicNames != null ? topicNames : topics.keySet();
        List<TopicMetadata> result = new ArrayList<>(names.size());

        for (String topicName : names) {
            TopicConfig config = topics.get(topicName);
            if (config != null) {
                List<PartitionMetadata> partitions = new ArrayList<>(config.partitionCount());
                for (int i = 0; i < config.partitionCount(); i++) {
                    PartitionLog log = partitionLogs.get(new TopicPartition(topicName, i));
                    if (log != null) {
                        partitions.add(new PartitionMetadata(
                                i,
                                brokerId, // leader
                                log.leaderEpoch(),
                                List.of(brokerId), // replicas
                                List.of(brokerId)  // ISR
                        ));
                    }
                }
                result.add(new TopicMetadata(topicName, config.isInternal(), partitions));
            }
        }

        return result;
    }

    public int getBrokerId() {
        return brokerId;
    }

    @Override
    public void close() {
        for (PartitionLog log : partitionLogs.values()) {
            log.close();
        }
        partitionLogs.clear();
        topics.clear();
    }

    /**
     * Topic configuration.
     */
    public record TopicConfig(
            String name,
            int partitionCount,
            int replicationFactor,
            boolean isInternal
    ) {}

    /**
     * Topic metadata for Metadata API response.
     */
    public record TopicMetadata(
            String name,
            boolean isInternal,
            List<PartitionMetadata> partitions
    ) {}

    /**
     * Partition metadata for Metadata API response.
     */
    public record PartitionMetadata(
            int partitionIndex,
            int leaderId,
            int leaderEpoch,
            List<Integer> replicas,
            List<Integer> isr
    ) {}
}


