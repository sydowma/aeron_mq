package io.aeron.mq.broker;

import java.nio.file.Path;

/**
 * Configuration for the Aeron MQ Broker.
 */
public record BrokerConfig(
        int brokerId,
        String host,
        int kafkaPort,
        Path dataDir,
        Path aeronDir,
        boolean autoCreateTopics,
        int defaultPartitionCount,
        int defaultReplicationFactor,
        ClusterConfig clusterConfig
) {
    public static final int DEFAULT_KAFKA_PORT = 9092;
    public static final int DEFAULT_PARTITION_COUNT = 1;
    public static final int DEFAULT_REPLICATION_FACTOR = 1;

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private int brokerId = 0;
        private String host = "localhost";
        private int kafkaPort = DEFAULT_KAFKA_PORT;
        private Path dataDir = Path.of("data");
        private Path aeronDir = Path.of("aeron");
        private boolean autoCreateTopics = true;
        private int defaultPartitionCount = DEFAULT_PARTITION_COUNT;
        private int defaultReplicationFactor = DEFAULT_REPLICATION_FACTOR;
        private ClusterConfig clusterConfig = null;

        public Builder brokerId(int brokerId) {
            this.brokerId = brokerId;
            return this;
        }

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder kafkaPort(int kafkaPort) {
            this.kafkaPort = kafkaPort;
            return this;
        }

        public Builder dataDir(Path dataDir) {
            this.dataDir = dataDir;
            return this;
        }

        public Builder aeronDir(Path aeronDir) {
            this.aeronDir = aeronDir;
            return this;
        }

        public Builder autoCreateTopics(boolean autoCreateTopics) {
            this.autoCreateTopics = autoCreateTopics;
            return this;
        }

        public Builder defaultPartitionCount(int defaultPartitionCount) {
            this.defaultPartitionCount = defaultPartitionCount;
            return this;
        }

        public Builder defaultReplicationFactor(int defaultReplicationFactor) {
            this.defaultReplicationFactor = defaultReplicationFactor;
            return this;
        }

        public Builder clusterConfig(ClusterConfig clusterConfig) {
            this.clusterConfig = clusterConfig;
            return this;
        }

        public BrokerConfig build() {
            return new BrokerConfig(
                    brokerId, host, kafkaPort, dataDir, aeronDir,
                    autoCreateTopics, defaultPartitionCount, defaultReplicationFactor,
                    clusterConfig
            );
        }
    }

    /**
     * Cluster configuration.
     */
    public record ClusterConfig(
            String clusterMembers,
            int consensusModulePort,
            int clusterPort,
            int archivePort
    ) {
        public static ClusterConfig parse(String members) {
            // Format: nodeId,host,port|nodeId,host,port|...
            return new ClusterConfig(members, 20000, 20001, 20002);
        }
    }
}


