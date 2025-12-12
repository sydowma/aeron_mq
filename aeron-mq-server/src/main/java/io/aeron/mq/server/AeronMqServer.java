package io.aeron.mq.server;

import io.aeron.Aeron;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.mq.broker.*;
import io.aeron.mq.gateway.TcpGateway;
import org.agrona.CloseHelper;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;

/**
 * Main entry point for Aeron MQ Server.
 * Starts the embedded Aeron Media Driver, Archive, and TCP Gateway.
 */
public final class AeronMqServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(AeronMqServer.class);

    private final BrokerConfig config;

    private MediaDriver mediaDriver;
    private Archive archive;
    private Aeron aeron;
    private AeronArchive aeronArchive;
    private TopicManager topicManager;
    private ConsumerGroupCoordinator groupCoordinator;
    private RequestHandler requestHandler;
    private TcpGateway tcpGateway;

    public AeronMqServer(BrokerConfig config) {
        this.config = config;
    }

    /**
     * Start the Aeron MQ server.
     */
    public void start() throws Exception {
        LOG.info("Starting Aeron MQ Server with broker ID {}", config.brokerId());

        // Create directories
        File aeronDir = config.aeronDir().toFile();
        File archiveDir = config.dataDir().resolve("archive").toFile();

        aeronDir.mkdirs();
        archiveDir.mkdirs();

        // Start embedded Media Driver
        LOG.info("Starting Aeron Media Driver at {}", aeronDir);
        MediaDriver.Context mediaDriverCtx = new MediaDriver.Context()
                .aeronDirectoryName(aeronDir.getAbsolutePath())
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);

        mediaDriver = MediaDriver.launch(mediaDriverCtx);

        // Connect Aeron client first
        LOG.info("Connecting Aeron client");
        Aeron.Context aeronCtx = new Aeron.Context()
                .aeronDirectoryName(aeronDir.getAbsolutePath());

        aeron = Aeron.connect(aeronCtx);

        // Start Archive with IPC channels
        LOG.info("Starting Aeron Archive at {}", archiveDir);
        String controlChannel = "aeron:ipc?term-length=64k";
        String replicationChannel = "aeron:udp?endpoint=localhost:0";

        Archive.Context archiveCtx = new Archive.Context()
                .aeronDirectoryName(aeronDir.getAbsolutePath())
                .archiveDir(archiveDir)
                .controlChannel(controlChannel)
                .localControlChannel(controlChannel)
                .recordingEventsChannel(controlChannel)
                .replicationChannel(replicationChannel)
                .threadingMode(ArchiveThreadingMode.SHARED)
                .deleteArchiveOnStart(false);

        archive = Archive.launch(archiveCtx);

        // Connect Archive client
        LOG.info("Connecting Archive client");
        AeronArchive.Context archiveClientCtx = new AeronArchive.Context()
                .aeron(aeron)
                .controlRequestChannel(controlChannel)
                .controlResponseChannel(controlChannel)
                .recordingEventsChannel(controlChannel)
                .ownsAeronClient(false);

        aeronArchive = AeronArchive.connect(archiveClientCtx);

        // Initialize broker components
        LOG.info("Initializing broker components");
        topicManager = new TopicManager(
                aeronArchive,
                aeron,
                config.brokerId(),
                config.autoCreateTopics(),
                config.defaultPartitionCount(),
                config.defaultReplicationFactor()
        );
        groupCoordinator = new ConsumerGroupCoordinator();
        requestHandler = new RequestHandler(topicManager, groupCoordinator, config);

        // Start TCP Gateway
        LOG.info("Starting TCP Gateway on {}:{}", config.host(), config.kafkaPort());
        tcpGateway = new TcpGateway(config.host(), config.kafkaPort(), requestHandler);
        tcpGateway.start();

        LOG.info("Aeron MQ Server started successfully");
        LOG.info("  Broker ID: {}", config.brokerId());
        LOG.info("  Kafka port: {}", config.kafkaPort());
        LOG.info("  Data dir: {}", config.dataDir());
    }

    /**
     * Wait for shutdown signal.
     */
    public void awaitTermination() throws InterruptedException {
        if (tcpGateway != null) {
            tcpGateway.awaitTermination();
        }
    }

    @Override
    public void close() {
        LOG.info("Shutting down Aeron MQ Server");

        CloseHelper.quietClose(tcpGateway);
        CloseHelper.quietClose(topicManager);
        CloseHelper.quietClose(aeronArchive);
        CloseHelper.quietClose(aeron);
        CloseHelper.quietClose(archive);
        CloseHelper.quietClose(mediaDriver);

        LOG.info("Aeron MQ Server shutdown complete");
    }

    public static void main(String[] args) {
        // Parse command line arguments
        int brokerId = getIntEnv("NODE_ID", 0);
        String host = getEnv("HOST", "0.0.0.0");
        int kafkaPort = getIntEnv("KAFKA_PORT", BrokerConfig.DEFAULT_KAFKA_PORT);
        String dataDir = getEnv("DATA_DIR", "data");
        String aeronDir = getEnv("AERON_DIR", "/dev/shm/aeron-mq-" + brokerId);
        boolean autoCreateTopics = getBoolEnv("AUTO_CREATE_TOPICS", true);
        String clusterMembers = getEnv("CLUSTER_MEMBERS", null);

        BrokerConfig.Builder configBuilder = BrokerConfig.builder()
                .brokerId(brokerId)
                .host(host)
                .kafkaPort(kafkaPort)
                .dataDir(Path.of(dataDir))
                .aeronDir(Path.of(aeronDir))
                .autoCreateTopics(autoCreateTopics);

        if (clusterMembers != null) {
            configBuilder.clusterConfig(BrokerConfig.ClusterConfig.parse(clusterMembers));
        }

        BrokerConfig config = configBuilder.build();

        // Setup shutdown hook
        ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

        try (AeronMqServer server = new AeronMqServer(config)) {
            server.start();

            // Wait for shutdown signal
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.info("Shutdown signal received");
                barrier.signal();
            }));

            barrier.await();

        } catch (Exception e) {
            LOG.error("Failed to start Aeron MQ Server", e);
            System.exit(1);
        }
    }

    private static String getEnv(String name, String defaultValue) {
        String value = System.getenv(name);
        return value != null ? value : defaultValue;
    }

    private static int getIntEnv(String name, int defaultValue) {
        String value = System.getenv(name);
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                LOG.warn("Invalid integer value for {}: {}", name, value);
            }
        }
        return defaultValue;
    }

    private static boolean getBoolEnv(String name, boolean defaultValue) {
        String value = System.getenv(name);
        if (value != null) {
            return Boolean.parseBoolean(value);
        }
        return defaultValue;
    }
}

