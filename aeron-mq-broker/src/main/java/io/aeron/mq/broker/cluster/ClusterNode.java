package io.aeron.mq.broker.cluster;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Represents a node in the Aeron Cluster.
 * Manages the clustered media driver, consensus module, and service container.
 */
public final class ClusterNode implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterNode.class);

    private final int nodeId;
    private final String clusterMembers;
    private final String baseDir;
    private final String hostname;
    private final int basePort;

    private ClusteredMediaDriver clusteredMediaDriver;
    private ClusteredServiceContainer serviceContainer;

    public ClusterNode(int nodeId, String clusterMembers, String baseDir, String hostname, int basePort) {
        this.nodeId = nodeId;
        this.clusterMembers = clusterMembers;
        this.baseDir = baseDir;
        this.hostname = hostname;
        this.basePort = basePort;
    }

    /**
     * Start the cluster node.
     */
    public void start() {
        LOG.info("Starting cluster node {} at {}:{}", nodeId, hostname, basePort);

        String aeronDirName = baseDir + "/aeron-" + nodeId;
        String archiveDir = baseDir + "/archive-" + nodeId;
        String clusterDir = baseDir + "/cluster-" + nodeId;

        new File(aeronDirName).mkdirs();
        new File(archiveDir).mkdirs();
        new File(clusterDir).mkdirs();

        // Media Driver context
        MediaDriver.Context mediaDriverCtx = new MediaDriver.Context()
                .aeronDirectoryName(aeronDirName)
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);

        // Archive context
        Archive.Context archiveCtx = new Archive.Context()
                .archiveDir(new File(archiveDir))
                .controlChannel(udpChannel(hostname, basePort + 1))
                .localControlChannel("aeron:ipc?term-length=64k")
                .recordingEventsEnabled(false)
                .threadingMode(ArchiveThreadingMode.SHARED);

        // Aeron Archive context
        AeronArchive.Context aeronArchiveCtx = new AeronArchive.Context()
                .controlRequestChannel(archiveCtx.controlChannel())
                .controlResponseChannel(udpChannel(hostname, 0))
                .recordingEventsChannel("aeron:ipc?term-length=64k");

        // Consensus module context
        ConsensusModule.Context consensusModuleCtx = new ConsensusModule.Context()
                .clusterMemberId(nodeId)
                .clusterMembers(clusterMembers)
                .clusterDir(new File(clusterDir))
                .ingressChannel("aeron:udp?term-length=64k")
                .logChannel(logChannel())
                .replicationChannel(udpChannel(hostname, 0))
                .archiveContext(aeronArchiveCtx.clone());

        // Service container context
        ClusteredServiceContainer.Context serviceContainerCtx = new ClusteredServiceContainer.Context()
                .clusteredService(new BrokerStateMachine())
                .clusterDir(new File(clusterDir))
                .archiveContext(aeronArchiveCtx.clone());

        // Launch clustered media driver
        clusteredMediaDriver = ClusteredMediaDriver.launch(
                mediaDriverCtx,
                archiveCtx,
                consensusModuleCtx
        );

        // Launch service container
        serviceContainer = ClusteredServiceContainer.launch(serviceContainerCtx);

        LOG.info("Cluster node {} started", nodeId);
    }

    private String udpChannel(String host, int port) {
        if (port == 0) {
            return "aeron:udp?endpoint=" + host + ":0";
        }
        return "aeron:udp?endpoint=" + host + ":" + port;
    }

    private String logChannel() {
        return "aeron:udp?term-length=256k|control-mode=manual|control=" + hostname + ":" + (basePort + 2);
    }

    @Override
    public void close() {
        LOG.info("Closing cluster node {}", nodeId);
        CloseHelper.quietClose(serviceContainer);
        CloseHelper.quietClose(clusteredMediaDriver);
    }

    /**
     * Parse cluster members string.
     * Format: nodeId,hostname,port|nodeId,hostname,port|...
     *
     * @return Aeron cluster members string format
     */
    public static String parseClusterMembers(String input) {
        if (input == null || input.isEmpty()) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        String[] members = input.split("\\|");

        for (int i = 0; i < members.length; i++) {
            String[] parts = members[i].split(",");
            if (parts.length >= 3) {
                int memberId = Integer.parseInt(parts[0].trim());
                String host = parts[1].trim();
                int basePort = Integer.parseInt(parts[2].trim());

                if (i > 0) {
                    sb.append(",");
                }

                // Format: memberId,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint
                sb.append(memberId).append(",")
                        .append(host).append(":").append(basePort).append(",")        // ingress
                        .append(host).append(":").append(basePort + 1).append(",")    // consensus
                        .append(host).append(":").append(basePort + 2).append(",")    // log
                        .append(host).append(":").append(basePort + 3).append(",")    // catchup
                        .append(host).append(":").append(basePort + 4);               // archive
            }
        }

        return sb.toString();
    }

    public static void main(String[] args) {
        int nodeId = Integer.parseInt(System.getenv().getOrDefault("NODE_ID", "0"));
        String hostname = System.getenv().getOrDefault("HOSTNAME", "localhost");
        int basePort = Integer.parseInt(System.getenv().getOrDefault("BASE_PORT", "20000"));
        String clusterMembersInput = System.getenv().getOrDefault("CLUSTER_MEMBERS", "0,localhost,20000");
        String baseDir = System.getenv().getOrDefault("DATA_DIR", "data");

        String clusterMembers = parseClusterMembers(clusterMembersInput);
        LOG.info("Cluster members: {}", clusterMembers);

        ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

        try (ClusterNode node = new ClusterNode(nodeId, clusterMembers, baseDir, hostname, basePort)) {
            node.start();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.info("Shutdown signal received");
                barrier.signal();
            }));

            barrier.await();
        }
    }
}


