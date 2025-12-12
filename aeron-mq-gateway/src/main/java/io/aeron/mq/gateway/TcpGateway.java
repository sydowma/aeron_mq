package io.aeron.mq.gateway;

import io.aeron.mq.broker.RequestHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * TCP Gateway for Kafka protocol.
 * Uses Netty for high-performance networking with optional epoll support on Linux.
 */
public final class TcpGateway implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TcpGateway.class);

    private final String host;
    private final int port;
    private final RequestHandler requestHandler;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private EventExecutorGroup businessGroup;
    private Channel serverChannel;

    public TcpGateway(String host, int port, RequestHandler requestHandler) {
        this.host = host;
        this.port = port;
        this.requestHandler = requestHandler;
    }

    /**
     * Start the TCP gateway.
     */
    public void start() throws InterruptedException {
        boolean useEpoll = Epoll.isAvailable();
        LOG.info("Starting TCP gateway on {}:{} (epoll={})", host, port, useEpoll);

        if (useEpoll) {
            bossGroup = new EpollEventLoopGroup(1);
            workerGroup = new EpollEventLoopGroup();
        } else {
            bossGroup = new NioEventLoopGroup(1);
            workerGroup = new NioEventLoopGroup();
        }

        // Run protocol decode/encode on IO threads, but offload request processing to dedicated threads.
        // This prevents broker backpressure/replay from stalling Netty event loops.
        businessGroup = new DefaultEventExecutorGroup(Math.max(1, Runtime.getRuntime().availableProcessors()));

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(useEpoll ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.RCVBUF_ALLOCATOR, new AdaptiveRecvByteBufAllocator())
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();

                        // Inbound: Frame decoder -> Request handler
                        pipeline.addLast("frameDecoder", new KafkaFrameDecoder());

                        // Outbound: Frame encoder
                        pipeline.addLast("frameEncoder", new KafkaFrameEncoder());

                        // Business logic handler
                        pipeline.addLast(businessGroup, "requestHandler", new KafkaRequestHandler(requestHandler));
                    }
                });

        ChannelFuture future = bootstrap.bind(new InetSocketAddress(host, port)).sync();
        serverChannel = future.channel();

        LOG.info("TCP gateway started on {}:{}", host, port);
    }

    /**
     * Wait for the server to close.
     */
    public void awaitTermination() throws InterruptedException {
        if (serverChannel != null) {
            serverChannel.closeFuture().sync();
        }
    }

    /**
     * Stop the TCP gateway.
     */
    @Override
    public void close() {
        LOG.info("Stopping TCP gateway");

        if (serverChannel != null) {
            serverChannel.close();
        }

        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }

        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }

        if (businessGroup != null) {
            businessGroup.shutdownGracefully();
        }

        LOG.info("TCP gateway stopped");
    }
}


