package io.aeron.mq.gateway;

import io.aeron.mq.broker.RequestHandler;
import io.aeron.mq.protocol.*;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Handles incoming Kafka protocol requests.
 * Decodes requests, processes them via RequestHandler, and encodes responses.
 */
public final class KafkaRequestHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaRequestHandler.class);

    private final RequestHandler requestHandler;
    private final KafkaRequestDecoder decoder = new KafkaRequestDecoder();
    private final KafkaResponseEncoder encoder = new KafkaResponseEncoder();

    // Reusable buffers for decoding/encoding (per-channel, so thread-safe)
    private final UnsafeBuffer decodeBuffer = new UnsafeBuffer();
    private final ExpandableArrayBuffer encodeBuffer = new ExpandableArrayBuffer(64 * 1024);

    public KafkaRequestHandler(RequestHandler requestHandler) {
        this.requestHandler = requestHandler;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
        try {
            // Wrap Netty ByteBuf for decoding, preferring zero-copy paths.
            final int readableBytes = msg.readableBytes();
            if (readableBytes == 0) {
                return;
            }

            if (msg.hasArray()) {
                decodeBuffer.wrap(msg.array(), msg.arrayOffset() + msg.readerIndex(), readableBytes);
                decoder.wrap(decodeBuffer, 0, readableBytes);
            } else if (msg.nioBufferCount() == 1) {
                // Direct buffer: decode from NIO view to avoid heap copy
                ByteBuffer nio = msg.nioBuffer(msg.readerIndex(), readableBytes);
                decoder.wrap(nio);
            } else {
                // Composite buffers: copy as a fallback
                byte[] bytes = new byte[readableBytes];
                msg.getBytes(msg.readerIndex(), bytes);
                decodeBuffer.wrap(bytes);
                decoder.wrap(decodeBuffer, 0, readableBytes);
            }

            RequestHeader header = decoder.decodeHeader();

            if (LOG.isDebugEnabled()) {
                ApiKeys apiKey = ApiKeys.forId(header.apiKey());
                LOG.debug("Received request: api={}, version={}, correlationId={}, clientId={}",
                        apiKey != null ? apiKey.apiName() : header.apiKey(),
                        header.apiVersion(), header.correlationId(), header.clientId());
            }

            // Decode request body
            KafkaRequest request = decoder.decodeRequest(header);

            // Handle request
            KafkaResponse response = requestHandler.handleRequest(request);

            // Encode response
            encoder.wrap(encodeBuffer, 0);
            encoder.encodeHeader(header.correlationId());
            encoder.encodeResponse(response, header.apiVersion());

            int responseLength = encoder.encodedLength();

            // Write response to Netty buffer
            ByteBuf responseBuf = ctx.alloc().buffer(responseLength);
            responseBuf.writeBytes(encodeBuffer.byteArray(), 0, responseLength);

            ctx.writeAndFlush(responseBuf);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Sent response: correlationId={}, length={}",
                        header.correlationId(), responseLength);
            }

        } catch (Exception e) {
            LOG.error("Error processing request", e);
            ctx.close();
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        LOG.info("Client connected: {}", ctx.channel().remoteAddress());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        LOG.info("Client disconnected: {}", ctx.channel().remoteAddress());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error("Channel exception", cause);
        ctx.close();
    }
}


