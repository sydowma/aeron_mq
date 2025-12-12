package io.aeron.mq.gateway;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * Decodes Kafka protocol frames.
 * Kafka uses a length-prefixed framing where each message starts with a 4-byte length field.
 */
public final class KafkaFrameDecoder extends LengthFieldBasedFrameDecoder {

    private static final int MAX_FRAME_SIZE = 100 * 1024 * 1024; // 100MB max frame
    private static final int LENGTH_FIELD_OFFSET = 0;
    private static final int LENGTH_FIELD_LENGTH = 4;
    private static final int LENGTH_ADJUSTMENT = 0;
    private static final int INITIAL_BYTES_TO_STRIP = 4; // Strip the length field

    public KafkaFrameDecoder() {
        super(MAX_FRAME_SIZE, LENGTH_FIELD_OFFSET, LENGTH_FIELD_LENGTH, LENGTH_ADJUSTMENT, INITIAL_BYTES_TO_STRIP);
    }

    public KafkaFrameDecoder(int maxFrameSize) {
        super(maxFrameSize, LENGTH_FIELD_OFFSET, LENGTH_FIELD_LENGTH, LENGTH_ADJUSTMENT, INITIAL_BYTES_TO_STRIP);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        return super.decode(ctx, in);
    }
}


