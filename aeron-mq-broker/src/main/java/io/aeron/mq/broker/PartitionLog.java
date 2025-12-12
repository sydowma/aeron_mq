package io.aeron.mq.broker;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.mq.protocol.Errors;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.collections.IntArrayList;
import org.agrona.collections.LongArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages a single partition's log using Aeron Archive for persistence.
 * Each partition maps to an Aeron recording that can be replayed for consumers.
 */
public final class PartitionLog implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionLog.class);

    /**
     * We model a partition log as a single Aeron stream where each appended Kafka record-batch bytes
     * is sent as a single Aeron message (no fragmentation). Kafka offsets are sequential longs
     * (0..N-1) mapped to Aeron {@code position} values for replay.
     *
     * <p>Important constraints for correctness:
     * - Publication must use a deterministic {@code session-id} so we can extend the same recording after restart.
     * - We must not allow message fragmentation, otherwise offset->position indexing becomes ambiguous.
     */
    private static final int STREAM_ID_BASE = 10_000;
    private static final int REPLAY_STREAM_ID_BASE = 2_000_000_000;
    private static final AtomicInteger REPLAY_STREAM_ID = new AtomicInteger(REPLAY_STREAM_ID_BASE);

    private static final String CHANNEL_TEMPLATE =
            "aeron:ipc?term-length=64k|alias=aeron-mq-%s|session-id=%d";

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration OFFER_TIMEOUT = Duration.ofMillis(50);

    private final TopicPartition topicPartition;
    private final AeronArchive archive;
    private final Aeron aeron;
    private final int streamId;
    private final String channel;
    private final String channelFragment;
    private final int sessionId;

    private ExclusivePublication publication;
    private long recordingId = Aeron.NULL_VALUE;
    private volatile long nextOffset = 0;
    private volatile long logStartOffset = 0;

    // Offset -> Aeron start position, payload length, and frame span (position delta).
    // Stored in-memory for fast lookup; rebuilt by scanning the recording on startup.
    private final LongArrayList positionsByOffset = new LongArrayList();
    private final IntArrayList payloadLengthsByOffset = new IntArrayList();
    private final IntArrayList spansByOffset = new IntArrayList();

    // Leader epoch tracking
    private int leaderEpoch = 0;

    public PartitionLog(TopicPartition topicPartition, AeronArchive archive, Aeron aeron) {
        this.topicPartition = topicPartition;
        this.archive = archive;
        this.aeron = aeron;
        this.streamId = computeStreamId(topicPartition);
        this.sessionId = computeSessionId(topicPartition);

        final String alias = sanitizeAlias("tp-" + topicPartition.topic() + "-" + topicPartition.partition());
        this.channel = CHANNEL_TEMPLATE.formatted(alias, sessionId);
        this.channelFragment = "alias=aeron-mq-" + alias;
    }

    /**
     * Initialize the partition log, creating or resuming the recording.
     */
    public void initialize() {
        // Find existing recording or start a new one.
        recordingId = findLatestRecordingId();

        if (recordingId == Aeron.NULL_VALUE) {
            archive.startRecording(channel, streamId, SourceLocation.LOCAL);
            recordingId = awaitRecordingId();
            LOG.info("Started new recording {} for {}", recordingId, topicPartition);
        } else {
            archive.extendRecording(recordingId, channel, streamId, SourceLocation.LOCAL);
            LOG.info("Extending existing recording {} for {}", recordingId, topicPartition);
        }

        publication = aeron.addExclusivePublication(channel, streamId);
        awaitConnected(publication);

        // Rebuild offset index from persisted recording so offsets do not reset on restart.
        rebuildIndexFromRecording();
        LOG.info("Partition log initialized for {}: recordingId={}, nextOffset={}",
                topicPartition, recordingId, nextOffset);
    }

    private long findLatestRecordingId() {
        final long[] lastRecordingId = {Aeron.NULL_VALUE};

        archive.listRecordingsForUri(0, 10_000, channelFragment, streamId,
                (controlSessionId, correlationId, recId,
                 startTimestamp, stopTimestamp, startPosition, stopPosition,
                 initialTermId, segmentFileLength, termBufferLength,
                 mtuLength, sessionIdRec, streamIdRec, strippedChannel,
                 originalChannel, sourceIdentity) -> {
                    if (streamIdRec == streamId && sessionIdRec == sessionId) {
                        if (recId > lastRecordingId[0]) {
                            lastRecordingId[0] = recId;
                        }
                    }
                });

        return lastRecordingId[0];
    }

    private long awaitRecordingId() {
        final long deadlineNs = System.nanoTime() + CONNECT_TIMEOUT.toNanos();
        long recId;
        while ((recId = findLatestRecordingId()) == Aeron.NULL_VALUE) {
            if (System.nanoTime() >= deadlineNs) {
                throw new IllegalStateException("Timed out waiting for recordingId for " + topicPartition);
            }
            Thread.onSpinWait();
        }
        return recId;
    }

    private static void awaitConnected(final ExclusivePublication publication) {
        final long deadlineNs = System.nanoTime() + CONNECT_TIMEOUT.toNanos();
        while (!publication.isConnected()) {
            if (System.nanoTime() >= deadlineNs) {
                throw new IllegalStateException("Timed out waiting for publication to connect: " + publication);
            }
            Thread.onSpinWait();
        }
    }

    private void rebuildIndexFromRecording() {
        positionsByOffset.clear();
        payloadLengthsByOffset.clear();
        spansByOffset.clear();

        if (recordingId == Aeron.NULL_VALUE) {
            nextOffset = 0;
            return;
        }

        final long startPosition = archive.getStartPosition(recordingId);
        long stopPosition = archive.getStopPosition(recordingId);
        if (stopPosition == AeronArchive.NULL_POSITION) {
            stopPosition = archive.getRecordingPosition(recordingId);
        }

        if (stopPosition <= startPosition) {
            nextOffset = 0;
            return;
        }

        final long length = stopPosition - startPosition;
        final int replayStreamId = REPLAY_STREAM_ID.getAndIncrement();
        Subscription replaySub = null;

        try {
            replaySub = archive.replay(recordingId, startPosition, length, "aeron:ipc", replayStreamId);

            final BackoffIdleStrategy idle = new BackoffIdleStrategy(
                    1, 10, 1_000, 100_000);

            Image image;
            final long connectDeadlineNs = System.nanoTime() + CONNECT_TIMEOUT.toNanos();
            while ((image = replaySub.imageAtIndex(0)) == null) {
                if (System.nanoTime() >= connectDeadlineNs) {
                    throw new IllegalStateException("Timed out waiting for replay image: " + topicPartition);
                }
                idle.idle();
            }

            // Each Aeron fragment corresponds to one appended record-batch (we enforce no fragmentation on append).
            while (!image.isClosed()) {
                final int fragments = image.poll((buffer, offset, fragmentLength, header) -> {
                    positionsByOffset.addLong(header.position());
                    payloadLengthsByOffset.addInt(fragmentLength);
                    spansByOffset.addInt(align(header.frameLength(), FrameDescriptor.FRAME_ALIGNMENT));
                }, 10);

                if (fragments == 0) {
                    // When replay is finished, image will close.
                    idle.idle();
                } else {
                    idle.reset();
                }
            }
        } finally {
            CloseHelper.quietClose(replaySub);
        }

        nextOffset = positionsByOffset.size();
    }

    /**
     * Append records to the partition log.
     *
     * @param records the record batch bytes
     * @return the base offset of appended records, or -1 on failure
     */
    public long append(byte[] records) {
        if (records == null || records.length == 0) {
            return nextOffset;
        }

        return append(new UnsafeBuffer(records), 0, records.length);
    }

    /**
     * Append records from a DirectBuffer (zero-copy).
     *
     * @param buffer the buffer containing records
     * @param offset the offset in the buffer
     * @param length the length of records
     * @return the base offset of appended records, or -1 on failure
     */
    public long append(DirectBuffer buffer, int offset, int length) {
        if (length == 0) {
            return nextOffset;
        }

        final ExclusivePublication pub = this.publication;
        if (pub == null) {
            LOG.error("PartitionLog not initialized: {}", topicPartition);
            return -1;
        }

        // Enforce single Aeron fragment per Kafka record-batch to keep offset indexing correct.
        if (length > pub.maxPayloadLength()) {
            LOG.warn("Rejecting append to {}: record batch too large ({} > maxPayloadLength={})",
                    topicPartition, length, pub.maxPayloadLength());
            return -1;
        }

        final long baseOffset;
        final long startPosition;
        synchronized (this) {
            baseOffset = nextOffset;
            startPosition = pub.position();
        }

        final BackoffIdleStrategy idle = new BackoffIdleStrategy(1, 10, 1_000, 100_000);
        final long deadlineNs = System.nanoTime() + OFFER_TIMEOUT.toNanos();
        long result;
        while (true) {
            result = pub.offer(buffer, offset, length);
            if (result >= 0) {
                break;
            }
            if (result == ExclusivePublication.NOT_CONNECTED ||
                result == ExclusivePublication.MAX_POSITION_EXCEEDED) {
                LOG.error("Failed to append to {}: result={}", topicPartition, result);
                return -1;
            }
            if (System.nanoTime() >= deadlineNs) {
                LOG.warn("Append timed out due to backpressure for {}: result={}", topicPartition, result);
                return -1;
            }
            idle.idle();
        }

        // Update index and next offset (each record batch = 1 Kafka offset in this prototype).
        synchronized (this) {
            final int span = (int) (result - startPosition);
            positionsByOffset.addLong(startPosition);
            payloadLengthsByOffset.addInt(length);
            spansByOffset.addInt(span);
            nextOffset++;
        }

        return baseOffset;
    }

    /**
     * Read the record-batch at a given Kafka offset.
     *
     * <p>This is a minimal implementation: one append == one fetchable record-batch == one offset.</p>
     */
    public ReadResult read(long offset, int maxBytes) {
        if (offset < logStartOffset) {
            return ReadResult.offsetOutOfRange();
        }
        if (offset == nextOffset) {
            return ReadResult.empty();
        }
        if (offset > nextOffset) {
            return ReadResult.offsetOutOfRange();
        }

        if (offset > Integer.MAX_VALUE) {
            return ReadResult.offsetOutOfRange();
        }

        final int index = (int) offset;
        final long startPosition;
        final int payloadLength;
        final int span;
        synchronized (this) {
            if (index >= positionsByOffset.size()) {
                return ReadResult.offsetOutOfRange();
            }
            startPosition = positionsByOffset.getLong(index);
            payloadLength = payloadLengthsByOffset.getInt(index);
            span = spansByOffset.getInt(index);
        }

        if (payloadLength > maxBytes) {
            return ReadResult.tooLarge();
        }

        if (recordingId == Aeron.NULL_VALUE) {
            return ReadResult.empty();
        }

        final int replayStreamId = REPLAY_STREAM_ID.getAndIncrement();
        final long replaySessionId = archive.startReplay(recordingId, startPosition, span, "aeron:ipc", replayStreamId);
        Subscription replaySub = null;

        try {
            replaySub = archive.context().aeron().addSubscription("aeron:ipc", replayStreamId);

            final BackoffIdleStrategy idle = new BackoffIdleStrategy(1, 10, 1_000, 100_000);
            final long deadlineNs = System.nanoTime() + CONNECT_TIMEOUT.toNanos();

            Image image;
            while ((image = replaySub.imageAtIndex(0)) == null) {
                if (System.nanoTime() >= deadlineNs) {
                    return ReadResult.empty();
                }
                idle.idle();
            }

            final byte[] out = new byte[payloadLength];
            final AtomicLong received = new AtomicLong(0);

            while (received.get() == 0) {
                final int fragments = image.poll((buffer, bufferOffset, length, header) -> {
                    if (length != payloadLength) {
                        // Defensive: we rely on no fragmentation and stable payload sizes.
                        LOG.warn("Unexpected replay fragment length for {}: expected={}, actual={}",
                                topicPartition, payloadLength, length);
                    }
                    final int toCopy = Math.min(length, out.length);
                    buffer.getBytes(bufferOffset, out, 0, toCopy);
                    received.set(toCopy);
                }, 10);

                if (received.get() != 0) {
                    break;
                }
                if (fragments == 0) {
                    if (System.nanoTime() >= deadlineNs) {
                        return ReadResult.empty();
                    }
                    idle.idle();
                } else {
                    idle.reset();
                }
            }

            return ReadResult.records(out);
        } finally {
            CloseHelper.quietClose(replaySub);
            try {
                archive.stopReplay(replaySessionId);
            } catch (Exception e) {
                // ignore
            }
        }
    }

    /**
     * Get the high watermark (next offset to be written).
     */
    public long highWatermark() {
        return nextOffset;
    }

    /**
     * Maximum payload length that can be appended without fragmentation.
     */
    public int maxPayloadLength() {
        final ExclusivePublication pub = this.publication;
        return pub != null ? pub.maxPayloadLength() : 0;
    }

    /**
     * Get the log start offset.
     */
    public long logStartOffset() {
        return logStartOffset;
    }

    /**
     * Get the current leader epoch.
     */
    public int leaderEpoch() {
        return leaderEpoch;
    }

    /**
     * Increment and get the leader epoch.
     */
    public int incrementLeaderEpoch() {
        return ++leaderEpoch;
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public long recordingId() {
        return recordingId;
    }

    private static int computeStreamId(TopicPartition tp) {
        // StreamId is shared across sessions; partition index is enough to keep it stable and predictable.
        return STREAM_ID_BASE + Math.max(0, tp.partition());
    }

    private static int computeSessionId(TopicPartition tp) {
        // Deterministic 31-bit positive session-id derived from (topic,partition).
        int h = 17;
        h = 31 * h + Objects.hashCode(tp.topic());
        h = 31 * h + tp.partition();
        h &= 0x7fffffff;
        return h == 0 ? 1 : h;
    }

    private static String sanitizeAlias(String value) {
        final StringBuilder sb = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            final char c = value.charAt(i);
            if ((c >= 'a' && c <= 'z') ||
                (c >= 'A' && c <= 'Z') ||
                (c >= '0' && c <= '9') ||
                c == '-' || c == '_' || c == '.') {
                sb.append(c);
            } else {
                sb.append('_');
            }
        }
        // Keep alias reasonably short (Aeron URIs can get unwieldy).
        final int maxLen = 128;
        return sb.length() <= maxLen ? sb.toString() : sb.substring(0, maxLen);
    }

    private static int align(final int value, final int alignment) {
        return (value + (alignment - 1)) & ~(alignment - 1);
    }

    @Override
    public void close() {
        CloseHelper.quietClose(publication);
        try {
            archive.stopRecording(channel, streamId);
        } catch (Exception e) {
            LOG.debug("Error stopping recording for {}: {}", topicPartition, e.getMessage());
        }
    }

    public record ReadResult(short errorCode, byte[] records) {
        static ReadResult records(byte[] records) {
            return new ReadResult(Errors.NONE.code(), records);
        }

        static ReadResult empty() {
            return new ReadResult(Errors.NONE.code(), new byte[0]);
        }

        static ReadResult offsetOutOfRange() {
            return new ReadResult(Errors.OFFSET_OUT_OF_RANGE.code(), new byte[0]);
        }

        static ReadResult tooLarge() {
            return new ReadResult(Errors.RECORD_LIST_TOO_LARGE.code(), new byte[0]);
        }
    }
}


