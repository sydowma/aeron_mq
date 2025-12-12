package io.aeron.mq.protocol;

/**
 * Kafka error codes.
 */
public enum Errors {
    NONE(0, "No error"),
    UNKNOWN_SERVER_ERROR(-1, "Unknown server error"),
    OFFSET_OUT_OF_RANGE(1, "Offset out of range"),
    CORRUPT_MESSAGE(2, "Corrupt message"),
    UNKNOWN_TOPIC_OR_PARTITION(3, "Unknown topic or partition"),
    INVALID_FETCH_SIZE(4, "Invalid fetch size"),
    LEADER_NOT_AVAILABLE(5, "Leader not available"),
    NOT_LEADER_OR_FOLLOWER(6, "Not leader or follower"),
    REQUEST_TIMED_OUT(7, "Request timed out"),
    BROKER_NOT_AVAILABLE(8, "Broker not available"),
    REPLICA_NOT_AVAILABLE(9, "Replica not available"),
    MESSAGE_TOO_LARGE(10, "Message too large"),
    STALE_CONTROLLER_EPOCH(11, "Stale controller epoch"),
    OFFSET_METADATA_TOO_LARGE(12, "Offset metadata too large"),
    NETWORK_EXCEPTION(13, "Network exception"),
    COORDINATOR_LOAD_IN_PROGRESS(14, "Coordinator load in progress"),
    COORDINATOR_NOT_AVAILABLE(15, "Coordinator not available"),
    NOT_COORDINATOR(16, "Not coordinator"),
    INVALID_TOPIC_EXCEPTION(17, "Invalid topic"),
    RECORD_LIST_TOO_LARGE(18, "Record list too large"),
    NOT_ENOUGH_REPLICAS(19, "Not enough replicas"),
    NOT_ENOUGH_REPLICAS_AFTER_APPEND(20, "Not enough replicas after append"),
    INVALID_REQUIRED_ACKS(21, "Invalid required acks"),
    ILLEGAL_GENERATION(22, "Illegal generation"),
    INCONSISTENT_GROUP_PROTOCOL(23, "Inconsistent group protocol"),
    INVALID_GROUP_ID(24, "Invalid group id"),
    UNKNOWN_MEMBER_ID(25, "Unknown member id"),
    INVALID_SESSION_TIMEOUT(26, "Invalid session timeout"),
    REBALANCE_IN_PROGRESS(27, "Rebalance in progress"),
    INVALID_COMMIT_OFFSET_SIZE(28, "Invalid commit offset size"),
    TOPIC_AUTHORIZATION_FAILED(29, "Topic authorization failed"),
    GROUP_AUTHORIZATION_FAILED(30, "Group authorization failed"),
    CLUSTER_AUTHORIZATION_FAILED(31, "Cluster authorization failed"),
    INVALID_TIMESTAMP(32, "Invalid timestamp"),
    UNSUPPORTED_SASL_MECHANISM(33, "Unsupported SASL mechanism"),
    ILLEGAL_SASL_STATE(34, "Illegal SASL state"),
    UNSUPPORTED_VERSION(35, "Unsupported version"),
    TOPIC_ALREADY_EXISTS(36, "Topic already exists"),
    INVALID_PARTITIONS(37, "Invalid partitions"),
    INVALID_REPLICATION_FACTOR(38, "Invalid replication factor"),
    INVALID_REPLICA_ASSIGNMENT(39, "Invalid replica assignment"),
    INVALID_CONFIG(40, "Invalid config"),
    NOT_CONTROLLER(41, "Not controller"),
    INVALID_REQUEST(42, "Invalid request"),
    UNSUPPORTED_FOR_MESSAGE_FORMAT(43, "Unsupported for message format"),
    POLICY_VIOLATION(44, "Policy violation");

    private final short code;
    private final String message;

    Errors(int code, String message) {
        this.code = (short) code;
        this.message = message;
    }

    public short code() {
        return code;
    }

    public String message() {
        return message;
    }

    private static final Errors[] CODE_TO_ERROR;

    static {
        int maxCode = 0;
        int minCode = 0;
        for (Errors error : values()) {
            maxCode = Math.max(maxCode, error.code);
            minCode = Math.min(minCode, error.code);
        }
        CODE_TO_ERROR = new Errors[maxCode - minCode + 1];
        for (Errors error : values()) {
            CODE_TO_ERROR[error.code - minCode] = error;
        }
    }

    public static Errors forCode(short code) {
        int index = code + 1; // offset by -1 (UNKNOWN_SERVER_ERROR)
        if (index < 0 || index >= CODE_TO_ERROR.length) {
            return UNKNOWN_SERVER_ERROR;
        }
        Errors error = CODE_TO_ERROR[index];
        return error != null ? error : UNKNOWN_SERVER_ERROR;
    }
}


