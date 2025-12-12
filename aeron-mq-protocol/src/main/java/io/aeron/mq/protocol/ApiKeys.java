package io.aeron.mq.protocol;

/**
 * Kafka API keys enumeration.
 * Only includes APIs that are implemented by Aeron MQ.
 */
public enum ApiKeys {
    PRODUCE(0, "Produce"),
    FETCH(1, "Fetch"),
    LIST_OFFSETS(2, "ListOffsets"),
    METADATA(3, "Metadata"),
    OFFSET_COMMIT(8, "OffsetCommit"),
    OFFSET_FETCH(9, "OffsetFetch"),
    FIND_COORDINATOR(10, "FindCoordinator"),
    JOIN_GROUP(11, "JoinGroup"),
    HEARTBEAT(12, "Heartbeat"),
    LEAVE_GROUP(13, "LeaveGroup"),
    SYNC_GROUP(14, "SyncGroup"),
    API_VERSIONS(18, "ApiVersions");

    private final short id;
    private final String name;

    ApiKeys(int id, String name) {
        this.id = (short) id;
        this.name = name;
    }

    public short id() {
        return id;
    }

    public String apiName() {
        return name;
    }

    private static final ApiKeys[] ID_TO_API;

    static {
        int maxId = 0;
        for (ApiKeys api : values()) {
            maxId = Math.max(maxId, api.id);
        }
        ID_TO_API = new ApiKeys[maxId + 1];
        for (ApiKeys api : values()) {
            ID_TO_API[api.id] = api;
        }
    }

    public static ApiKeys forId(int id) {
        if (id < 0 || id >= ID_TO_API.length) {
            return null;
        }
        return ID_TO_API[id];
    }

    public static boolean isValidApiKey(int id) {
        return forId(id) != null;
    }
}


