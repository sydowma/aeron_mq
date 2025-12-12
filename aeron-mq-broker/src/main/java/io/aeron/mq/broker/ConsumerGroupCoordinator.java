package io.aeron.mq.broker;

import io.aeron.mq.protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Coordinates consumer groups, handling group membership and offset management.
 * In cluster mode, this state would be replicated through Aeron Cluster.
 */
public final class ConsumerGroupCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerGroupCoordinator.class);

    private static final int DEFAULT_SESSION_TIMEOUT_MS = 30000;
    private static final int MIN_SESSION_TIMEOUT_MS = 6000;
    private static final int MAX_SESSION_TIMEOUT_MS = 300000;

    private final ConcurrentHashMap<String, ConsumerGroup> groups = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<GroupTopicPartition, CommittedOffset> offsets = new ConcurrentHashMap<>();
    private final AtomicInteger memberIdCounter = new AtomicInteger(0);

    public ConsumerGroupCoordinator() {
    }

    /**
     * Handle JoinGroup request.
     */
    public JoinGroupResponse handleJoinGroup(JoinGroupRequest request) {
        String groupId = request.groupId();
        String memberId = request.memberId();

        // Validate request
        if (groupId == null || groupId.isEmpty()) {
            return errorJoinGroupResponse(Errors.INVALID_GROUP_ID);
        }

        if (request.sessionTimeoutMs() < MIN_SESSION_TIMEOUT_MS ||
            request.sessionTimeoutMs() > MAX_SESSION_TIMEOUT_MS) {
            return errorJoinGroupResponse(Errors.INVALID_SESSION_TIMEOUT);
        }

        ConsumerGroup group = groups.computeIfAbsent(groupId, ConsumerGroup::new);

        synchronized (group) {
            // If member ID is empty, generate a new one
            if (memberId == null || memberId.isEmpty()) {
                memberId = generateMemberId("consumer");
            }

            // Add or update member
            GroupMember member = new GroupMember(
                    memberId,
                    request.groupInstanceId(),
                    request.sessionTimeoutMs(),
                    request.rebalanceTimeoutMs(),
                    request.protocolType(),
                    extractProtocols(request.protocols()),
                    System.currentTimeMillis()
            );

            GroupMember existingMember = group.members.get(memberId);
            if (existingMember == null) {
                group.members.put(memberId, member);
                group.pendingJoinMembers.add(memberId);
            } else {
                // Update existing member
                group.members.put(memberId, member);
            }

            // Check if we should complete the join
            boolean allMembersJoined = group.pendingJoinMembers.containsAll(group.members.keySet());

            if (allMembersJoined || group.members.size() == 1) {
                // Complete the join phase
                group.generationId++;
                group.state = GroupState.COMPLETING_REBALANCE;
                group.pendingJoinMembers.clear();

                // First member to join becomes leader
                if (group.leader == null || !group.members.containsKey(group.leader)) {
                    group.leader = memberId;
                }

                // Select protocol
                String selectedProtocol = selectProtocol(group);
                group.protocolName = selectedProtocol;
                group.protocolType = request.protocolType();

                // Build member list for leader
                List<JoinGroupResponse.JoinGroupMember> members = new ArrayList<>();
                if (memberId.equals(group.leader)) {
                    for (GroupMember m : group.members.values()) {
                        byte[] metadata = m.protocols.getOrDefault(selectedProtocol, new byte[0]);
                        members.add(new JoinGroupResponse.JoinGroupMember(
                                m.memberId,
                                m.groupInstanceId,
                                metadata
                        ));
                    }
                }

                LOG.info("JoinGroup completed for group={}, member={}, generation={}, leader={}",
                        groupId, memberId, group.generationId, group.leader);

                return new JoinGroupResponse(
                        0, // throttleTimeMs
                        Errors.NONE.code(),
                        group.generationId,
                        group.protocolType,
                        selectedProtocol,
                        group.leader,
                        false, // skipAssignment
                        memberId,
                        members
                );
            } else {
                // Wait for more members - return REBALANCE_IN_PROGRESS
                // In production, this would be async with timeout
                return new JoinGroupResponse(
                        0,
                        Errors.REBALANCE_IN_PROGRESS.code(),
                        -1,
                        null,
                        null,
                        "",
                        false,
                        memberId,
                        List.of()
                );
            }
        }
    }

    /**
     * Handle SyncGroup request.
     */
    public SyncGroupResponse handleSyncGroup(SyncGroupRequest request) {
        String groupId = request.groupId();
        String memberId = request.memberId();

        ConsumerGroup group = groups.get(groupId);
        if (group == null) {
            return new SyncGroupResponse(0, Errors.UNKNOWN_MEMBER_ID.code(), null, null, new byte[0]);
        }

        synchronized (group) {
            GroupMember member = group.members.get(memberId);
            if (member == null) {
                return new SyncGroupResponse(0, Errors.UNKNOWN_MEMBER_ID.code(), null, null, new byte[0]);
            }

            if (request.generationId() != group.generationId) {
                return new SyncGroupResponse(0, Errors.ILLEGAL_GENERATION.code(), null, null, new byte[0]);
            }

            // If this is the leader, store the assignments
            if (memberId.equals(group.leader) && request.assignments() != null) {
                for (SyncGroupRequest.SyncGroupAssignment assignment : request.assignments()) {
                    group.assignments.put(assignment.memberId(), assignment.assignment());
                }
                group.state = GroupState.STABLE;
                LOG.info("SyncGroup completed for group={}, leader stored {} assignments",
                        groupId, request.assignments().size());
            }

            // Return the assignment for this member
            byte[] assignment = group.assignments.getOrDefault(memberId, new byte[0]);

            return new SyncGroupResponse(
                    0,
                    Errors.NONE.code(),
                    group.protocolType,
                    group.protocolName,
                    assignment
            );
        }
    }

    /**
     * Handle Heartbeat request.
     */
    public HeartbeatResponse handleHeartbeat(HeartbeatRequest request) {
        String groupId = request.groupId();
        String memberId = request.memberId();

        ConsumerGroup group = groups.get(groupId);
        if (group == null) {
            return new HeartbeatResponse(0, Errors.UNKNOWN_MEMBER_ID.code());
        }

        synchronized (group) {
            GroupMember member = group.members.get(memberId);
            if (member == null) {
                return new HeartbeatResponse(0, Errors.UNKNOWN_MEMBER_ID.code());
            }

            if (request.generationId() != group.generationId) {
                return new HeartbeatResponse(0, Errors.ILLEGAL_GENERATION.code());
            }

            // Update heartbeat timestamp
            member.lastHeartbeat = System.currentTimeMillis();

            // Check if rebalance is needed
            if (group.state == GroupState.PREPARING_REBALANCE) {
                return new HeartbeatResponse(0, Errors.REBALANCE_IN_PROGRESS.code());
            }

            return new HeartbeatResponse(0, Errors.NONE.code());
        }
    }

    /**
     * Handle LeaveGroup request.
     */
    public LeaveGroupResponse handleLeaveGroup(LeaveGroupRequest request) {
        String groupId = request.groupId();

        ConsumerGroup group = groups.get(groupId);
        if (group == null) {
            return new LeaveGroupResponse(0, Errors.NONE.code(), null);
        }

        synchronized (group) {
            List<LeaveGroupResponse.MemberResponse> memberResponses = new ArrayList<>();

            if (request.members() != null) {
                // v3+ batch leave
                for (LeaveGroupRequest.MemberIdentity identity : request.members()) {
                    GroupMember removed = group.members.remove(identity.memberId());
                    short errorCode = removed != null ? Errors.NONE.code() : Errors.UNKNOWN_MEMBER_ID.code();
                    memberResponses.add(new LeaveGroupResponse.MemberResponse(
                            identity.memberId(),
                            identity.groupInstanceId(),
                            errorCode
                    ));
                }
            } else if (request.memberId() != null) {
                // v0-v2 single member leave
                group.members.remove(request.memberId());
            }

            // Trigger rebalance if members remain
            if (!group.members.isEmpty()) {
                group.state = GroupState.PREPARING_REBALANCE;
            } else {
                group.state = GroupState.EMPTY;
            }

            LOG.info("LeaveGroup for group={}, remaining members={}", groupId, group.members.size());

            return new LeaveGroupResponse(0, Errors.NONE.code(), memberResponses);
        }
    }

    /**
     * Handle OffsetCommit request.
     */
    public OffsetCommitResponse handleOffsetCommit(OffsetCommitRequest request) {
        String groupId = request.groupId();
        List<OffsetCommitResponse.OffsetCommitTopicResponse> topics = new ArrayList<>();

        for (OffsetCommitRequest.OffsetCommitTopic topic : request.topics()) {
            List<OffsetCommitResponse.OffsetCommitPartitionResponse> partitions = new ArrayList<>();

            for (OffsetCommitRequest.OffsetCommitPartition partition : topic.partitions()) {
                GroupTopicPartition key = new GroupTopicPartition(
                        groupId, topic.name(), partition.partitionIndex());

                offsets.put(key, new CommittedOffset(
                        partition.committedOffset(),
                        partition.committedLeaderEpoch(),
                        partition.committedMetadata(),
                        System.currentTimeMillis()
                ));

                partitions.add(new OffsetCommitResponse.OffsetCommitPartitionResponse(
                        partition.partitionIndex(),
                        Errors.NONE.code()
                ));
            }

            topics.add(new OffsetCommitResponse.OffsetCommitTopicResponse(topic.name(), partitions));
        }

        LOG.debug("OffsetCommit for group={}", groupId);

        return new OffsetCommitResponse(0, topics);
    }

    /**
     * Handle OffsetFetch request.
     */
    public OffsetFetchResponse handleOffsetFetch(OffsetFetchRequest request) {
        if (request.apiVersion() >= 8 && request.groups() != null) {
            // v8+ batch format
            List<OffsetFetchResponse.OffsetFetchGroupResponse> groups = new ArrayList<>();

            for (OffsetFetchRequest.OffsetFetchGroup group : request.groups()) {
                List<OffsetFetchResponse.OffsetFetchTopicResponse> topics = fetchOffsetsForGroup(
                        group.groupId(), group.topics());
                groups.add(new OffsetFetchResponse.OffsetFetchGroupResponse(
                        group.groupId(),
                        topics,
                        Errors.NONE.code()
                ));
            }

            return new OffsetFetchResponse(0, null, Errors.NONE.code(), groups);
        } else {
            // v0-v7 format
            List<OffsetFetchResponse.OffsetFetchTopicResponse> topics = fetchOffsetsForGroup(
                    request.groupId(), request.topics());

            return new OffsetFetchResponse(0, topics, Errors.NONE.code(), null);
        }
    }

    private List<OffsetFetchResponse.OffsetFetchTopicResponse> fetchOffsetsForGroup(
            String groupId, List<OffsetFetchRequest.OffsetFetchTopic> requestTopics) {

        List<OffsetFetchResponse.OffsetFetchTopicResponse> topics = new ArrayList<>();

        if (requestTopics == null) {
            // Return all offsets for the group
            Map<String, List<OffsetFetchResponse.OffsetFetchPartitionResponse>> byTopic = new HashMap<>();

            for (Map.Entry<GroupTopicPartition, CommittedOffset> entry : offsets.entrySet()) {
                if (entry.getKey().groupId.equals(groupId)) {
                    String topic = entry.getKey().topic;
                    CommittedOffset offset = entry.getValue();

                    byTopic.computeIfAbsent(topic, k -> new ArrayList<>())
                            .add(new OffsetFetchResponse.OffsetFetchPartitionResponse(
                                    entry.getKey().partition,
                                    offset.offset,
                                    offset.leaderEpoch,
                                    offset.metadata,
                                    Errors.NONE.code()
                            ));
                }
            }

            for (Map.Entry<String, List<OffsetFetchResponse.OffsetFetchPartitionResponse>> entry : byTopic.entrySet()) {
                topics.add(new OffsetFetchResponse.OffsetFetchTopicResponse(entry.getKey(), entry.getValue()));
            }
        } else {
            // Return requested offsets
            for (OffsetFetchRequest.OffsetFetchTopic topic : requestTopics) {
                List<OffsetFetchResponse.OffsetFetchPartitionResponse> partitions = new ArrayList<>();

                for (int partition : topic.partitionIndexes()) {
                    GroupTopicPartition key = new GroupTopicPartition(groupId, topic.name(), partition);
                    CommittedOffset offset = offsets.get(key);

                    if (offset != null) {
                        partitions.add(new OffsetFetchResponse.OffsetFetchPartitionResponse(
                                partition,
                                offset.offset,
                                offset.leaderEpoch,
                                offset.metadata,
                                Errors.NONE.code()
                        ));
                    } else {
                        partitions.add(new OffsetFetchResponse.OffsetFetchPartitionResponse(
                                partition,
                                -1L, // No committed offset
                                -1,
                                "",
                                Errors.NONE.code()
                        ));
                    }
                }

                topics.add(new OffsetFetchResponse.OffsetFetchTopicResponse(topic.name(), partitions));
            }
        }

        return topics;
    }

    private JoinGroupResponse errorJoinGroupResponse(Errors error) {
        return new JoinGroupResponse(0, error.code(), -1, null, null, "", false, "", List.of());
    }

    private String generateMemberId(String clientId) {
        String prefix = clientId != null ? clientId : "consumer";
        return prefix + "-" + UUID.randomUUID();
    }

    private Map<String, byte[]> extractProtocols(List<JoinGroupRequest.JoinGroupProtocol> protocols) {
        Map<String, byte[]> result = new HashMap<>();
        if (protocols != null) {
            for (JoinGroupRequest.JoinGroupProtocol protocol : protocols) {
                result.put(protocol.name(), protocol.metadata());
            }
        }
        return result;
    }

    private String selectProtocol(ConsumerGroup group) {
        // Find a protocol supported by all members
        Set<String> candidates = null;
        for (GroupMember member : group.members.values()) {
            if (candidates == null) {
                candidates = new HashSet<>(member.protocols.keySet());
            } else {
                candidates.retainAll(member.protocols.keySet());
            }
        }

        if (candidates == null || candidates.isEmpty()) {
            return "range"; // Default
        }

        // Prefer "range" or "roundrobin" if available
        if (candidates.contains("range")) return "range";
        if (candidates.contains("roundrobin")) return "roundrobin";

        return candidates.iterator().next();
    }

    // Internal classes

    private enum GroupState {
        EMPTY,
        PREPARING_REBALANCE,
        COMPLETING_REBALANCE,
        STABLE,
        DEAD
    }

    private static class ConsumerGroup {
        final String groupId;
        GroupState state = GroupState.EMPTY;
        int generationId = 0;
        String protocolType;
        String protocolName;
        String leader;
        final Map<String, GroupMember> members = new HashMap<>();
        final Set<String> pendingJoinMembers = new HashSet<>();
        final Map<String, byte[]> assignments = new HashMap<>();

        ConsumerGroup(String groupId) {
            this.groupId = groupId;
        }
    }

    private static class GroupMember {
        final String memberId;
        final String groupInstanceId;
        final int sessionTimeoutMs;
        final int rebalanceTimeoutMs;
        final String protocolType;
        final Map<String, byte[]> protocols;
        long lastHeartbeat;

        GroupMember(String memberId, String groupInstanceId, int sessionTimeoutMs,
                    int rebalanceTimeoutMs, String protocolType, Map<String, byte[]> protocols,
                    long lastHeartbeat) {
            this.memberId = memberId;
            this.groupInstanceId = groupInstanceId;
            this.sessionTimeoutMs = sessionTimeoutMs;
            this.rebalanceTimeoutMs = rebalanceTimeoutMs;
            this.protocolType = protocolType;
            this.protocols = protocols;
            this.lastHeartbeat = lastHeartbeat;
        }
    }

    private record GroupTopicPartition(String groupId, String topic, int partition) {}

    private record CommittedOffset(long offset, int leaderEpoch, String metadata, long commitTimestamp) {}
}

