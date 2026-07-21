package com.tibco.tibrv;

/**
 * Stub of {@code com.tibco.tibrv.TibrvFtMember}: membership in a fault-tolerance
 * group. The stub elects the highest-weight members up to {@code activeGoal} as
 * active (ACTIVATE) and demotes the rest (DEACTIVATE); elections re-run whenever a
 * member joins or leaves, which is enough to simulate failover within one JVM.
 * Heartbeat/preparation/activation intervals are accepted but not simulated.
 */
public class TibrvFtMember {

    public static final int PREPARE_TO_ACTIVATE = 1;
    public static final int ACTIVATE = 2;
    public static final int DEACTIVATE = 3;

    private final TibrvQueue queue;
    private final TibrvFtMemberCallback callback;
    private final String busKey;
    private final String groupName;
    private final int weight;
    private final int activeGoal;

    public TibrvFtMember(TibrvQueue queue, TibrvFtMemberCallback callback, TibrvTransport transport,
                         String groupName, int weight, int activeGoal,
                         double heartbeatInterval, double preparationInterval, double activationInterval,
                         Object closure) throws TibrvException {
        this.queue = queue;
        this.callback = callback;
        this.busKey = transport.busKey();
        this.groupName = groupName;
        this.weight = weight;
        this.activeGoal = activeGoal;
        StubBus.ftJoin(busKey, this);
    }

    void post(int action) {
        queue.post(() -> callback.onFtAction(this, groupName, action));
    }

    String busGroupKey() {
        return busKey + "|" + groupName;
    }

    int weight() {
        return weight;
    }

    int activeGoal() {
        return activeGoal;
    }

    public String getGroupName() {
        return groupName;
    }

    public int getWeight() {
        return weight;
    }

    public void destroy() {
        StubBus.ftLeave(this);
    }
}
