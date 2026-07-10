package com.tibco.tibrv;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-memory message bus backing the stub. Transports sharing the same service/network
 * ("bus key") see each other's messages; distributed-queue listeners sharing a CM name
 * receive round-robin (one member per message), plain listeners all receive a copy.
 * Everything is JVM-local: this simulates Rendezvous semantics for tests only.
 */
final class StubBus {

    private static final Map<String, CopyOnWriteArrayList<TibrvListener>> LISTENERS = new ConcurrentHashMap<>();
    private static final Map<String, AtomicInteger> DQ_ROUND_ROBIN = new ConcurrentHashMap<>();

    private StubBus() {
    }

    static void register(String busKey, TibrvListener listener) {
        LISTENERS.computeIfAbsent(busKey, key -> new CopyOnWriteArrayList<>()).add(listener);
    }

    static void unregister(String busKey, TibrvListener listener) {
        List<TibrvListener> listeners = LISTENERS.get(busKey);
        if (listeners != null) {
            listeners.remove(listener);
        }
    }

    static void send(String busKey, TibrvMsg message) throws TibrvException {
        String subject = message.getSendSubject();
        if (subject == null || subject.isEmpty()) {
            throw new TibrvException("send subject not set");
        }
        List<TibrvListener> listeners = LISTENERS.getOrDefault(busKey, new CopyOnWriteArrayList<>());
        Map<String, List<TibrvListener>> dqGroups = new ConcurrentHashMap<>();
        List<TibrvListener> plain = new ArrayList<>();
        for (TibrvListener listener : listeners) {
            if (!matches(listener.subjectPattern(), subject)) {
                continue;
            }
            if (listener.dqName() != null) {
                dqGroups.computeIfAbsent(listener.dqName(), key -> new ArrayList<>()).add(listener);
            } else {
                plain.add(listener);
            }
        }
        plain.forEach(listener -> listener.post(message.copy()));
        dqGroups.forEach((dqName, members) -> {
            int next = DQ_ROUND_ROBIN
                    .computeIfAbsent(busKey + "|" + dqName, key -> new AtomicInteger())
                    .getAndIncrement();
            members.get(next % members.size()).post(message.copy());
        });
    }

    // --- Fault-tolerance groups: highest-weight members up to activeGoal are active ---

    private static final Map<String, CopyOnWriteArrayList<TibrvFtMember>> FT_GROUPS = new ConcurrentHashMap<>();
    private static final java.util.Set<TibrvFtMember> FT_ACTIVE = ConcurrentHashMap.newKeySet();

    static synchronized void ftJoin(String busKey, TibrvFtMember member) {
        FT_GROUPS.computeIfAbsent(member.busGroupKey(), key -> new CopyOnWriteArrayList<>()).add(member);
        ftElect(FT_GROUPS.get(member.busGroupKey()));
    }

    static synchronized void ftLeave(TibrvFtMember member) {
        List<TibrvFtMember> members = FT_GROUPS.get(member.busGroupKey());
        if (members == null) {
            return;
        }
        members.remove(member);
        FT_ACTIVE.remove(member);
        ftElect(members);
    }

    private static void ftElect(List<TibrvFtMember> members) {
        if (members.isEmpty()) {
            return;
        }
        List<TibrvFtMember> ranked = new ArrayList<>(members);
        ranked.sort((left, right) -> Integer.compare(right.weight(), left.weight()));
        int activeGoal = ranked.stream().mapToInt(TibrvFtMember::activeGoal).max().orElse(1);
        for (int rank = 0; rank < ranked.size(); rank++) {
            TibrvFtMember member = ranked.get(rank);
            boolean shouldBeActive = rank < activeGoal;
            if (shouldBeActive && FT_ACTIVE.add(member)) {
                member.post(TibrvFtMember.ACTIVATE);
            } else if (!shouldBeActive && FT_ACTIVE.remove(member)) {
                member.post(TibrvFtMember.DEACTIVATE);
            }
        }
    }

    /** Rendezvous subject matching: '*' matches exactly one element, '>' the rest (one or more). */
    static boolean matches(String pattern, String subject) {
        String[] patternElements = pattern.split("\\.", -1);
        String[] subjectElements = subject.split("\\.", -1);
        for (int i = 0; i < patternElements.length; i++) {
            if (patternElements[i].equals(">")) {
                return subjectElements.length > i;
            }
            if (i >= subjectElements.length) {
                return false;
            }
            if (!patternElements[i].equals("*") && !patternElements[i].equals(subjectElements[i])) {
                return false;
            }
        }
        return patternElements.length == subjectElements.length;
    }
}
