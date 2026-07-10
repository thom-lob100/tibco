package com.tibco.tibrv;

/** Stub of {@code com.tibco.tibrv.TibrvFtCallback} (fault-tolerance action callback). */
public interface TibrvFtCallback {

    void onFtAction(TibrvFtMember member, String ftgroupName, int action);
}
