package com.tibco.tibrv;

/** Exact TIBCO Rendezvous FT callback signature used by {@link TibrvFtMember}. */
public interface TibrvFtMemberCallback {

    void onFtAction(TibrvFtMember member, String ftgroupName, int action);
}
