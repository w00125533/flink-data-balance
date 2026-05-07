package com.fdb.job.coordinator;

import java.io.Serializable;

public class RoutingEntry implements Serializable {

    private String siteId;
    private int vbucketId;
    private int slotShift;
    private int assignedSubtask;
    private long routingVersion;
    private long decisionTs;

    public RoutingEntry() {}

    public RoutingEntry(String siteId, int vbucketId, int slotShift,
                        int assignedSubtask, long routingVersion, long decisionTs) {
        this.siteId = siteId;
        this.vbucketId = vbucketId;
        this.slotShift = slotShift;
        this.assignedSubtask = assignedSubtask;
        this.routingVersion = routingVersion;
        this.decisionTs = decisionTs;
    }

    public String getSiteId() { return siteId; }
    public void setSiteId(String s) { this.siteId = s; }

    public int getVbucketId() { return vbucketId; }
    public void setVbucketId(int v) { this.vbucketId = v; }

    public int getSlotShift() { return slotShift; }
    public void setSlotShift(int s) { this.slotShift = s; }

    public int getAssignedSubtask() { return assignedSubtask; }
    public void setAssignedSubtask(int s) { this.assignedSubtask = s; }

    public long getRoutingVersion() { return routingVersion; }
    public void setRoutingVersion(long v) { this.routingVersion = v; }

    public long getDecisionTs() { return decisionTs; }
    public void setDecisionTs(long ts) { this.decisionTs = ts; }

    public String toCsvHeader() {
        return "siteId,vbucketId,slotShift,assignedSubtask,routingVersion,decisionTs";
    }

    public String toCsv() {
        return String.format("%s,%d,%d,%d,%d,%d",
            siteId, vbucketId, slotShift, assignedSubtask, routingVersion, decisionTs);
    }

    public static RoutingEntry fromCsv(String line) {
        String[] parts = line.split(",");
        return new RoutingEntry(
            parts[0],                     // siteId
            Integer.parseInt(parts[1]),   // vbucketId
            Integer.parseInt(parts[2]),   // slotShift
            Integer.parseInt(parts[3]),   // assignedSubtask
            Long.parseLong(parts[4]),     // routingVersion
            Long.parseLong(parts[5])      // decisionTs
        );
    }

    @Override
    public String toString() {
        return "RoutingEntry{site=" + siteId + ", vb=" + vbucketId
            + ", shift=" + slotShift + ", subtask=" + assignedSubtask + "}";
    }
}
