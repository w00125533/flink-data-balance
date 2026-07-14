package com.fdb.job.balance.coordinator;

import java.io.Serializable;
import java.util.Arrays;

public class HeartbeatPayload implements Serializable {

    private int subtaskId;
    private double eps;
    private double[] vbucketEps;
    private long timestamp;
    private String hotspotSiteId;
    private int hotspotVbucketId = -1;

    public HeartbeatPayload() {}

    public HeartbeatPayload(int subtaskId, double eps, double[] vbucketEps, long timestamp) {
        this.subtaskId = subtaskId;
        this.eps = eps;
        this.vbucketEps = vbucketEps;
        this.timestamp = timestamp;
    }

    public HeartbeatPayload(int subtaskId, double eps, double[] vbucketEps, long timestamp,
                            String hotspotSiteId, int hotspotVbucketId) {
        this(subtaskId, eps, vbucketEps, timestamp);
        this.hotspotSiteId = hotspotSiteId;
        this.hotspotVbucketId = hotspotVbucketId;
    }

    public int getSubtaskId() { return subtaskId; }
    public void setSubtaskId(int id) { this.subtaskId = id; }

    public double getEps() { return eps; }
    public void setEps(double eps) { this.eps = eps; }

    public double[] getVbucketEps() { return vbucketEps; }
    public void setVbucketEps(double[] vbucketEps) { this.vbucketEps = vbucketEps; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long ts) { this.timestamp = ts; }
    public String getHotspotSiteId() { return hotspotSiteId; }
    public int getHotspotVbucketId() { return hotspotVbucketId; }

    @Override
    public String toString() {
        return "Heartbeat{subtask=" + subtaskId + ", eps=" + eps + ", ts=" + timestamp + "}";
    }

    public String toCsv() {
        StringBuilder sb = new StringBuilder();
        sb.append(subtaskId).append(',').append(eps).append(',').append(timestamp);
        if (vbucketEps != null) {
            for (double v : vbucketEps) {
                sb.append(',').append((long) v);
            }
        }
        if (hotspotSiteId != null) sb.append(",site=").append(hotspotSiteId).append(",vb=").append(hotspotVbucketId);
        return sb.toString();
    }

    public static HeartbeatPayload fromCsv(String line) {
        String[] parts = line.split(",");
        HeartbeatPayload hb = new HeartbeatPayload();
        hb.subtaskId = Integer.parseInt(parts[0]);
        hb.eps = Double.parseDouble(parts[1]);
        hb.timestamp = Long.parseLong(parts[2]);
        int numericEnd = parts.length;
        for (int i = 3; i < parts.length; i++) {
            if (parts[i].startsWith("site=")) {
                hb.hotspotSiteId = parts[i].substring("site=".length());
                if (i + 1 < parts.length && parts[i + 1].startsWith("vb=")) {
                    hb.hotspotVbucketId = Integer.parseInt(parts[i + 1].substring("vb=".length()));
                }
                numericEnd = i;
                break;
            }
        }
        if (numericEnd > 3) {
            hb.vbucketEps = new double[numericEnd - 3];
            for (int i = 3; i < numericEnd; i++) {
                hb.vbucketEps[i - 3] = Long.parseLong(parts[i]);
            }
        }
        return hb;
    }
}
