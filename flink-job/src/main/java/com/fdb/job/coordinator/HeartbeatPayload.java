package com.fdb.job.coordinator;

import java.io.Serializable;
import java.util.Arrays;

public class HeartbeatPayload implements Serializable {

    private int subtaskId;
    private double eps;
    private double[] vbucketEps;
    private long timestamp;

    public HeartbeatPayload() {}

    public HeartbeatPayload(int subtaskId, double eps, double[] vbucketEps, long timestamp) {
        this.subtaskId = subtaskId;
        this.eps = eps;
        this.vbucketEps = vbucketEps;
        this.timestamp = timestamp;
    }

    public int getSubtaskId() { return subtaskId; }
    public void setSubtaskId(int id) { this.subtaskId = id; }

    public double getEps() { return eps; }
    public void setEps(double eps) { this.eps = eps; }

    public double[] getVbucketEps() { return vbucketEps; }
    public void setVbucketEps(double[] vbucketEps) { this.vbucketEps = vbucketEps; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long ts) { this.timestamp = ts; }

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
        return sb.toString();
    }

    public static HeartbeatPayload fromCsv(String line) {
        String[] parts = line.split(",");
        HeartbeatPayload hb = new HeartbeatPayload();
        hb.subtaskId = Integer.parseInt(parts[0]);
        hb.eps = Double.parseDouble(parts[1]);
        hb.timestamp = Long.parseLong(parts[2]);
        if (parts.length > 3) {
            hb.vbucketEps = new double[parts.length - 3];
            for (int i = 3; i < parts.length; i++) {
                hb.vbucketEps[i - 3] = Long.parseLong(parts[i]);
            }
        }
        return hb;
    }
}
