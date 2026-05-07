package com.fdb.job.coordinator;

import java.util.*;
import java.util.stream.Collectors;

public class RebalancePolicy {

    private final double overloadThreshold;
    private final long overloadDurationMs;
    private final int topHotspotsPerSubtask;
    private final int numVBuckets;

    public RebalancePolicy() {
        this.overloadThreshold = 1.5;
        this.overloadDurationMs = 60_000;
        this.topHotspotsPerSubtask = 3;
        this.numVBuckets = 1024;
    }

    public RebalancePolicy(double overloadThreshold, long overloadDurationMs,
                           int topHotspotsPerSubtask, int numVBuckets) {
        this.overloadThreshold = overloadThreshold;
        this.overloadDurationMs = overloadDurationMs;
        this.topHotspotsPerSubtask = topHotspotsPerSubtask;
        this.numVBuckets = numVBuckets;
    }

    public record HotspotSite(String siteId, int vbucketId, double eps) {}

    public record RebalanceDecision(
        HotspotSite site,
        int currentSubtask,
        int targetSubtask,
        int newSlotShift
    ) {}

    public List<RebalanceDecision> evaluate(
            Map<Integer, HeartbeatPayload> heartbeats,
            Map<String, Long> overloadStartTime,
            long now,
            long routingVersion) {

        List<RebalanceDecision> decisions = new ArrayList<>();
        if (heartbeats.isEmpty()) return decisions;

        int numSubtasks = heartbeats.size();
        double[] epsValues = heartbeats.values().stream()
            .mapToDouble(HeartbeatPayload::getEps)
            .sorted()
            .toArray();

        double medianEps = median(epsValues);
        double overloadLine = medianEps * overloadThreshold;

        List<Integer> overloadedSubtasks = heartbeats.entrySet().stream()
            .filter(e -> e.getValue().getEps() > overloadLine)
            .map(Map.Entry::getKey)
            .sorted()
            .toList();

        if (overloadedSubtasks.isEmpty()) return decisions;

        List<Integer> idleSubtasks = heartbeats.entrySet().stream()
            .filter(e -> e.getValue().getEps() <= medianEps * 0.8)
            .map(Map.Entry::getKey)
            .sorted()
            .collect(Collectors.toList());

        if (idleSubtasks.isEmpty()) return decisions;

        for (int subtaskId : overloadedSubtasks) {
            Long since = overloadStartTime.get(String.valueOf(subtaskId));
            if (since == null || (now - since) < overloadDurationMs) continue;

            HeartbeatPayload hb = heartbeats.get(subtaskId);
            if (hb.getVbucketEps() == null) continue;

            List<HotspotSite> hotspots = findHotspots(hb, subtaskId);

            for (HotspotSite site : hotspots) {
                if (idleSubtasks.isEmpty()) break;
                int target = idleSubtasks.get(0);
                idleSubtasks.remove(0);

                int newSlotShift = computeSlotShift(site.vbucketId(), subtaskId, target);
                decisions.add(new RebalanceDecision(site, subtaskId, target, newSlotShift));
            }
        }

        return decisions;
    }

    public double median(double[] sorted) {
        int n = sorted.length;
        if (n == 0) return 0;
        if (n % 2 == 0) {
            return (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0;
        }
        return sorted[n / 2];
    }

    private List<HotspotSite> findHotspots(HeartbeatPayload hb, int subtaskId) {
        if (hb.getVbucketEps() == null) return List.of();

        double[] eps = hb.getVbucketEps();
        int numVBuckets = eps.length;
        int subtaskVbCount = numVBuckets / Math.max(1, hb.getEps() > 0 ? heartbeatsCount() : 1);

        List<HotspotSite> candidates = new ArrayList<>();
        for (int vb = 0; vb < eps.length; vb++) {
            if (eps[vb] > 0) {
                candidates.add(new HotspotSite("VB-" + vb, vb, eps[vb]));
            }
        }

        candidates.sort((a, b) -> Double.compare(b.eps(), a.eps()));
        return candidates.stream().limit(topHotspotsPerSubtask).collect(Collectors.toList());
    }

    private int heartbeatsCount() { return 4; }

    private int computeSlotShift(int vbucketId, int currentSubtask, int targetSubtask) {
        int xor = currentSubtask ^ targetSubtask;
        return xor & (numVBuckets - 1);
    }
}
