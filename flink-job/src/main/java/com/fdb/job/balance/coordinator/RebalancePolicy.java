package com.fdb.job.balance.coordinator;

import com.fdb.common.hash.Hashes;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import java.util.*;
import java.util.stream.Collectors;

public class RebalancePolicy {

    private final double overloadThreshold;
    private final long overloadDurationMs;
    private final int topHotspotsPerSubtask;
    private final int numVBuckets;
    private final Integer maxParallelism;

    public RebalancePolicy() {
        this(1.5, 60_000, 3, 1024);
    }

    public RebalancePolicy(double overloadThreshold, long overloadDurationMs,
                           int topHotspotsPerSubtask, int numVBuckets) {
        this(overloadThreshold, overloadDurationMs, topHotspotsPerSubtask, numVBuckets, null);
    }

    public RebalancePolicy(double overloadThreshold, long overloadDurationMs,
                           int topHotspotsPerSubtask, int numVBuckets, Integer maxParallelism) {
        this.overloadThreshold = overloadThreshold;
        this.overloadDurationMs = overloadDurationMs;
        this.topHotspotsPerSubtask = topHotspotsPerSubtask;
        this.numVBuckets = numVBuckets;
        this.maxParallelism = maxParallelism;
    }

    public record HotspotSite(String siteId, int vbucketId, double eps) {
        public String routeKey() {
            return siteId;
        }
    }

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

                int newSlotShift = computeSlotShift(site.routeKey(), target, numSubtasks);
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
        if (hb.getHotspotSiteId() != null && hb.getHotspotVbucketId() >= 0) {
            double eps = hb.getHotspotVbucketId() < hb.getVbucketEps().length
                ? hb.getVbucketEps()[hb.getHotspotVbucketId()] : hb.getEps();
            return List.of(new HotspotSite(hb.getHotspotSiteId(), hb.getHotspotVbucketId(), eps));
        }

        double[] eps = hb.getVbucketEps();

        List<HotspotSite> candidates = new ArrayList<>();
        for (int vb = 0; vb < eps.length; vb++) {
            if (eps[vb] > 0) {
                candidates.add(new HotspotSite("VB-" + vb, vb, eps[vb]));
            }
        }

        candidates.sort((a, b) -> Double.compare(b.eps(), a.eps()));
        return candidates.stream().limit(topHotspotsPerSubtask).collect(Collectors.toList());
    }

    private int computeSlotShift(String routeKey, int targetSubtask, int parallelism) {
        int effectiveMaxParallelism = effectiveMaxParallelism(parallelism);
        for (int shift = 0; shift < numVBuckets; shift++) {
            int candidateVbucket = Hashes.toVBucketWithShift(routeKey, numVBuckets, shift);
            int assignedSubtask = KeyGroupRangeAssignment.assignKeyToParallelOperator(
                candidateVbucket, effectiveMaxParallelism, parallelism);
            if (assignedSubtask == targetSubtask) {
                return shift;
            }
        }
        throw new IllegalStateException("No slot shift found for routeKey=" + routeKey
            + ", targetSubtask=" + targetSubtask + ", parallelism=" + parallelism
            + ", maxParallelism=" + effectiveMaxParallelism);
    }

    private int effectiveMaxParallelism(int parallelism) {
        if (maxParallelism != null) {
            return maxParallelism;
        }
        return KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);
    }
}
