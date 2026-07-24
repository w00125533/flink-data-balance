package com.fdb.job.metrics;

import java.io.Serializable;
import java.util.Arrays;

final class LatencyStats implements Serializable {
    private static final int DEFAULT_CAPACITY = 4096;
    private final long[] samples;
    private int size;
    private long seen;

    LatencyStats() {
        this(DEFAULT_CAPACITY);
    }

    LatencyStats(int capacity) {
        this.samples = new long[Math.max(1, capacity)];
    }

    void recordObservedAt(long observedAtMs, long eventTimeMs) {
        if (eventTimeMs == Long.MIN_VALUE || eventTimeMs == Long.MAX_VALUE || eventTimeMs < 0L) {
            return;
        }
        long latencyMs = Math.max(0L, observedAtMs - eventTimeMs);
        recordLatency(latencyMs);
    }

    Snapshot snapshotAndReset() {
        if (size == 0) {
            seen = 0L;
            return Snapshot.empty();
        }
        long[] copy = Arrays.copyOf(samples, size);
        Arrays.sort(copy);
        Snapshot snapshot = new Snapshot(
            percentile(copy, 50),
            percentile(copy, 95),
            percentile(copy, 99),
            copy[copy.length - 1]);
        size = 0;
        seen = 0L;
        return snapshot;
    }

    void recordLatency(long latencyMs) {
        seen++;
        if (size < samples.length) {
            samples[size++] = latencyMs;
            return;
        }
        int slot = Math.floorMod(seen * 1_103_515_245L + 12_345L, samples.length);
        samples[slot] = latencyMs;
    }

    private static long percentile(long[] sorted, int percentile) {
        if (sorted.length == 0) {
            return -1L;
        }
        int index = (int) Math.ceil((percentile / 100.0d) * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(sorted.length - 1, index))];
    }

    record Snapshot(long p50Ms, long p95Ms, long p99Ms, long maxMs) {
        static Snapshot empty() {
            return new Snapshot(-1L, -1L, -1L, -1L);
        }
    }
}
