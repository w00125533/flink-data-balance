package com.fdb.simulator;

final class SimulationRuntime {
    private SimulationRuntime() {
    }

    static boolean shouldContinue(long startMs, long nowMs, long durationSec) {
        return durationSec <= 0 || nowMs - startMs < durationSec * 1000L;
    }

    static long metricDurationMs(long startMs, long nowMs, long durationSec) {
        long elapsed = Math.max(0L, nowMs - startMs);
        if (durationSec <= 0) {
            return elapsed;
        }
        return Math.min(elapsed, durationSec * 1000L);
    }
}
