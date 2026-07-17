package com.fdb.simulator;

final class AnomalyInjection {
    private AnomalyInjection() {
    }

    static boolean inAnomalyCohort(String id, double ratio) {
        if (id == null || id.isBlank() || ratio <= 0.0d) {
            return false;
        }
        if (ratio >= 1.0d) {
            return true;
        }
        int bucket = Math.floorMod(id.hashCode(), 10_000);
        return bucket < Math.round(ratio * 10_000);
    }
}
