package com.fdb.job.metrics;

final class TestNanoClock implements ConnectorSinkMetrics.NanoClock {
    private long nowNanos;

    @Override
    public long nanoTime() {
        return nowNanos;
    }

    void advanceMillis(long millis) {
        nowNanos += millis * 1_000_000L;
    }
}
