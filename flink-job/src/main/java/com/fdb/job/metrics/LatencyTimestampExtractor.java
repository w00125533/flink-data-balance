package com.fdb.job.metrics;

import java.io.Serializable;

@FunctionalInterface
public interface LatencyTimestampExtractor<T> extends Serializable {
    long extractTimestampMs(T value);
}
