package com.fdb.common.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;

public record StageMetricSample(
    String stageId,
    String displayName,
    String status,
    double inEps,
    double outEps,
    long latencyP95Ms,
    long watermarkLagMs,
    long errorCount,
    long rowsWritten,
    long rebalanceTotal,
    String source,
    String sink,
    String window,
    long updatedAtEpochMs
) {
    private static final ObjectMapper JSON = new ObjectMapper();

    public StageMetricSample {
        stageId = blankToDefault(stageId, "unknown");
        displayName = blankToDefault(displayName, stageId);
        status = blankToDefault(status, "healthy");
        source = blankToDefault(source, "");
        sink = blankToDefault(sink, "");
        window = blankToDefault(window, "");
    }

    public static StageMetricSample stage(String stageId, String displayName, String status,
                                          double inEps, double outEps, long latencyP95Ms,
                                          long watermarkLagMs, long errorCount,
                                          long updatedAtEpochMs) {
        return new StageMetricSample(stageId, displayName, status, inEps, outEps, latencyP95Ms,
            watermarkLagMs, errorCount, 0L, 0L, sourceName(stageId), "", "", updatedAtEpochMs);
    }

    public static StageMetricSample sink(String stageId, String displayName, String status,
                                         String sink, String window, long rowsWritten,
                                         long latencyP95Ms, long updatedAtEpochMs) {
        return new StageMetricSample(stageId, displayName, status, rowsWritten, rowsWritten,
            latencyP95Ms, 0L, 0L, rowsWritten, 0L, "", sink, window, updatedAtEpochMs);
    }

    public StageMetricSample withRebalanceTotal(long rebalanceTotal) {
        return new StageMetricSample(stageId, displayName, status, inEps, outEps, latencyP95Ms,
            watermarkLagMs, errorCount, rowsWritten, rebalanceTotal, source, sink, window, updatedAtEpochMs);
    }

    public String toJson() {
        try {
            return JSON.writeValueAsString(this);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static StageMetricSample fromJson(String json) {
        try {
            return JSON.readValue(json, StageMetricSample.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String sourceName(String stageId) {
        return switch (stageId) {
            case "chr-source" -> "chr";
            case "pm-source" -> "pm";
            case "cfg-source" -> "cfg";
            default -> "";
        };
    }

    private static String blankToDefault(String value, String defaultValue) {
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
