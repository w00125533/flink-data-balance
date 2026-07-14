package com.fdb.common.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
    String sinkType,
    String dataset,
    String windowKind,
    long records,
    long bytes,
    long durationMs,
    long latencyP50Ms,
    long latencyP99Ms,
    long failureCount,
    String errorMessage,
    long checkpointId,
    String runId,
    String resultSink,
    int parallelism,
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
        sinkType = blankToDefault(sinkType, "");
        dataset = blankToDefault(dataset, "");
        windowKind = blankToDefault(windowKind, "");
        if (records == 0L && rowsWritten > 0L) {
            records = rowsWritten;
        }
        errorMessage = blankToDefault(errorMessage, "");
        runId = blankToDefault(runId, "unknown-run");
        resultSink = blankToDefault(resultSink, "");
    }

    public static StageMetricSample stage(String stageId, String displayName, String status,
                                          double inEps, double outEps, long latencyP95Ms,
                                          long watermarkLagMs, long errorCount,
                                          long updatedAtEpochMs) {
        return new StageMetricSample(stageId, displayName, status, inEps, outEps, latencyP95Ms,
            watermarkLagMs, errorCount, 0L, 0L, sourceName(stageId), "", "", "", "", "", 0L, 0L, 0L,
            0L, 0L, 0L, "", -1L, "unknown-run", "", -1, updatedAtEpochMs);
    }

    public static StageMetricSample sink(String stageId, String displayName, String status,
                                         String sink, String window, long rowsWritten,
                                         long latencyP95Ms, long updatedAtEpochMs) {
        return sinkLatency(stageId, displayName, status, sink, "", window, rowsWritten, 0L, 0L,
            0L, latencyP95Ms, 0L, 0L, "", -1L, updatedAtEpochMs);
    }

    public static StageMetricSample sinkLatency(String stageId, String displayName, String status,
                                                String sinkType, String dataset, String windowKind,
                                                long records, long bytes, long durationMs,
                                                long latencyP50Ms, long latencyP95Ms,
                                                long latencyP99Ms, long failureCount,
                                                String errorMessage, long checkpointId,
                                                long updatedAtEpochMs) {
        return new StageMetricSample(stageId, displayName, status, records, records, latencyP95Ms,
            0L, failureCount, records, 0L, "", sinkType, windowLabel(windowKind), sinkType, dataset,
            windowKind, records, bytes, durationMs, latencyP50Ms, latencyP99Ms, failureCount,
            errorMessage, checkpointId, "unknown-run", "", -1, updatedAtEpochMs);
    }

    public StageMetricSample withRebalanceTotal(long rebalanceTotal) {
        return new StageMetricSample(stageId, displayName, status, inEps, outEps, latencyP95Ms,
            watermarkLagMs, errorCount, rowsWritten, rebalanceTotal, source, sink, window, sinkType,
            dataset, windowKind, records, bytes, durationMs, latencyP50Ms, latencyP99Ms,
            failureCount, errorMessage, checkpointId, runId, resultSink, parallelism, updatedAtEpochMs);
    }

    public StageMetricSample withRunMetadata(String runId, String resultSink, int parallelism) {
        return new StageMetricSample(stageId, displayName, status, inEps, outEps, latencyP95Ms,
            watermarkLagMs, errorCount, rowsWritten, rebalanceTotal, source, sink, window, sinkType,
            dataset, windowKind, records, bytes, durationMs, latencyP50Ms, latencyP99Ms,
            failureCount, errorMessage, checkpointId, runId, resultSink, parallelism, updatedAtEpochMs);
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
            JsonNode node = JSON.readTree(json);
            if (node instanceof ObjectNode object && !object.hasNonNull("checkpointId")) {
                object.put("checkpointId", -1L);
            }
            if (node instanceof ObjectNode object && !object.hasNonNull("runId")) {
                object.put("runId", "unknown-run");
            }
            if (node instanceof ObjectNode object && !object.hasNonNull("resultSink")) {
                object.put("resultSink", "");
            }
            if (node instanceof ObjectNode object && !object.hasNonNull("parallelism")) {
                object.put("parallelism", -1);
            }
            return JSON.treeToValue(node, StageMetricSample.class);
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

    private static String windowLabel(String windowKind) {
        return switch (blankToDefault(windowKind, "")) {
            case "MIN_1" -> "1m";
            case "MIN_5" -> "5m";
            case "MIN_15" -> "15m";
            case "HOUR_1" -> "1h";
            default -> blankToDefault(windowKind, "");
        };
    }

    private static String blankToDefault(String value, String defaultValue) {
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
