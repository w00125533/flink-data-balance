package com.fdb.benchmark;

import java.util.Locale;

public record SinkLatencySnapshot(
    String sinkName,
    String scope,
    String sinkType,
    String dataset,
    String windowKind,
    long records,
    long bytes,
    long latencyP50Ms,
    long latencyP95Ms,
    long latencyP99Ms,
    long failures) {
  public SinkLatencySnapshot(
      String sinkName,
      String sinkType,
      String dataset,
      String windowKind,
      long records,
      long bytes,
      long latencyP50Ms,
      long latencyP95Ms,
      long latencyP99Ms,
      long failures) {
    this(sinkName, inferScope(sinkName), sinkType, dataset, windowKind, records, bytes, latencyP50Ms, latencyP95Ms,
        latencyP99Ms, failures);
  }

  public SinkLatencySnapshot(
      String sinkName,
      long records,
      long bytes,
      long latencyP50Ms,
      long latencyP95Ms,
      long latencyP99Ms,
      long failures) {
    this(sinkName, "", "", "", records, bytes, latencyP50Ms, latencyP95Ms, latencyP99Ms, failures);
  }

  public SinkLatencySnapshot {
    sinkName = blankToDefault(sinkName, "");
    scope = blankToDefault(scope, inferScope(sinkName));
    sinkType = blankToDefault(sinkType, "");
    dataset = blankToDefault(dataset, "");
    windowKind = blankToDefault(windowKind, "");
  }

  public static String inferScope(String sinkName) {
    String value = sinkName == null ? "" : sinkName.toLowerCase(Locale.ROOT);
    if (value.endsWith(".connector-write") || value.contains(".connector-write.")) {
      return "connector-write";
    }
    if (value.endsWith(".connector-commit") || value.contains(".connector-commit.")) {
      return "connector-commit";
    }
    return "sink-front";
  }

  private static String blankToDefault(String value, String defaultValue) {
    return value == null || value.isBlank() ? defaultValue : value;
  }
}
