package com.fdb.benchmark;

import java.util.Arrays;
import java.util.Locale;

public enum BenchmarkSink {
  NONE("none"),
  STARROCKS("starrocks"),
  KAFKA("kafka"),
  HIVE("hive"),
  ICEBERG("iceberg");

  private final String value;

  BenchmarkSink(String value) {
    this.value = value;
  }

  public static BenchmarkSink parse(String raw) {
    String normalized = raw == null ? "" : raw.trim().toLowerCase(Locale.ROOT);
    return Arrays.stream(values())
        .filter(sink -> sink.value.equals(normalized))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("unsupported benchmark sink: " + raw));
  }

  public String value() {
    return value;
  }
}
