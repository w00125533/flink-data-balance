package com.fdb.benchmark;

import java.util.List;

public final class KafkaStorageProbe implements StorageProbe {
  private final CommandRunner commandRunner;

  public KafkaStorageProbe(CommandRunner commandRunner) {
    this.commandRunner = commandRunner;
  }

  @Override
  public StorageSnapshot snapshot() throws Exception {
    CommandResult result = commandRunner.run(List.of("bash", "-lc",
        "kafka-consumer-groups --bootstrap-server ${FDB_KAFKA_BOOTSTRAP:-localhost:9092} --describe --all-groups"));
    return snapshotFromCommand("kafka offsets", result);
  }

  static StorageSnapshot snapshotFromCommand(String label, CommandResult result) {
    String summary = result.success() ? label + " healthy" : nonBlank(result.stderr(), result.stdout());
    return new StorageSnapshot(result.success(), summary, parseFirstLong(result.stdout()), 0, 0);
  }

  private static String nonBlank(String preferred, String fallback) {
    return preferred == null || preferred.isBlank() ? fallback : preferred;
  }

  private static long parseFirstLong(String value) {
    if (value == null) {
      return 0;
    }
    for (String token : value.split("\\s+")) {
      try {
        return Long.parseLong(token);
      } catch (NumberFormatException ignored) {
        // Keep scanning.
      }
    }
    return 0;
  }
}
