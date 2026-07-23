package com.fdb.benchmark;

import java.util.HashMap;
import java.util.Map;

public final class DefaultBenchmarkClients implements BenchmarkClients {
  private static final String DEFAULT_PROBE_COMMAND_TIMEOUT_SEC = "20";

  private final BenchmarkConfig config;
  private final FlinkRestClient flink;
  private final ObservabilityClient observability;
  private final CommandRunner commandRunner;

  public DefaultBenchmarkClients(BenchmarkConfig config, HttpGateway http, CommandRunner commandRunner) {
    this.config = config;
    this.flink = new FlinkRestClient(config.flinkRestUrl(), http);
    this.observability = new ObservabilityClient(config.observabilityApiUrl(), http);
    this.commandRunner = commandRunner;
  }

  public DefaultBenchmarkClients(
      BenchmarkConfig config,
      HttpGateway http,
      ProcessCommandRunner commandRunner,
      Map<String, String> env) {
    this(config, http, command -> commandRunner.run(command, probeCommandEnv(env)));
  }

  @Override
  public RunObservation observe(BenchmarkRunPlan plan) throws Exception {
    TopologyMetricsSnapshot topology =
        TopologyMetricsSnapshot.read(TopologyMetricsSnapshot.metricsFile(config.outputRoot(), plan));
    SourceMetricsSnapshot source = SourceMetricsSnapshot.read(config.outputRoot(), plan);
    return new RunObservation(flink.snapshot(), observability.snapshot(), observeStorage(plan), topology, source);
  }

  @Override
  public StorageSnapshot observeStorage(BenchmarkRunPlan plan) throws Exception {
    StorageProbe storageProbe = StorageProbe.forSink(plan.sink(), commandRunner);
    return storageProbe.snapshot();
  }

  static Map<String, String> probeCommandEnv(Map<String, String> env) {
    Map<String, String> result = new HashMap<>(env);
    result.putIfAbsent("FDB_BENCHMARK_COMMAND_TIMEOUT_SEC",
        positiveOrDefault(result.get("FDB_BENCHMARK_PROBE_COMMAND_TIMEOUT_SEC"),
            positiveOrDefault(result.get("FDB_HDFS_PROBE_TIMEOUT_SEC"), DEFAULT_PROBE_COMMAND_TIMEOUT_SEC)));
    return Map.copyOf(result);
  }

  private static String positiveOrDefault(String raw, String fallback) {
    if (raw == null || raw.isBlank()) {
      return fallback;
    }
    try {
      long value = Long.parseLong(raw.trim());
      return value > 0 ? Long.toString(value) : fallback;
    } catch (NumberFormatException e) {
      return fallback;
    }
  }
}
