package com.fdb.benchmark;

import java.util.Map;

public final class DefaultBenchmarkClients implements BenchmarkClients {
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
    this(config, http, command -> commandRunner.run(command, env));
  }

  @Override
  public RunObservation observe(BenchmarkRunPlan plan) throws Exception {
    StorageProbe storageProbe = StorageProbe.forSink(plan.sink(), commandRunner);
    return new RunObservation(flink.snapshot(), observability.snapshot(), storageProbe.snapshot());
  }
}
