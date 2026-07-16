package com.fdb.benchmark;

public interface SimulatorProcessManager {
  void start(BenchmarkRunPlan plan) throws Exception;

  void stop() throws Exception;
}
