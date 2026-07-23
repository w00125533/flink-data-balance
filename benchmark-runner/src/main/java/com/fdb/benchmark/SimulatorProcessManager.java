package com.fdb.benchmark;

public interface SimulatorProcessManager {
  void start(BenchmarkRunPlan plan) throws Exception;

  void awaitCompletion(long timeoutSec) throws Exception;

  void stop() throws Exception;
}
