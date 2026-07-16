package com.fdb.benchmark;

public interface DeployCommandClient {
  void submit(BenchmarkRunPlan plan) throws Exception;

  void stop(BenchmarkRunPlan plan) throws Exception;
}
