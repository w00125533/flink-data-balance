package com.fdb.benchmark;

public interface DeployCommandClient {
  void prepare(BenchmarkRunPlan plan) throws Exception;

  void submit(BenchmarkRunPlan plan) throws Exception;

  void stop(BenchmarkRunPlan plan) throws Exception;
}
