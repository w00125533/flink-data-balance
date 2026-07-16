package com.fdb.benchmark;

public interface BenchmarkClients {
  RunObservation observe(BenchmarkRunPlan plan) throws Exception;
}
