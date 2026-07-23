package com.fdb.benchmark;

public interface BenchmarkClients {
  RunObservation observe(BenchmarkRunPlan plan) throws Exception;

  default StorageSnapshot observeStorage(BenchmarkRunPlan plan) throws Exception {
    return observe(plan).storage();
  }
}
