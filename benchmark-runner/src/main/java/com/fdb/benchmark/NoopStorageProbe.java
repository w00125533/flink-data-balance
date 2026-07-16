package com.fdb.benchmark;

public final class NoopStorageProbe implements StorageProbe {
  @Override
  public StorageSnapshot snapshot() {
    return new StorageSnapshot(true, "no business sink", 0, 0, 0);
  }
}
