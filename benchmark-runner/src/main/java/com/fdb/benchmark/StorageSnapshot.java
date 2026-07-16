package com.fdb.benchmark;

public record StorageSnapshot(boolean healthy, String summary, long records, long smallFiles, long inProgressFiles) {
}
