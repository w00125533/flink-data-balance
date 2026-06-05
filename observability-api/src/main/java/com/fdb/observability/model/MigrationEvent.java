package com.fdb.observability.model;

import java.util.List;

public record MigrationEvent(
    String migrationId,
    int routingVersionBefore,
    int routingVersionAfter,
    String reason,
    String status,
    String startedAt,
    String completedAt,
    List<MovedVBucket> movedVbuckets) {

  public record MovedVBucket(
      int vbucket,
      int fromSubtask,
      int toSubtask,
      double estimatedEps,
      List<String> hotKeys) {
  }
}
