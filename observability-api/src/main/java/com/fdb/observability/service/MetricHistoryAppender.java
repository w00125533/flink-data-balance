package com.fdb.observability.service;

import com.fdb.common.metrics.StageMetricSample;

@FunctionalInterface
public interface MetricHistoryAppender {
  void append(StageMetricSample sample);
}
