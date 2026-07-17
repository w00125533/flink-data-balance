package com.fdb.topology;

import com.fdb.common.avro.TopologyRecord;
import java.util.List;

public record TopologyMetrics(
    long generatedRecords,
    long siteCount,
    long frequencyBandCount,
    long generationDurationMs,
    long publishDurationMs,
    long totalDurationMs,
    long publishedRecords,
    long publishFailures,
    double minLatitude,
    double maxLatitude,
    double minLongitude,
    double maxLongitude) {

  public static TopologyMetrics from(
      List<TopologyRecord> records,
      long generationDurationMs,
      KafkaTopologyPublisher.PublishResult publishResult,
      long publishDurationMs,
      long totalDurationMs) {
    long siteCount = records.stream().map(record -> record.getSiteId().toString()).distinct().count();
    long bandCount = records.stream().map(record -> record.getFrequencyBand().toString()).distinct().count();
    double minLatitude = records.stream().mapToDouble(TopologyRecord::getSiteLat).min().orElse(0.0);
    double maxLatitude = records.stream().mapToDouble(TopologyRecord::getSiteLat).max().orElse(0.0);
    double minLongitude = records.stream().mapToDouble(TopologyRecord::getSiteLon).min().orElse(0.0);
    double maxLongitude = records.stream().mapToDouble(TopologyRecord::getSiteLon).max().orElse(0.0);
    return new TopologyMetrics(
        records.size(),
        siteCount,
        bandCount,
        generationDurationMs,
        publishDurationMs,
        totalDurationMs,
        publishResult.publishedRecords(),
        publishResult.failedRecords(),
        minLatitude,
        maxLatitude,
        minLongitude,
        maxLongitude);
  }
}
