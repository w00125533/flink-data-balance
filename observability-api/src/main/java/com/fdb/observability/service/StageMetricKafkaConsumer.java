package com.fdb.observability.service;

import com.fdb.common.metrics.StageMetricSample;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public final class StageMetricKafkaConsumer implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(StageMetricKafkaConsumer.class);

  private final KafkaConsumer<String, String> consumer;
  private final ObservabilitySnapshotService service;
  private final Thread thread;
  private volatile boolean running = true;

  public StageMetricKafkaConsumer(String bootstrap, String topic, ObservabilitySnapshotService service) {
    this.service = service;
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "fdb-observability-api");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    this.consumer = new KafkaConsumer<>(properties);
    this.consumer.subscribe(List.of(topic));
    this.thread = new Thread(this::pollLoop, "fdb-stage-metric-consumer");
    this.thread.setDaemon(true);
  }

  public void start() {
    thread.start();
  }

  private void pollLoop() {
    try {
      while (running) {
        try {
          consumer.poll(Duration.ofSeconds(1)).forEach(record ->
              service.applyMetricSample(StageMetricSample.fromJson(record.value())));
        } catch (Exception e) {
          if (running) {
            log.warn("Failed to consume stage metric sample", e);
          }
        }
      }
    } finally {
      consumer.close();
    }
  }

  @Override
  public void close() {
    running = false;
    consumer.wakeup();
    try {
      thread.join(5_000L);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
