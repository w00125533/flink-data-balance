package com.fdb.benchmark;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

public final class KafkaTopicResetTool {
  private static final String DEFAULT_RETENTION_MS = "3600000";
  private static final String DEFAULT_RETENTION_BYTES = "10737418240";
  private static final String DEFAULT_SEGMENT_MS = "600000";
  private static final long POLL_INTERVAL_MS = 200L;

  private KafkaTopicResetTool() {}

  public record TopicSpec(String name, int partitions, Map<String, String> configs) {}

  public static void main(String[] args) throws Exception {
    Map<String, String> env = System.getenv();
    String bootstrap = value(env, "FDB_KAFKA_HOST_BOOTSTRAP",
        value(env, "FDB_KAFKA_BOOTSTRAP", "localhost:9092"));
    long timeoutSec = longValue(env, "FDB_KAFKA_ADMIN_TIMEOUT_SEC", 60);
    Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
    properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, Long.toString(timeoutSec * 1000L));

    try (Admin admin = Admin.create(properties)) {
      reset(admin, topicSpecs(env), Duration.ofSeconds(timeoutSec));
    }
    System.out.println("[OK] Kafka benchmark topics reset via AdminClient: " + bootstrap);
  }

  static List<TopicSpec> topicSpecs(Map<String, String> env) {
    String retentionMs = value(env, "FDB_RETENTION_MS", DEFAULT_RETENTION_MS);
    String retentionBytes = value(env, "FDB_RETENTION_BYTES", DEFAULT_RETENTION_BYTES);
    String segmentMs = value(env, "FDB_KAFKA_SEGMENT_MS", DEFAULT_SEGMENT_MS);
    Map<String, TopicSpec> specs = new LinkedHashMap<>();

    add(specs, deleteTopic(topic(env, "FDB_CHR_TOPIC", "chr-events"), 64,
        value(env, "FDB_CHR_RETENTION_MS", retentionMs), segmentMs, retentionBytes));
    add(specs, deleteTopic(topic(env, "FDB_PM_TOPIC", "pm-stats"), 16,
        value(env, "FDB_PM_RETENTION_MS", retentionMs), segmentMs, retentionBytes));
    add(specs, compactTopic(topic(env, "FDB_CFG_TOPIC", "cfg-config"), 8));
    add(specs, compactTopic(topic(env, "FDB_TOPOLOGY_TOPIC", "topology"), 4));

    if ("true".equalsIgnoreCase(value(env, "FDB_DYNAMIC_BALANCING_ENABLED", "false"))) {
      add(specs, deleteTopic(topic(env, "FDB_LB_HEARTBEAT_TOPIC", "lb-heartbeat"), 1,
          retentionMs, segmentMs, retentionBytes));
      add(specs, compactTopic(topic(env, "FDB_LB_ROUTING_TOPIC", "lb-routing"), 1));
    }

    add(specs, deleteTopic(topic(env, "FDB_METRICS_TOPIC", "fdb-stage-metrics"), 1,
        value(env, "FDB_METRICS_RETENTION_MS", retentionMs), segmentMs, retentionBytes));
    add(specs, deleteTopic(topic(env, "FDB_CELL_ANOMALY_TOPIC", "cell-anomaly-events"), 16,
        value(env, "FDB_CELL_ANOMALY_RETENTION_MS", value(env, "FDB_ANOMALY_RETENTION_MS", retentionMs)),
        segmentMs, retentionBytes));
    add(specs, deleteTopic(topic(env, "FDB_USER_ANOMALY_TOPIC", "user-anomaly-events"), 16,
        value(env, "FDB_USER_ANOMALY_RETENTION_MS", value(env, "FDB_ANOMALY_RETENTION_MS", retentionMs)),
        segmentMs, retentionBytes));
    add(specs, deleteTopic(topic(env, "FDB_GRID_ANOMALY_TOPIC", "grid-anomaly-events"), 16,
        value(env, "FDB_GRID_ANOMALY_RETENTION_MS", value(env, "FDB_ANOMALY_RETENTION_MS", retentionMs)),
        segmentMs, retentionBytes));
    add(specs, deleteTopic(topic(env, "FDB_KPI_1M_TOPIC", "cell-kpi-1m"), 8,
        value(env, "FDB_KPI_1M_RETENTION_MS", retentionMs), segmentMs, retentionBytes));
    add(specs, deleteTopic(topic(env, "FDB_KPI_5M_TOPIC", "cell-kpi-5m"), 8,
        value(env, "FDB_KPI_5M_RETENTION_MS", retentionMs), segmentMs, retentionBytes));
    add(specs, deleteTopic(topic(env, "FDB_CHR_DLQ_TOPIC", "chr-dlq"), 4,
        retentionMs, segmentMs, retentionBytes));
    add(specs, deleteTopic(topic(env, "FDB_PM_DLQ_TOPIC", "pm-dlq"), 4,
        retentionMs, segmentMs, retentionBytes));
    add(specs, deleteTopic(topic(env, "FDB_CFG_DLQ_TOPIC", "cfg-dlq"), 4,
        retentionMs, segmentMs, retentionBytes));
    add(specs, deleteTopic(topic(env, "FDB_ENRICHMENT_LATE_TOPIC", "enrichment-late"), 4,
        retentionMs, segmentMs, retentionBytes));

    return new ArrayList<>(specs.values());
  }

  static void reset(Admin admin, List<TopicSpec> specs, Duration timeout) throws Exception {
    Set<String> names = specs.stream().map(TopicSpec::name).collect(Collectors.toSet());
    deleteTopics(admin, names, timeout);
    waitUntilDeleted(admin, names, timeout);
    createTopics(admin, specs, timeout);
    waitUntilPresent(admin, names, timeout);
    applyConfigs(admin, specs, timeout);
  }

  private static void deleteTopics(Admin admin, Set<String> names, Duration timeout) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    Set<String> pending = new LinkedHashSet<>(names);
    Exception lastRetryableFailure = null;

    while (!pending.isEmpty() && System.nanoTime() < deadline) {
      DeleteTopicsResult result = admin.deleteTopics(pending);
      for (String name : new ArrayList<>(pending)) {
        try {
          result.topicNameValues().get(name).get(operationWaitMillis(deadline), TimeUnit.MILLISECONDS);
          pending.remove(name);
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();
          if (cause instanceof UnknownTopicOrPartitionException) {
            pending.remove(name);
          } else if (cause instanceof UnknownServerException) {
            lastRetryableFailure = e;
          } else {
            throw e;
          }
        } catch (TimeoutException e) {
          lastRetryableFailure = e;
        }
      }

      Set<String> current = topicNames(admin, deadline);
      pending.removeIf(name -> !current.contains(name));
      if (!pending.isEmpty()) {
        sleepBeforeRetry(deadline);
      }
    }

    if (!pending.isEmpty()) {
      TimeoutException timeoutException =
          new TimeoutException("timed out waiting for Kafka topics to accept delete: " + pending);
      if (lastRetryableFailure != null) {
        timeoutException.initCause(lastRetryableFailure);
      }
      throw timeoutException;
    }
  }

  private static void waitUntilDeleted(Admin admin, Set<String> names, Duration timeout) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      Set<String> current = topicNames(admin, deadline);
      if (current.stream().noneMatch(names::contains)) {
        return;
      }
      sleepBeforeRetry(deadline);
    }
    throw new TimeoutException("timed out waiting for Kafka topics to be deleted: " + names);
  }

  private static void createTopics(Admin admin, List<TopicSpec> specs, Duration timeout) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    Map<String, TopicSpec> pending = new LinkedHashMap<>();
    for (TopicSpec spec : specs) {
      pending.put(spec.name(), spec);
    }
    Exception lastRetryableFailure = null;

    while (!pending.isEmpty() && System.nanoTime() < deadline) {
      CreateTopicsResult result = admin.createTopics(newTopics(pending.values()));
      for (TopicSpec spec : new ArrayList<>(pending.values())) {
        try {
          result.values().get(spec.name()).get(remainingMillis(deadline), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
          if (e.getCause() instanceof TopicExistsException) {
            lastRetryableFailure = e;
          } else {
            throw e;
          }
        }
      }

      Set<String> existing = topicNames(admin, deadline);
      pending.keySet().removeIf(existing::contains);
      if (!pending.isEmpty()) {
        sleepBeforeRetry(deadline);
      }
    }

    if (!pending.isEmpty()) {
      TimeoutException timeoutException =
          new TimeoutException("timed out waiting for Kafka topics to be created: " + pending.keySet());
      if (lastRetryableFailure != null) {
        timeoutException.initCause(lastRetryableFailure);
      }
      throw timeoutException;
    }
  }

  private static void waitUntilPresent(Admin admin, Set<String> names, Duration timeout) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    Set<String> missing = new LinkedHashSet<>(names);
    while (System.nanoTime() < deadline) {
      Set<String> current = topicNames(admin, deadline);
      missing.removeIf(current::contains);
      if (missing.isEmpty()) {
        return;
      }
      sleepBeforeRetry(deadline);
    }
    throw new TimeoutException("timed out waiting for Kafka topics to be visible: " + missing);
  }

  private static List<NewTopic> newTopics(Collection<TopicSpec> specs) {
    return specs.stream()
        .map(spec -> new NewTopic(spec.name(), spec.partitions(), (short) 1).configs(spec.configs()))
        .toList();
  }

  private static Set<String> topicNames(Admin admin, long deadlineNanos) throws Exception {
    return admin.listTopics().names().get(remainingMillis(deadlineNanos), TimeUnit.MILLISECONDS);
  }

  private static long remainingMillis(long deadlineNanos) throws TimeoutException {
    long remainingNanos = deadlineNanos - System.nanoTime();
    if (remainingNanos <= 0L) {
      throw new TimeoutException("Kafka AdminClient operation timed out");
    }
    return Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos));
  }

  private static long operationWaitMillis(long deadlineNanos) throws TimeoutException {
    return Math.min(remainingMillis(deadlineNanos), POLL_INTERVAL_MS);
  }

  private static void sleepBeforeRetry(long deadlineNanos) throws InterruptedException {
    long remainingMillis = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
    if (remainingMillis > 0L) {
      Thread.sleep(Math.min(POLL_INTERVAL_MS, remainingMillis));
    }
  }

  private static void applyConfigs(Admin admin, List<TopicSpec> specs, Duration timeout) throws Exception {
    Map<ConfigResource, Collection<AlterConfigOp>> configs = new LinkedHashMap<>();
    for (TopicSpec spec : specs) {
      ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, spec.name());
      List<AlterConfigOp> ops = spec.configs().entrySet().stream()
          .map(entry -> new AlterConfigOp(new ConfigEntry(entry.getKey(), entry.getValue()), AlterConfigOp.OpType.SET))
          .toList();
      configs.put(resource, ops);
    }
    admin.incrementalAlterConfigs(configs).all().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
  }

  private static TopicSpec deleteTopic(
      String name,
      int partitions,
      String retentionMs,
      String segmentMs,
      String retentionBytes) {
    Map<String, String> configs = new LinkedHashMap<>();
    configs.put("cleanup.policy", "delete");
    configs.put("retention.ms", retentionMs);
    configs.put("segment.ms", segmentMs);
    configs.put("retention.bytes", retentionBytes);
    return new TopicSpec(name, partitions, configs);
  }

  private static TopicSpec compactTopic(String name, int partitions) {
    return new TopicSpec(name, partitions, Map.of("cleanup.policy", "compact"));
  }

  private static void add(Map<String, TopicSpec> specs, TopicSpec spec) {
    specs.putIfAbsent(spec.name(), spec);
  }

  private static String topic(Map<String, String> env, String key, String defaultValue) {
    return value(env, key, defaultValue);
  }

  private static String value(Map<String, String> env, String key, String defaultValue) {
    String value = env.get(key);
    return value == null || value.isBlank() ? defaultValue : value.trim();
  }

  private static long longValue(Map<String, String> env, String key, long defaultValue) {
    String value = env.get(key);
    if (value == null || value.isBlank()) {
      return defaultValue;
    }
    try {
      return Long.parseLong(value.trim());
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }
}
