package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;

class KafkaTopicResetToolTest {
  @Test
  void default_topic_specs_match_local_benchmark_topics() {
    var specs = KafkaTopicResetTool.topicSpecs(Map.of(
        "FDB_RETENTION_MS", "1000",
        "FDB_RETENTION_BYTES", "2048",
        "FDB_KAFKA_SEGMENT_MS", "500"));

    assertThat(specs).extracting(KafkaTopicResetTool.TopicSpec::name)
        .contains("chr-events", "pm-stats", "cfg-config", "topology", "fdb-stage-metrics",
            "cell-anomaly-events", "user-anomaly-events", "grid-anomaly-events",
            "cell-kpi-1m", "cell-kpi-5m", "chr-dlq", "pm-dlq", "cfg-dlq", "enrichment-late")
        .doesNotContain("lb-heartbeat", "lb-routing");
    assertThat(spec(specs, "chr-events").partitions()).isEqualTo(64);
    assertThat(spec(specs, "pm-stats").partitions()).isEqualTo(16);
    assertThat(spec(specs, "cell-kpi-1m").configs())
        .containsEntry("cleanup.policy", "delete")
        .containsEntry("retention.ms", "1000")
        .containsEntry("segment.ms", "500")
        .containsEntry("retention.bytes", "2048");
    assertThat(spec(specs, "cfg-config").configs())
        .containsEntry("cleanup.policy", "compact")
        .doesNotContainKeys("retention.ms", "segment.ms", "retention.bytes");
  }

  @Test
  void dynamic_balancing_topics_are_created_only_when_enabled() {
    var specs = KafkaTopicResetTool.topicSpecs(Map.of("FDB_DYNAMIC_BALANCING_ENABLED", "true"));

    assertThat(spec(specs, "lb-heartbeat").partitions()).isEqualTo(1);
    assertThat(spec(specs, "lb-routing").configs()).containsEntry("cleanup.policy", "compact");
  }

  @Test
  void topic_names_can_be_overridden_from_environment() {
    var specs = KafkaTopicResetTool.topicSpecs(Map.of(
        "FDB_CHR_TOPIC", "chr-a",
        "FDB_PM_TOPIC", "pm-a",
        "FDB_KPI_1M_TOPIC", "kpi-a"));

    assertThat(specs).extracting(KafkaTopicResetTool.TopicSpec::name)
        .contains("chr-a", "pm-a", "kpi-a")
        .doesNotContain("chr-events", "pm-stats", "cell-kpi-1m");
  }

  @Test
  void reset_retries_topics_that_are_still_marked_for_deletion() {
    var specs = List.of(new KafkaTopicResetTool.TopicSpec(
        "cell-anomaly-events", 16, Map.of("cleanup.policy", "delete")));
    var admin = new MarkedForDeletionAdmin(Set.of("cell-anomaly-events"));

    assertThatCode(() -> KafkaTopicResetTool.reset(admin.proxy(), specs, Duration.ofSeconds(3)))
        .doesNotThrowAnyException();
    assertThat(admin.createCalls()).isGreaterThanOrEqualTo(2);
  }

  private static KafkaTopicResetTool.TopicSpec spec(
      Iterable<KafkaTopicResetTool.TopicSpec> specs,
      String name) {
    for (KafkaTopicResetTool.TopicSpec spec : specs) {
      if (spec.name().equals(name)) {
        return spec;
      }
    }
    throw new AssertionError("missing topic spec: " + name);
  }

  private static final class MarkedForDeletionAdmin implements InvocationHandler {
    private final Set<String> visibleTopics = new LinkedHashSet<>();
    private final Set<String> deletingTopics = new LinkedHashSet<>();
    private int createCalls;

    private MarkedForDeletionAdmin(Set<String> initialTopics) {
      visibleTopics.addAll(initialTopics);
    }

    private Admin proxy() {
      return (Admin) Proxy.newProxyInstance(
          Admin.class.getClassLoader(),
          new Class<?>[] {Admin.class},
          this);
    }

    private int createCalls() {
      return createCalls;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      return switch (method.getName()) {
        case "deleteTopics" -> deleteTopics(args[0]);
        case "listTopics" -> listTopics();
        case "createTopics" -> createTopics(args[0]);
        case "incrementalAlterConfigs" -> alterConfigs(args[0]);
        case "close" -> null;
        case "toString" -> "MarkedForDeletionAdmin";
        case "hashCode" -> System.identityHashCode(proxy);
        case "equals" -> proxy == args[0];
        default -> throw new UnsupportedOperationException(method.toString());
      };
    }

    private DeleteTopicsResult deleteTopics(Object namesArg) throws Exception {
      @SuppressWarnings("unchecked")
      Collection<String> names = (Collection<String>) namesArg;
      Map<String, KafkaFuture<Void>> futures = new LinkedHashMap<>();
      for (String name : names) {
        visibleTopics.remove(name);
        deletingTopics.add(name);
        futures.put(name, completedVoid());
      }
      Method factory = DeleteTopicsResult.class.getDeclaredMethod("ofTopicNames", Map.class);
      factory.setAccessible(true);
      return (DeleteTopicsResult) factory.invoke(null, futures);
    }

    private ListTopicsResult listTopics() throws Exception {
      Map<String, TopicListing> listings = new LinkedHashMap<>();
      for (String topic : visibleTopics) {
        listings.put(topic, new TopicListing(topic, false));
      }
      Constructor<ListTopicsResult> constructor =
          ListTopicsResult.class.getDeclaredConstructor(KafkaFuture.class);
      constructor.setAccessible(true);
      return constructor.newInstance(completed(listings));
    }

    private CreateTopicsResult createTopics(Object topicsArg) throws Exception {
      createCalls++;
      @SuppressWarnings("unchecked")
      Collection<NewTopic> topics = (Collection<NewTopic>) topicsArg;
      Map<String, KafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>> futures =
          new LinkedHashMap<>();
      for (NewTopic topic : topics) {
        String name = topic.name();
        if (deletingTopics.remove(name)) {
          futures.put(name, failed(new TopicExistsException("Topic '" + name + "' is marked for deletion.")));
        } else {
          visibleTopics.add(name);
          futures.put(name, completed(new CreateTopicsResult.TopicMetadataAndConfig(
              Uuid.randomUuid(), topic.numPartitions(), 1, new Config(List.of()))));
        }
      }
      Constructor<CreateTopicsResult> constructor =
          CreateTopicsResult.class.getDeclaredConstructor(Map.class);
      constructor.setAccessible(true);
      return constructor.newInstance(futures);
    }

    private AlterConfigsResult alterConfigs(Object configsArg) throws Exception {
      @SuppressWarnings("unchecked")
      Map<ConfigResource, ?> configs = (Map<ConfigResource, ?>) configsArg;
      Map<ConfigResource, KafkaFuture<Void>> futures = new LinkedHashMap<>();
      for (ConfigResource resource : configs.keySet()) {
        if (visibleTopics.contains(resource.name())) {
          futures.put(resource, completedVoid());
        } else {
          futures.put(resource, failed(new UnknownTopicOrPartitionException(
              "The topic '" + resource.name() + "' does not exist.")));
        }
      }
      Constructor<AlterConfigsResult> constructor =
          AlterConfigsResult.class.getDeclaredConstructor(Map.class);
      constructor.setAccessible(true);
      return constructor.newInstance(futures);
    }
  }

  private static KafkaFuture<Void> completedVoid() {
    return completed(null);
  }

  private static <T> KafkaFuture<T> completed(T value) {
    KafkaFutureImpl<T> future = new KafkaFutureImpl<>();
    future.complete(value);
    return future;
  }

  private static <T> KafkaFuture<T> failed(Throwable error) {
    KafkaFutureImpl<T> future = new KafkaFutureImpl<>();
    future.completeExceptionally(error);
    return future;
  }
}
