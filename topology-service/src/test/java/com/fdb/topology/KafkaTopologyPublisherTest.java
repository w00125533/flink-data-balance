package com.fdb.topology;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fdb.common.avro.TopologyRecord;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;

class KafkaTopologyPublisherTest {
  @Test
  void waits_for_topic_metadata_before_sending_records() throws Exception {
    FakeSender sender = new FakeSender(List.of(List.of(), partitions()));
    KafkaTopologyPublisher publisher = new KafkaTopologyPublisher("topology", sender, millis -> {}, 1_000);

    KafkaTopologyPublisher.PublishResult result = publisher.publishAll(records(2));

    assertThat(sender.partitionCalls).isEqualTo(2);
    assertThat(sender.sentRecords).hasSize(2);
    assertThat(result.failedRecords()).isZero();
  }

  @Test
  void fails_fast_when_topic_metadata_is_not_visible() {
    FakeSender sender = new FakeSender(List.of(List.of(), List.of()));
    KafkaTopologyPublisher publisher = new KafkaTopologyPublisher("topology", sender, millis -> {}, 0);

    assertThatThrownBy(() -> publisher.publishAll(records(1)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("topology")
        .hasMessageContaining("metadata");
  }

  @Test
  void close_uses_bounded_timeout() {
    FakeSender sender = new FakeSender(List.of(partitions()));
    KafkaTopologyPublisher publisher = new KafkaTopologyPublisher("topology", sender, millis -> {}, 1_000);

    publisher.close();

    assertThat(sender.closeTimeout).isEqualTo(Duration.ofSeconds(5));
  }

  private static List<TopologyRecord> records(int sites) {
    TopologyConfig config = new TopologyConfig();
    config.getSites().setCount(sites);
    config.getSites().getCellsPerSite().setMin(1);
    config.getSites().getCellsPerSite().setMax(1);
    return new TopologyGenerator(config).generate();
  }

  private static List<PartitionInfo> partitions() {
    return List.of(new PartitionInfo("topology", 0, null, null, null));
  }

  private static final class FakeSender implements KafkaTopologyPublisher.Sender {
    private final List<List<PartitionInfo>> metadataResponses;
    private final List<ProducerRecord<String, TopologyRecord>> sentRecords = new ArrayList<>();
    private int partitionCalls;
    private Duration closeTimeout;

    FakeSender(List<List<PartitionInfo>> metadataResponses) {
      this.metadataResponses = metadataResponses;
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
      int index = Math.min(partitionCalls, metadataResponses.size() - 1);
      partitionCalls++;
      return metadataResponses.get(index);
    }

    @Override
    public void send(ProducerRecord<String, TopologyRecord> record, Callback callback) {
      sentRecords.add(record);
      callback.onCompletion(null, null);
    }

    @Override
    public void flush() {
    }

    @Override
    public void close(Duration timeout) {
      closeTimeout = timeout;
    }
  }
}
