package com.fdb.simulator;

import static org.assertj.core.api.Assertions.assertThat;

import com.fdb.common.avro.ChrEvent;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

class KafkaPublisherTest {
    @Test
    void tracks_submitted_delivered_and_failed_records_from_callbacks() {
        FakeSender<ChrEvent> sender = new FakeSender<>();
        KafkaPublisher<ChrEvent> publisher = new KafkaPublisher<>("chr-events", sender);

        publisher.publish("cell-a", null);
        publisher.publish("cell-b", null);

        assertThat(publisher.submittedRecords()).isEqualTo(2);
        assertThat(publisher.deliveredRecords()).isZero();
        assertThat(publisher.failedRecords()).isZero();

        sender.succeed(0);
        sender.fail(1);

        assertThat(publisher.submittedRecords()).isEqualTo(2);
        assertThat(publisher.deliveredRecords()).isEqualTo(1);
        assertThat(publisher.failedRecords()).isEqualTo(1);
    }

    @Test
    void close_uses_bounded_timeout() {
        FakeSender<ChrEvent> sender = new FakeSender<>();
        KafkaPublisher<ChrEvent> publisher = new KafkaPublisher<>("chr-events", sender);

        publisher.close();

        assertThat(sender.closeTimeout).isEqualTo(Duration.ofSeconds(5));
    }

    private static final class FakeSender<T extends org.apache.avro.specific.SpecificRecord>
        implements KafkaPublisher.Sender<T> {
        private final List<Callback> callbacks = new ArrayList<>();
        private Duration closeTimeout;

        @Override
        public void send(ProducerRecord<String, T> record, Callback callback) {
            callbacks.add(callback);
        }

        @Override
        public void flush() {
        }

        @Override
        public void close(Duration timeout) {
            closeTimeout = timeout;
        }

        void succeed(int index) {
            callbacks.get(index).onCompletion(null, null);
        }

        void fail(int index) {
            callbacks.get(index).onCompletion(null, new RuntimeException("send failed"));
        }
    }
}
