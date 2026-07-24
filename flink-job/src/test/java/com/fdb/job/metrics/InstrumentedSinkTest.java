package com.fdb.job.metrics;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.OptionalLong;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class InstrumentedSinkTest {

    @Test
    void records_new_sink_writer_and_committer_latency_separately() throws Exception {
        TestNanoClock clock = new TestNanoClock();
        FakeSink delegate = new FakeSink(clock);
        ConnectorSinkMetrics metrics = new ConnectorSinkMetrics(
            "hive-kpi-1m",
            "Cell KPI 1m Hive Sink",
            "hive",
            "kpi_1m",
            "MIN_1",
            1,
            new MetricRuntimeConfig("run-a", "hive", 4, false));
        InstrumentedSink<String> sink = new InstrumentedSink<>(delegate, metrics, clock);

        SinkWriter<String> writer = sink.createWriter((Sink.InitContext) null);
        writer.write("a", null);
        writer.flush(false);
        assertThat(((CommittingSinkWriter<String, String>) writer).prepareCommit()).containsExactly("commit-a");
        sink.createCommitter(null).commit(List.of(new FakeCommitRequest<>("commit-a")));

        assertThat(delegate.writer.writes).isEqualTo(1);
        assertThat(delegate.writer.flushes).isEqualTo(1);
        assertThat(delegate.writer.prepareCommits).isEqualTo(1);
        assertThat(delegate.committer.commits).isEqualTo(1);
        assertThat(metrics.writeSample(1_717_400_000_000L).latencyP95Ms()).isEqualTo(3L);
        assertThat(metrics.commitSample(1_717_400_000_100L).latencyP95Ms()).isEqualTo(11L);
    }

    @Test
    void initializes_metrics_when_only_committer_is_created() throws Exception {
        TestNanoClock clock = new TestNanoClock();
        FakeSink delegate = new FakeSink(clock);
        ConnectorSinkMetrics metrics = new ConnectorSinkMetrics(
            "hive-kpi-1m",
            "Cell KPI 1m Hive Sink",
            "hive",
            "kpi_1m",
            "MIN_1",
            1,
            new MetricRuntimeConfig("run-a", "hive", 4, false));
        InstrumentedSink<String> sink = new InstrumentedSink<>(delegate, metrics, clock);

        sink.createCommitter(new FakeCommitterInitContext(7))
            .commit(List.of(new FakeCommitRequest<>("commit-a")));

        assertThat(metrics.commitSample(1_717_400_000_100L).subtaskIndex()).isEqualTo(7);
    }

    private static final class FakeSink implements Sink<String>, SupportsCommitter<String> {
        private final FakeWriter writer;
        private final FakeCommitter committer;

        private FakeSink(TestNanoClock clock) {
            this.writer = new FakeWriter(clock);
            this.committer = new FakeCommitter(clock);
        }

        @Override
        public SinkWriter<String> createWriter(InitContext context) {
            return writer;
        }

        @Override
        public Committer<String> createCommitter(CommitterInitContext context) {
            return committer;
        }

        @Override
        public SimpleVersionedSerializer<String> getCommittableSerializer() {
            return new StringSerializer();
        }
    }

    private static final class FakeWriter implements CommittingSinkWriter<String, String> {
        private final TestNanoClock clock;
        private int writes;
        private int flushes;
        private int prepareCommits;

        private FakeWriter(TestNanoClock clock) {
            this.clock = clock;
        }

        @Override
        public void write(String element, Context context) {
            writes++;
            clock.advanceMillis(3L);
        }

        @Override
        public void flush(boolean endOfInput) {
            flushes++;
            clock.advanceMillis(7L);
        }

        @Override
        public Collection<String> prepareCommit() {
            prepareCommits++;
            clock.advanceMillis(11L);
            return List.of("commit-a");
        }

        @Override
        public void close() {
        }
    }

    private static final class FakeCommitter implements Committer<String> {
        private final TestNanoClock clock;
        private int commits;

        private FakeCommitter(TestNanoClock clock) {
            this.clock = clock;
        }

        @Override
        public void commit(Collection<CommitRequest<String>> committables) {
            commits++;
            clock.advanceMillis(5L);
        }

        @Override
        public void close() {
        }
    }

    private record FakeCommitRequest<T>(T committable) implements Committer.CommitRequest<T> {
        @Override
        public T getCommittable() {
            return committable;
        }

        @Override
        public int getNumberOfRetries() {
            return 0;
        }

        @Override
        public void signalFailedWithKnownReason(Throwable throwable) {
        }

        @Override
        public void signalFailedWithUnknownReason(Throwable throwable) {
        }

        @Override
        public void retryLater() {
        }

        @Override
        public void updateAndRetryLater(T committable) {
        }

        @Override
        public void signalAlreadyCommitted() {
        }
    }

    private static final class StringSerializer implements SimpleVersionedSerializer<String> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(String obj) {
            return obj.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }

        @Override
        public String deserialize(int version, byte[] serialized) throws IOException {
            return new String(serialized, java.nio.charset.StandardCharsets.UTF_8);
        }
    }

    private record FakeCommitterInitContext(int subtaskIndex) implements CommitterInitContext {
        @Override
        public OptionalLong getRestoredCheckpointId() {
            return OptionalLong.empty();
        }

        @Override
        public JobInfo getJobInfo() {
            return new JobInfo() {
                @Override
                public JobID getJobId() {
                    return new JobID();
                }

                @Override
                public String getJobName() {
                    return "test-job";
                }
            };
        }

        @Override
        public TaskInfo getTaskInfo() {
            return new TaskInfo() {
                @Override
                public String getTaskName() {
                    return "test-committer";
                }

                @Override
                public int getMaxNumberOfParallelSubtasks() {
                    return 8;
                }

                @Override
                public int getIndexOfThisSubtask() {
                    return subtaskIndex;
                }

                @Override
                public int getNumberOfParallelSubtasks() {
                    return 8;
                }

                @Override
                public int getAttemptNumber() {
                    return 0;
                }

                @Override
                public String getTaskNameWithSubtasks() {
                    return "test-committer (8/8)";
                }

                @Override
                public String getAllocationIDAsString() {
                    return "allocation-a";
                }
            };
        }

        @Override
        public SinkCommitterMetricGroup metricGroup() {
            return null;
        }
    }
}
