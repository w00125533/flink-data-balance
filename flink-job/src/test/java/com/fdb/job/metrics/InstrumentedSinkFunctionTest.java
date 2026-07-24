package com.fdb.job.metrics;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class InstrumentedSinkFunctionTest {

    @Test
    void records_legacy_sink_write_and_checkpoint_commit_separately() throws Exception {
        TestNanoClock clock = new TestNanoClock();
        FakeLegacySink delegate = new FakeLegacySink(clock);
        ConnectorSinkMetrics metrics = disabledMetrics("starrocks-kpi-1m");
        InstrumentedSinkFunction<String> sink = new InstrumentedSinkFunction<>(delegate, metrics, clock);

        sink.open(new Configuration());
        sink.invoke("a", null);
        sink.snapshotState(null);
        sink.notifyCheckpointComplete(42L);

        assertThat(delegate.opened).isTrue();
        assertThat(delegate.invoked).isEqualTo(1);
        assertThat(delegate.snapshots).isEqualTo(1);
        assertThat(delegate.completedCheckpointId).isEqualTo(42L);
        assertThat(metrics.writeSample(1_717_400_000_000L).latencyP95Ms()).isEqualTo(4L);
        assertThat(metrics.commitSample(1_717_400_000_100L).latencyP95Ms()).isEqualTo(70L);
    }

    private static ConnectorSinkMetrics disabledMetrics(String stageId) {
        return new ConnectorSinkMetrics(
            stageId,
            "Cell KPI 1m StarRocks Sink",
            "starrocks",
            "kpi_1m",
            "MIN_1",
            1,
            new MetricRuntimeConfig("run-a", "starrocks", 4, false));
    }

    private static final class FakeLegacySink extends RichSinkFunction<String>
        implements CheckpointedFunction, CheckpointListener {
        private final TestNanoClock clock;
        private boolean opened;
        private int invoked;
        private int snapshots;
        private long completedCheckpointId = -1L;

        private FakeLegacySink(TestNanoClock clock) {
            this.clock = clock;
        }

        @Override
        public void open(Configuration parameters) {
            opened = true;
        }

        @Override
        public void invoke(String value, Context context) {
            invoked++;
            clock.advanceMillis(4L);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            snapshots++;
            clock.advanceMillis(30L);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            completedCheckpointId = checkpointId;
            clock.advanceMillis(70L);
        }
    }
}
