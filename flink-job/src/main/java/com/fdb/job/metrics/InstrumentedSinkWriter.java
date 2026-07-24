package com.fdb.job.metrics;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;

final class InstrumentedSinkWriter<InputT, WriterStateT, CommittableT>
    implements StatefulSinkWriter<InputT, WriterStateT>, CommittingSinkWriter<InputT, CommittableT> {

    private final SinkWriter<InputT> delegate;
    private final ConnectorSinkMetrics metrics;
    private final ConnectorSinkMetrics.NanoClock clock;

    InstrumentedSinkWriter(SinkWriter<InputT> delegate, ConnectorSinkMetrics metrics,
                           ConnectorSinkMetrics.NanoClock clock) {
        this.delegate = delegate;
        this.metrics = metrics;
        this.clock = clock;
    }

    @Override
    public void write(InputT element, Context context) throws IOException, InterruptedException {
        long started = clock.nanoTime();
        try {
            delegate.write(element, context);
        } catch (IOException | InterruptedException | RuntimeException e) {
            metrics.recordWriteFailure();
            throw e;
        } finally {
            metrics.recordWriteDurationNanos(clock.nanoTime() - started);
            metrics.publishWriteIfDue();
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        long started = clock.nanoTime();
        try {
            delegate.flush(endOfInput);
        } catch (IOException | InterruptedException | RuntimeException e) {
            metrics.recordCommitFailure();
            throw e;
        } finally {
            metrics.recordCommitDurationNanos(clock.nanoTime() - started, -1L);
            metrics.publishCommit();
        }
    }

    @Override
    public Collection<CommittableT> prepareCommit() throws IOException, InterruptedException {
        if (!(delegate instanceof CommittingSinkWriter<?, ?> committing)) {
            return List.of();
        }
        long started = clock.nanoTime();
        try {
            @SuppressWarnings("unchecked")
            Collection<CommittableT> committables =
                ((CommittingSinkWriter<InputT, CommittableT>) committing).prepareCommit();
            return committables;
        } catch (IOException | InterruptedException | RuntimeException e) {
            metrics.recordCommitFailure();
            throw e;
        } finally {
            metrics.recordCommitDurationNanos(clock.nanoTime() - started, -1L);
            metrics.publishCommit();
        }
    }

    @Override
    public List<WriterStateT> snapshotState(long checkpointId) throws IOException {
        if (!(delegate instanceof StatefulSinkWriter<?, ?> stateful)) {
            return List.of();
        }
        long started = clock.nanoTime();
        try {
            @SuppressWarnings("unchecked")
            List<WriterStateT> state = ((StatefulSinkWriter<InputT, WriterStateT>) stateful)
                .snapshotState(checkpointId);
            return state;
        } catch (IOException | RuntimeException e) {
            metrics.recordCommitFailure();
            throw e;
        } finally {
            metrics.recordCommitDurationNanos(clock.nanoTime() - started, checkpointId);
            metrics.publishCommit();
        }
    }

    @Override
    public void writeWatermark(Watermark watermark) throws IOException, InterruptedException {
        delegate.writeWatermark(watermark);
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }
}
