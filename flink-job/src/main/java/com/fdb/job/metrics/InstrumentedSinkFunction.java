package com.fdb.job.metrics;

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public final class InstrumentedSinkFunction<T> extends RichSinkFunction<T>
    implements CheckpointedFunction, CheckpointListener {

    private final SinkFunction<T> delegate;
    private final ConnectorSinkMetrics metrics;
    private final ConnectorSinkMetrics.NanoClock clock;

    public InstrumentedSinkFunction(SinkFunction<T> delegate, ConnectorSinkMetrics metrics) {
        this(delegate, metrics, ConnectorSinkMetrics.systemClock());
    }

    InstrumentedSinkFunction(SinkFunction<T> delegate, ConnectorSinkMetrics metrics,
                             ConnectorSinkMetrics.NanoClock clock) {
        this.delegate = delegate;
        this.metrics = metrics;
        this.clock = clock;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        metrics.open(runtimeSubtaskIndex());
        if (delegate instanceof RichFunction rich) {
            try {
                rich.setRuntimeContext(getRuntimeContext());
            } catch (IllegalStateException ignored) {
                // Unit tests may invoke open outside a Flink runtime context.
            }
            rich.open(parameters);
        }
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        long started = clock.nanoTime();
        try {
            delegate.invoke(value, context);
        } catch (Exception e) {
            metrics.recordWriteFailure();
            throw e;
        } finally {
            metrics.recordWriteDurationNanos(clock.nanoTime() - started);
            metrics.publishWriteIfDue();
        }
    }

    @Override
    public void finish() throws Exception {
        long started = clock.nanoTime();
        try {
            delegate.finish();
        } catch (Exception e) {
            metrics.recordCommitFailure();
            throw e;
        } finally {
            metrics.recordCommitDurationNanos(clock.nanoTime() - started, -1L);
            metrics.publishCommit();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        if (delegate instanceof CheckpointedFunction checkpointed) {
            checkpointed.initializeState(context);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (!(delegate instanceof CheckpointedFunction checkpointed)) {
            return;
        }
        long started = clock.nanoTime();
        try {
            checkpointed.snapshotState(context);
        } catch (Exception e) {
            metrics.recordCommitFailure();
            throw e;
        } finally {
            metrics.recordCommitDurationNanos(clock.nanoTime() - started, checkpointId(context));
            metrics.publishCommit();
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (!(delegate instanceof CheckpointListener listener)) {
            return;
        }
        long started = clock.nanoTime();
        try {
            listener.notifyCheckpointComplete(checkpointId);
        } catch (Exception e) {
            metrics.recordCommitFailure();
            throw e;
        } finally {
            metrics.recordCommitDurationNanos(clock.nanoTime() - started, checkpointId);
            metrics.publishCommit();
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        if (delegate instanceof CheckpointListener listener) {
            listener.notifyCheckpointAborted(checkpointId);
        }
    }

    @Override
    public void close() throws Exception {
        try {
            if (delegate instanceof RichFunction rich) {
                rich.close();
            }
        } finally {
            metrics.close();
        }
    }

    private int runtimeSubtaskIndex() {
        try {
            return getRuntimeContext().getIndexOfThisSubtask();
        } catch (IllegalStateException ignored) {
            return -1;
        }
    }

    private static long checkpointId(FunctionSnapshotContext context) {
        return context == null ? -1L : context.getCheckpointId();
    }
}
