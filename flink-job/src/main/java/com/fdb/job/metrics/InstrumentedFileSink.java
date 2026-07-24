package com.fdb.job.metrics;

import java.io.IOException;
import java.util.Collection;
import org.apache.flink.api.common.SupportsConcurrentExecutionAttempts;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.SupportsWriterState;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.connector.file.sink.writer.FileWriterBucketState;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;

public final class InstrumentedFileSink<InputT> implements Sink<InputT>,
    SupportsWriterState<InputT, FileWriterBucketState>,
    SupportsWriterState.WithCompatibleState,
    SupportsCommitter<FileSinkCommittable>,
    SupportsPreCommitTopology<FileSinkCommittable, FileSinkCommittable>,
    SupportsConcurrentExecutionAttempts {

    private final FileSink<InputT> delegate;
    private final ConnectorSinkMetrics metrics;
    private final ConnectorSinkMetrics.NanoClock clock;

    public InstrumentedFileSink(FileSink<InputT> delegate, ConnectorSinkMetrics metrics) {
        this(delegate, metrics, ConnectorSinkMetrics.systemClock());
    }

    InstrumentedFileSink(FileSink<InputT> delegate, ConnectorSinkMetrics metrics,
                         ConnectorSinkMetrics.NanoClock clock) {
        this.delegate = delegate;
        this.metrics = metrics;
        this.clock = clock;
    }

    @Override
    public SinkWriter<InputT> createWriter(InitContext context) throws IOException {
        metrics.open(subtaskIndex(context));
        return new InstrumentedSinkWriter<>(delegate.createWriter(context), metrics, clock);
    }

    @Override
    public SinkWriter<InputT> createWriter(WriterInitContext context) throws IOException {
        metrics.open(subtaskIndex(context));
        return new InstrumentedSinkWriter<>(delegate.createWriter(context), metrics, clock);
    }

    @Override
    public StatefulSinkWriter<InputT, FileWriterBucketState> restoreWriter(
        WriterInitContext context,
        Collection<FileWriterBucketState> recoveredState) throws IOException {
        metrics.open(subtaskIndex(context));
        return new InstrumentedSinkWriter<>(delegate.restoreWriter(context, recoveredState), metrics, clock);
    }

    @Override
    public SimpleVersionedSerializer<FileWriterBucketState> getWriterStateSerializer() {
        return delegate.getWriterStateSerializer();
    }

    @Override
    public Collection<String> getCompatibleWriterStateNames() {
        return delegate.getCompatibleWriterStateNames();
    }

    @Override
    public Committer<FileSinkCommittable> createCommitter(CommitterInitContext context) throws IOException {
        metrics.open(subtaskIndex(context));
        return new InstrumentedCommitter<>(delegate.createCommitter(context), metrics, clock);
    }

    @Override
    public SimpleVersionedSerializer<FileSinkCommittable> getCommittableSerializer() {
        return delegate.getCommittableSerializer();
    }

    @Override
    public DataStream<CommittableMessage<FileSinkCommittable>> addPreCommitTopology(
        DataStream<CommittableMessage<FileSinkCommittable>> committables) {
        return delegate.addPreCommitTopology(committables);
    }

    @Override
    public SimpleVersionedSerializer<FileSinkCommittable> getWriteResultSerializer() {
        return delegate.getWriteResultSerializer();
    }

    private static int subtaskIndex(org.apache.flink.api.connector.sink2.InitContext context) {
        return context == null || context.getTaskInfo() == null ? -1 : context.getTaskInfo().getIndexOfThisSubtask();
    }
}
