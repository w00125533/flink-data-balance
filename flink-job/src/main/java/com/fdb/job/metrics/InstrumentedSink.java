package com.fdb.job.metrics;

import java.io.IOException;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

public final class InstrumentedSink<InputT> implements Sink<InputT>, SupportsCommitter<Object> {
    private final Sink<InputT> delegate;
    private final ConnectorSinkMetrics metrics;
    private final ConnectorSinkMetrics.NanoClock clock;

    public InstrumentedSink(Sink<InputT> delegate, ConnectorSinkMetrics metrics) {
        this(delegate, metrics, ConnectorSinkMetrics.systemClock());
    }

    InstrumentedSink(Sink<InputT> delegate, ConnectorSinkMetrics metrics, ConnectorSinkMetrics.NanoClock clock) {
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
    public Committer<Object> createCommitter(CommitterInitContext context) throws IOException {
        metrics.open(subtaskIndex(context));
        @SuppressWarnings("unchecked")
        Committer<Object> committer = ((SupportsCommitter<Object>) delegate).createCommitter(context);
        return new InstrumentedCommitter<>(committer, metrics, clock);
    }

    @Override
    public SimpleVersionedSerializer<Object> getCommittableSerializer() {
        @SuppressWarnings("unchecked")
        SimpleVersionedSerializer<Object> serializer =
            ((SupportsCommitter<Object>) delegate).getCommittableSerializer();
        return serializer;
    }

    private static int subtaskIndex(org.apache.flink.api.connector.sink2.InitContext context) {
        return context == null || context.getTaskInfo() == null ? -1 : context.getTaskInfo().getIndexOfThisSubtask();
    }
}
