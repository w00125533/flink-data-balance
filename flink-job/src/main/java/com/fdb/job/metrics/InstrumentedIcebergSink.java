package com.fdb.job.metrics;

import java.io.IOException;
import org.apache.flink.api.common.SupportsConcurrentExecutionAttempts;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.SupportsPostCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;

@SuppressWarnings({"rawtypes", "unchecked"})
public final class InstrumentedIcebergSink implements Sink<RowData>,
    SupportsCommitter,
    SupportsPreWriteTopology<RowData>,
    SupportsPreCommitTopology,
    SupportsPostCommitTopology,
    SupportsConcurrentExecutionAttempts {

    private final Sink<RowData> delegate;
    private final ConnectorSinkMetrics metrics;
    private final ConnectorSinkMetrics.NanoClock clock;

    public InstrumentedIcebergSink(Sink<RowData> delegate, ConnectorSinkMetrics metrics) {
        this(delegate, metrics, ConnectorSinkMetrics.systemClock());
    }

    InstrumentedIcebergSink(Sink<RowData> delegate, ConnectorSinkMetrics metrics,
                            ConnectorSinkMetrics.NanoClock clock) {
        this.delegate = delegate;
        this.metrics = metrics;
        this.clock = clock;
    }

    @Override
    public SinkWriter<RowData> createWriter(InitContext context) throws IOException {
        metrics.open(subtaskIndex(context));
        return new InstrumentedSinkWriter<>(delegate.createWriter(context), metrics, clock);
    }

    @Override
    public SinkWriter<RowData> createWriter(WriterInitContext context) throws IOException {
        metrics.open(subtaskIndex(context));
        return new InstrumentedSinkWriter<>(delegate.createWriter(context), metrics, clock);
    }

    @Override
    public Committer createCommitter(CommitterInitContext context) throws IOException {
        metrics.open(subtaskIndex(context));
        return new InstrumentedCommitter(((SupportsCommitter) delegate).createCommitter(context), metrics, clock);
    }

    @Override
    public SimpleVersionedSerializer getCommittableSerializer() {
        return ((SupportsCommitter) delegate).getCommittableSerializer();
    }

    @Override
    public DataStream<RowData> addPreWriteTopology(DataStream<RowData> inputDataStream) {
        return ((SupportsPreWriteTopology<RowData>) delegate).addPreWriteTopology(inputDataStream);
    }

    @Override
    public DataStream addPreCommitTopology(DataStream committables) {
        return ((SupportsPreCommitTopology) delegate).addPreCommitTopology(committables);
    }

    @Override
    public SimpleVersionedSerializer getWriteResultSerializer() {
        return ((SupportsPreCommitTopology) delegate).getWriteResultSerializer();
    }

    @Override
    public void addPostCommitTopology(DataStream committables) {
        ((SupportsPostCommitTopology) delegate).addPostCommitTopology(committables);
    }

    private static int subtaskIndex(org.apache.flink.api.connector.sink2.InitContext context) {
        return context == null || context.getTaskInfo() == null ? -1 : context.getTaskInfo().getIndexOfThisSubtask();
    }
}
