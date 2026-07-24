package com.fdb.job.metrics;

import java.io.IOException;
import java.util.Collection;
import org.apache.flink.api.connector.sink2.Committer;

final class InstrumentedCommitter<CommittableT> implements Committer<CommittableT> {

    private final Committer<CommittableT> delegate;
    private final ConnectorSinkMetrics metrics;
    private final ConnectorSinkMetrics.NanoClock clock;

    InstrumentedCommitter(Committer<CommittableT> delegate, ConnectorSinkMetrics metrics,
                          ConnectorSinkMetrics.NanoClock clock) {
        this.delegate = delegate;
        this.metrics = metrics;
        this.clock = clock;
    }

    @Override
    public void commit(Collection<CommitRequest<CommittableT>> committables) throws IOException, InterruptedException {
        long started = clock.nanoTime();
        try {
            delegate.commit(committables);
        } catch (IOException | InterruptedException | RuntimeException e) {
            metrics.recordCommitFailure();
            throw e;
        } finally {
            metrics.recordCommitDurationNanos(clock.nanoTime() - started, -1L);
            metrics.publishCommit();
        }
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }
}
