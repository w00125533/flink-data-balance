package com.fdb.job.balance.coordinator;

import com.fdb.common.metrics.StageMetricSample;
import com.fdb.job.metrics.MetricRuntimeConfig;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;

class LoadCoordinatorTest {

    @Test
    void tags_rebalance_metric_sample_with_run_metadata() throws Exception {
        LoadCoordinator coordinator = new LoadCoordinator(
            new MetricRuntimeConfig("run-a", "starrocks", 4, false));
        setRebalanceTotal(coordinator, 3L);

        StageMetricSample sample = coordinator.rebalanceMetricSample(1_717_400_000_000L);

        assertThat(sample.stageId()).isEqualTo("load-coordinator");
        assertThat(sample.rebalanceTotal()).isEqualTo(3L);
        assertThat(sample.runId()).isEqualTo("run-a");
        assertThat(sample.resultSink()).isEqualTo("starrocks");
        assertThat(sample.parallelism()).isEqualTo(4);
    }

    private static void setRebalanceTotal(LoadCoordinator coordinator, long value) throws Exception {
        Field field = LoadCoordinator.class.getDeclaredField("rebalanceTotal");
        field.setAccessible(true);
        field.setLong(coordinator, value);
    }
}
