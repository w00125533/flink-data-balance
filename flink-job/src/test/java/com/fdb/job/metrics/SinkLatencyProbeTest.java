package com.fdb.job.metrics;

import com.fdb.common.avro.AnomalyEvent;
import com.fdb.common.avro.AnomalyType;
import com.fdb.common.avro.CellKpi;
import com.fdb.common.avro.EntityType;
import com.fdb.common.avro.JoinQuality;
import com.fdb.common.avro.Severity;
import com.fdb.common.avro.WindowKind;
import com.fdb.common.metrics.StageMetricSample;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThat;

class SinkLatencyProbeTest {

    @Test
    void builds_sink_latency_sample_with_explicit_labels_for_cell_kpi() {
        SinkLatencyProbe<CellKpi> probe = new SinkLatencyProbe<>(
            "hive-kpi-1m", "Hive KPI 1m Sink", "hive", "kpi_1m", "MIN_1", 2);

        probe.record(kpi(1000L, "a"));
        probe.record(kpi(2000L, "bb"));

        StageMetricSample sample = probe.metricSample(1_717_400_000_000L);

        assertThat(sample.stageId()).isEqualTo("hive-kpi-1m");
        assertThat(sample.displayName()).isEqualTo("Hive KPI 1m Sink");
        assertThat(sample.sinkType()).isEqualTo("hive");
        assertThat(sample.dataset()).isEqualTo("kpi_1m");
        assertThat(sample.windowKind()).isEqualTo("MIN_1");
        assertThat(sample.records()).isEqualTo(2);
        assertThat(sample.bytes()).isEqualTo(217);
        assertThat(sample.durationMs()).isGreaterThanOrEqualTo(0L);
        assertThat(sample.latencyP95Ms()).isGreaterThanOrEqualTo(sample.latencyP50Ms());
    }

    @Test
    void builds_sink_latency_sample_for_anomaly_events() {
        SinkLatencyProbe<AnomalyEvent> probe = new SinkLatencyProbe<>(
            "starrocks-cell-anomaly", "StarRocks Cell Anomaly Sink", "starrocks",
            "cell_anomaly_events", "ANOMALY", 1);

        probe.record(anomalyEvent());

        StageMetricSample sample = probe.metricSample(1_717_400_000_000L);

        assertThat(sample.sinkType()).isEqualTo("starrocks");
        assertThat(sample.dataset()).isEqualTo("cell_anomaly_events");
        assertThat(sample.records()).isEqualTo(1);
        assertThat(sample.bytes()).isGreaterThan(32L);
        assertThat(sample.durationMs()).isGreaterThanOrEqualTo(0L);
        assertThat(sample.latencyP95Ms()).isGreaterThanOrEqualTo(sample.latencyP50Ms());
    }

    @Test
    void tags_sink_latency_sample_with_run_metadata() {
        SinkLatencyProbe<CellKpi> probe = new SinkLatencyProbe<>(
            "starrocks-kpi-1m", "StarRocks KPI 1m Sink", "starrocks",
            "kpi_1m", "MIN_1", 2, new MetricRuntimeConfig("run-a", "starrocks", 4, true));

        probe.record(kpi(1000L, "a"));

        StageMetricSample sample = probe.metricSample(1_717_400_000_000L);

        assertThat(sample.runId()).isEqualTo("run-a");
        assertThat(sample.resultSink()).isEqualTo("starrocks");
        assertThat(sample.parallelism()).isEqualTo(4);
    }

    @Test
    void open_uses_disabled_noop_publisher_without_creating_real_producer() throws Exception {
        SinkLatencyProbe<CellKpi> probe = new SinkLatencyProbe<>(
            "starrocks-kpi-1m", "StarRocks KPI 1m Sink", "starrocks",
            "kpi_1m", "MIN_1", 1, new MetricRuntimeConfig("run-a", "starrocks", 4, false));

        probe.open(new org.apache.flink.configuration.Configuration());

        assertThat(publisher(probe).enabled()).isFalse();
    }

    @Test
    void publish_metric_is_best_effort_when_publisher_fails() throws Exception {
        SinkLatencyProbe<CellKpi> probe = new SinkLatencyProbe<>(
            "hive-kpi-1m", "Hive KPI 1m Sink", "hive", "kpi_1m", "MIN_1", 1);
        MetricSamplePublisher publisher = new MetricSamplePublisher();
        publisher.close();
        setPublisher(probe, publisher);
        probe.record(kpi(1000L, "a"));

        assertThatCode(() -> invokePublishMetric(probe, 1_717_400_000_000L))
            .doesNotThrowAnyException();
    }

    private static void setPublisher(SinkLatencyProbe<?> probe, MetricSamplePublisher publisher) throws Exception {
        Field field = SinkLatencyProbe.class.getDeclaredField("metricPublisher");
        field.setAccessible(true);
        field.set(probe, publisher);
    }

    private static MetricSamplePublisher publisher(SinkLatencyProbe<?> probe) throws Exception {
        Field field = SinkLatencyProbe.class.getDeclaredField("metricPublisher");
        field.setAccessible(true);
        return (MetricSamplePublisher) field.get(probe);
    }

    private static void invokePublishMetric(SinkLatencyProbe<?> probe, long nowMs) throws Exception {
        Method method = SinkLatencyProbe.class.getDeclaredMethod("publishMetric", long.class);
        method.setAccessible(true);
        method.invoke(probe, nowMs);
    }

    private static CellKpi kpi(long windowStartTs, String suffix) {
        return new CellKpi(
            windowStartTs,
            windowStartTs + 60000L,
            WindowKind.MIN_1,
            JoinQuality.JOINED,
            "site-" + suffix,
            "cell-" + suffix,
            "grid-" + suffix,
            1L,
            2L,
            3L,
            4L,
            5L,
            3.0f,
            4.0f,
            5.0f,
            6.0f,
            7.0f,
            8.0f,
            9.0f);
    }

    private static AnomalyEvent anomalyEvent() {
        return AnomalyEvent.newBuilder()
            .setDetectionTs(1_700_000_010_000L)
            .setEventTs(1_700_000_000_000L)
            .setEntityType(EntityType.CELL)
            .setEntityId("cell-a")
            .setWindowStartTs(1_699_999_880_000L)
            .setWindowEndTs(1_700_000_000_000L)
            .setImsi(null)
            .setSiteId("site-a")
            .setCellId("cell-a")
            .setGridId("grid-a")
            .setLatitude(31.2304)
            .setLongitude(121.4737)
            .setAnomalyType(AnomalyType.CELL_RADIO_BAD)
            .setSeverity(Severity.HIGH)
            .setRuleVersion("rules-v1")
            .setContextJson("{\"reason\":\"rsrp\"}")
            .build();
    }
}
