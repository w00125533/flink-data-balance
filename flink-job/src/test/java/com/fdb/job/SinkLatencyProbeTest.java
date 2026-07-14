package com.fdb.job;

import com.fdb.common.avro.AnomalyEvent;
import com.fdb.common.avro.AnomalyType;
import com.fdb.common.avro.CellKpi;
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
            .setImsi("460001234567890")
            .setSiteId("site-a")
            .setCellId("cell-a")
            .setGridId("grid-a")
            .setLatitude(31.2304)
            .setLongitude(121.4737)
            .setAnomalyType(AnomalyType.LOW_SIGNAL)
            .setSeverity(Severity.HIGH)
            .setRuleVersion("rules-v1")
            .setContextJson("{\"reason\":\"rsrp\"}")
            .build();
    }
}
