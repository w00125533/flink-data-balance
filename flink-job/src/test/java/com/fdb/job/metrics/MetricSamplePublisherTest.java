package com.fdb.job.metrics;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class MetricSamplePublisherTest {

    @Test
    void parses_disabled_flags_from_environment_values() {
        for (String disabled : new String[] {"false", "0", "no", "off"}) {
            assertThat(MetricSamplePublisher.metricsEnabled(
                Map.of("FDB_METRICS_ENABLED", disabled), new Properties()))
                .as(disabled)
                .isFalse();
        }
    }

    @Test
    void uses_system_property_when_environment_is_absent() {
        String previous = System.getProperty("fdb.metrics.enabled");
        try {
            System.setProperty("fdb.metrics.enabled", "false");

            assertThat(MetricSamplePublisher.metricsEnabled(Map.of(), System.getProperties())).isFalse();
        } finally {
            if (previous == null) {
                System.clearProperty("fdb.metrics.enabled");
            } else {
                System.setProperty("fdb.metrics.enabled", previous);
            }
        }
    }

    @Test
    void environment_value_takes_precedence_over_system_property() {
        Properties properties = new Properties();
        properties.setProperty("fdb.metrics.enabled", "false");

        assertThat(MetricSamplePublisher.metricsEnabled(
            Map.of("FDB_METRICS_ENABLED", "on"), properties)).isTrue();
    }

    @Test
    void producer_properties_bound_kafka_waits_for_best_effort_metrics() {
        Properties properties = MetricSamplePublisher.producerProperties(
            "broker:9092",
            Map.of(),
            new Properties());

        assertThat(properties.getProperty("bootstrap.servers")).isEqualTo("broker:9092");
        assertThat(properties.getProperty("delivery.timeout.ms")).isEqualTo("10000");
        assertThat(properties.getProperty("request.timeout.ms")).isEqualTo("5000");
        assertThat(properties.getProperty("max.block.ms")).isEqualTo("5000");
        assertThat(properties.getProperty("retries")).isEqualTo("3");
    }

    @Test
    void parses_close_timeout_from_environment_values() {
        assertThat(MetricSamplePublisher.closeTimeoutMs(
            Map.of("FDB_METRICS_PRODUCER_CLOSE_TIMEOUT_MS", "2500"), new Properties()))
            .isEqualTo(2500L);

        assertThat(MetricSamplePublisher.closeTimeoutMs(
            Map.of("FDB_METRICS_PRODUCER_CLOSE_TIMEOUT_MS", "invalid"), new Properties()))
            .isEqualTo(5000L);
    }
}
