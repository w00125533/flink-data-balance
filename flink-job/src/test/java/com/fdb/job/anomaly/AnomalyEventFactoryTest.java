package com.fdb.job.anomaly;

import static org.assertj.core.api.Assertions.assertThat;

import com.fdb.common.avro.AnomalyEvent;
import com.fdb.common.avro.AnomalyType;
import com.fdb.common.avro.EntityType;
import com.fdb.common.avro.Severity;
import org.junit.jupiter.api.Test;

class AnomalyEventFactoryTest {

    @Test
    void builds_cell_event_without_user_or_coordinate_requirements() {
        AnomalyEvent event = AnomalyEventFactory.fromEvaluation(
            evaluation(EntityType.CELL, "CELL-001", "avgRsrp", AnomalyType.CELL_RADIO_BAD, -115d));

        assertThat(event.getEntityType()).isEqualTo(EntityType.CELL);
        assertThat(event.getEntityId()).isEqualTo("CELL-001");
        assertThat(event.getCellId()).isEqualTo("CELL-001");
        assertThat(event.getImsi()).isNull();
        assertThat(event.getLatitude()).isNull();
        assertThat(event.getWindowStartTs()).isEqualTo(1_000L);
        assertThat(event.getWindowEndTs()).isEqualTo(61_000L);
        assertThat(event.getRuleVersion()).isEqualTo("v1.0");
        assertThat(event.getSourceEventTsAvg()).isZero();
        assertThat(event.getSourceEventCount()).isZero();
    }

    @Test
    void builds_user_event_with_imsi_entity_id() {
        AnomalyEvent event = AnomalyEventFactory.fromEvaluation(
            evaluation(EntityType.USER, "460001234567890", "latencyMs", AnomalyType.USER_QOE_BAD, 900d));

        assertThat(event.getEntityType()).isEqualTo(EntityType.USER);
        assertThat(event.getEntityId()).isEqualTo("460001234567890");
        assertThat(event.getImsi()).isEqualTo("460001234567890");
        assertThat(event.getAnomalyType()).isEqualTo(AnomalyType.USER_QOE_BAD);
    }

    @Test
    void copies_source_event_time_statistics_from_evaluation() {
        AnomalyEvent event = AnomalyEventFactory.fromEvaluation(new AnomalyRuleEvaluation(
            EntityType.CELL, "CELL-001", "avgRsrp", true,
            1_000L, 61_000L, 61_000L,
            "SITE-001", "CELL-001", null,
            "wx4g0e", null, null,
            AnomalyType.CELL_RADIO_BAD, Severity.HIGH, "v1.0",
            "avgRsrp", -110d, -115d,
            "{\"metric\":\"avgRsrp\"}",
            31_000L, 1_000L, 59_000L, 3L));

        assertThat(event.getEventTs()).isEqualTo(61_000L);
        assertThat(event.getSourceEventTsAvg()).isEqualTo(31_000L);
        assertThat(event.getSourceEventTsMin()).isEqualTo(1_000L);
        assertThat(event.getSourceEventTsMax()).isEqualTo(59_000L);
        assertThat(event.getSourceEventCount()).isEqualTo(3L);
    }

    @Test
    void default_evaluation_constructor_does_not_invent_source_event_time_statistics() {
        AnomalyEvent event = AnomalyEventFactory.fromEvaluation(
            evaluation(EntityType.USER, "460001234567890", "latencyMs", AnomalyType.USER_QOE_BAD, 900d));

        assertThat(event.getEventTs()).isEqualTo(61_000L);
        assertThat(event.getSourceEventTsAvg()).isZero();
        assertThat(event.getSourceEventTsMin()).isZero();
        assertThat(event.getSourceEventTsMax()).isZero();
        assertThat(event.getSourceEventCount()).isZero();
    }

    private static AnomalyRuleEvaluation evaluation(
        EntityType entityType,
        String entityId,
        String dimension,
        AnomalyType anomalyType,
        double observedValue) {
        return new AnomalyRuleEvaluation(
            entityType, entityId, dimension, true,
            1_000L, 61_000L, 61_000L,
            "SITE-001", "CELL-001", entityType == EntityType.USER ? entityId : null,
            "wx4g0e", null, null,
            anomalyType, Severity.HIGH, "v1.0",
            dimension, -1d, observedValue,
            "{\"metric\":\"" + dimension + "\"}");
    }
}
