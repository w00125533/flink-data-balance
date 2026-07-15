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
