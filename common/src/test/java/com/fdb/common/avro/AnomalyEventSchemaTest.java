package com.fdb.common.avro;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

class AnomalyEventSchemaTest {

    @Test
    void roundtrip_cell_anomaly_event_with_nullable_user_and_coordinate_context() throws Exception {
        AnomalyEvent original = AnomalyEvent.newBuilder()
            .setDetectionTs(1714387210000L)
            .setEventTs(1714387200000L)
            .setEntityType(EntityType.CELL)
            .setEntityId("CELL-001-1")
            .setWindowStartTs(1714387080000L)
            .setWindowEndTs(1714387200000L)
            .setImsi(null)
            .setSiteId("SITE-001")
            .setCellId("CELL-001-1")
            .setGridId(null)
            .setLatitude(null)
            .setLongitude(null)
            .setAnomalyType(AnomalyType.CELL_RADIO_BAD)
            .setSeverity(Severity.LOW)
            .setRuleVersion("v1.0")
            .setContextJson("{\"metric\":\"avgRsrp\"}")
            .build();

        AnomalyEvent decoded = roundtrip(original);

        assertThat(decoded).isEqualTo(original);
        assertThat(decoded.getEntityType()).isEqualTo(EntityType.CELL);
        assertThat(decoded.getEntityId()).isEqualTo("CELL-001-1");
        assertThat(decoded.getAnomalyType()).isEqualTo(AnomalyType.CELL_RADIO_BAD);
        assertThat(decoded.getImsi()).isNull();
        assertThat(decoded.getLatitude()).isNull();
    }

    @Test
    void roundtrip_user_anomaly_event() throws Exception {
        AnomalyEvent original = AnomalyEvent.newBuilder()
            .setDetectionTs(1714387210000L)
            .setEventTs(1714387200000L)
            .setEntityType(EntityType.USER)
            .setEntityId("460001234567890")
            .setWindowStartTs(1714386600000L)
            .setWindowEndTs(1714387200000L)
            .setImsi("460001234567890")
            .setSiteId("SITE-001")
            .setCellId("CELL-001-1")
            .setGridId("wx4g0ec")
            .setLatitude(39.9042)
            .setLongitude(116.4074)
            .setAnomalyType(AnomalyType.USER_QOE_BAD)
            .setSeverity(Severity.MEDIUM)
            .setRuleVersion("v1.0")
            .setContextJson("{\"metric\":\"latencyMs\"}")
            .build();

        AnomalyEvent decoded = roundtrip(original);

        assertThat(decoded.getEntityType()).isEqualTo(EntityType.USER);
        assertThat(decoded.getEntityId()).isEqualTo("460001234567890");
        assertThat(decoded.getAnomalyType()).isEqualTo(AnomalyType.USER_QOE_BAD);
    }

    private static AnomalyEvent roundtrip(AnomalyEvent original) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        var encoder = EncoderFactory.get().binaryEncoder(out, null);
        new SpecificDatumWriter<>(AnomalyEvent.class).write(original, encoder);
        encoder.flush();
        return new SpecificDatumReader<>(AnomalyEvent.class).read(null,
            DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(out.toByteArray()), null));
    }
}
