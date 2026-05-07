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
    void roundtrip_anomaly_event() throws Exception {
        AnomalyEvent original = AnomalyEvent.newBuilder()
            .setDetectionTs(1714387210000L)
            .setEventTs(1714387200000L)
            .setImsi("460001234567890")
            .setSiteId("SITE-001")
            .setCellId("CELL-001-1")
            .setGridId("wx4g0ec")
            .setLatitude(39.9042)
            .setLongitude(116.4074)
            .setAnomalyType(AnomalyType.LOW_SIGNAL)
            .setSeverity(Severity.LOW)
            .setRuleVersion("v1.0")
            .setContextJson("{\"rsrp\":-115}")
            .build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        var encoder = EncoderFactory.get().binaryEncoder(out, null);
        new SpecificDatumWriter<>(AnomalyEvent.class).write(original, encoder);
        encoder.flush();

        AnomalyEvent decoded = new SpecificDatumReader<>(AnomalyEvent.class).read(null,
            DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(out.toByteArray()), null));

        assertThat(decoded).isEqualTo(original);
        assertThat(decoded.getAnomalyType()).isEqualTo(AnomalyType.LOW_SIGNAL);
        assertThat(decoded.getSeverity()).isEqualTo(Severity.LOW);
    }
}
