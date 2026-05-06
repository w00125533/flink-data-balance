package com.fdb.common.avro;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

class ChrEventSchemaTest {

    @Test
    void roundtrip_chr_event_preserves_all_fields() throws Exception {
        ChrEvent original = ChrEvent.newBuilder()
            .setChrId("chr-1")
            .setEventTs(1714387200000L)
            .setImsi("460001234567890")
            .setSiteId("SITE-001")
            .setCellId("CELL-001-1")
            .setEventType(ChrEventType.DATA_SESSION)
            .setRatType(RatType.NR_SA)
            .setPci(123)
            .setTac(40001)
            .setEci(1234567890L)
            .setMcc("460")
            .setMnc("00")
            .setResultCode(0)
            .setLatitude(39.9042)
            .setLongitude(116.4074)
            .setRsrp(-95.5f)
            .setSinr(12.0f)
            .build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<ChrEvent> writer = new SpecificDatumWriter<>(ChrEvent.class);
        var encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(original, encoder);
        encoder.flush();

        DatumReader<ChrEvent> reader = new SpecificDatumReader<>(ChrEvent.class);
        ChrEvent decoded = reader.read(null,
            DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(out.toByteArray()), null));

        assertThat(decoded).isEqualTo(original);
        assertThat(decoded.getEventType()).isEqualTo(ChrEventType.DATA_SESSION);
        assertThat(decoded.getRatType()).isEqualTo(RatType.NR_SA);
        assertThat(decoded.getRsrp()).isEqualTo(-95.5f);
    }
}
