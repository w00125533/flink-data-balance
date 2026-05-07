package com.fdb.common.avro;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

class TopologyRecordSchemaTest {

    @Test
    void roundtrip_topology_record() throws Exception {
        TopologyRecord original = TopologyRecord.newBuilder()
            .setSiteId("SITE-001")
            .setCellId("SITE-001-1")
            .setSiteLat(39.9042)
            .setSiteLon(116.4074)
            .setCellType(TopoCellType.NR_SA)
            .setCellIndex(1)
            .setPci(100)
            .setTac(40001)
            .setEci(123456789L)
            .setMcc("460")
            .setMnc("00")
            .setFrequencyBand("n78")
            .setArfcn(632448)
            .setBandwidthMhz(100)
            .setAzimuth(120)
            .setCoverageRadiusM(500)
            .setMaxPowerDbm(49.0f)
            .setVersion(1L)
            .build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        var encoder = EncoderFactory.get().binaryEncoder(out, null);
        new SpecificDatumWriter<>(TopologyRecord.class).write(original, encoder);
        encoder.flush();

        TopologyRecord decoded = new SpecificDatumReader<>(TopologyRecord.class).read(null,
            DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(out.toByteArray()), null));

        assertThat(decoded).isEqualTo(original);
        assertThat(decoded.getSiteId()).isEqualTo("SITE-001");
        assertThat(decoded.getCellType()).isEqualTo(TopoCellType.NR_SA);
    }
}
