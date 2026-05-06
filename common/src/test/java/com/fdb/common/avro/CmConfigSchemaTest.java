package com.fdb.common.avro;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CmConfigSchemaTest {

    @Test
    void roundtrip_cm_config_preserves_nested_nssai_and_neighbors() throws Exception {
        CmConfig original = CmConfig.newBuilder()
            .setSiteId("SITE-003")
            .setCellId("CELL-003-1")
            .setEffectiveTs(1714387200000L)
            .setVersion(7L)
            .setCellType(CellType.NR_SA)
            .setBandwidthMhz(100)
            .setFrequencyBand("n78")
            .setArfcn(632448)
            .setMaxPowerDbm(49.0f)
            .setAzimuth(120)
            .setCenterLat(39.9100)
            .setCenterLon(116.4100)
            .setCoverageRadiusM(800)
            .setPci(456)
            .setTac(40002)
            .setEci(2345678901L)
            .setMcc("460")
            .setMnc("00")
            .setNumerology(1)
            .setMimoMode(MimoMode.MIMO_4x4)
            .setAntennaPorts(4)
            .setNssai(List.of(NssaiEntry.newBuilder().setSst(1).setSd("000001").build()))
            .setNeighborCells(List.of("CELL-003-2", "CELL-004-1"))
            .setTombstone(false)
            .build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        var encoder = EncoderFactory.get().binaryEncoder(out, null);
        new SpecificDatumWriter<>(CmConfig.class).write(original, encoder);
        encoder.flush();

        CmConfig decoded = new SpecificDatumReader<>(CmConfig.class).read(null,
            DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(out.toByteArray()), null));

        assertThat(decoded).isEqualTo(original);
        assertThat(decoded.getNssai()).hasSize(1);
        assertThat(decoded.getNssai().get(0).getSst()).isEqualTo(1);
        assertThat(decoded.getNeighborCells()).containsExactly("CELL-003-2", "CELL-004-1");
        assertThat(decoded.getTombstone()).isFalse();
    }
}
