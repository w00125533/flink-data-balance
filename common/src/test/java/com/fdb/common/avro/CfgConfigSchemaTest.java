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

class CfgConfigSchemaTest {

    @Test
    void roundtrip_cfg_config() throws Exception {
        CfgConfig config = CfgConfig.newBuilder()
            .setSiteId("SITE-001")
            .setCellId("CELL-001")
            .setEffectiveTs(1_000L)
            .setVersion(3L)
            .setCellType(CellType.NR_SA)
            .setBandwidthMhz(100)
            .setFrequencyBand("n78")
            .setArfcn(632448)
            .setMaxPowerDbm(49.0f)
            .setAzimuth(90)
            .setCenterLat(39.9)
            .setCenterLon(116.4)
            .setCoverageRadiusM(500)
            .setPci(100)
            .setTac(40001)
            .setEci(1000L)
            .setMcc("460")
            .setMnc("00")
            .setNumerology(1)
            .setMimoMode(MimoMode.MIMO_4x4)
            .setAntennaPorts(4)
            .setNssai(List.of(NssaiEntry.newBuilder().setSst(1).setSd("000001").build()))
            .setNeighborCells(List.of("CELL-002", "CELL-003"))
            .setTombstone(false)
            .build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        var encoder = EncoderFactory.get().binaryEncoder(out, null);
        new SpecificDatumWriter<>(CfgConfig.class).write(config, encoder);
        encoder.flush();

        CfgConfig decoded = new SpecificDatumReader<>(CfgConfig.class).read(null,
            DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(out.toByteArray()), null));

        assertThat(decoded).isEqualTo(config);
        assertThat(decoded.getCellId()).isEqualTo("CELL-001");
        assertThat(decoded.getNumerology()).isEqualTo(1);
        assertThat(decoded.getMimoMode()).isEqualTo(MimoMode.MIMO_4x4);
        assertThat(decoded.getNssai()).hasSize(1);
        assertThat(decoded.getNssai().get(0).getSst()).isEqualTo(1);
        assertThat(decoded.getNeighborCells()).containsExactly("CELL-002", "CELL-003");
        assertThat(decoded.getTombstone()).isFalse();
    }
}
