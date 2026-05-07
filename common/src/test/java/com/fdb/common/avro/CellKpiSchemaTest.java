package com.fdb.common.avro;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

class CellKpiSchemaTest {

    @Test
    void roundtrip_cell_kpi_preserves_window_kind() throws Exception {
        CellKpi original = CellKpi.newBuilder()
            .setWindowStartTs(1714387200000L)
            .setWindowEndTs(1714387260000L)
            .setWindowKind(WindowKind.MIN_1)
            .setSiteId("SITE-001")
            .setCellId("CELL-001-1")
            .setGridId("wx4g0ec")
            .setNumChrEvents(1234L)
            .setNumUsers(456L)
            .setAvgRsrp(-93.5f)
            .setAvgSinr(10.2f)
            .setAvgPrbUsageDl(0.65f)
            .setThroughputDlMbpsAvg(120.0f)
            .setDropRate(0.02f)
            .setHoSuccessRate(0.97f)
            .setAttachSuccessRate(0.99f)
            .build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        var encoder = EncoderFactory.get().binaryEncoder(out, null);
        new SpecificDatumWriter<>(CellKpi.class).write(original, encoder);
        encoder.flush();

        CellKpi decoded = new SpecificDatumReader<>(CellKpi.class).read(null,
            DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(out.toByteArray()), null));

        assertThat(decoded).isEqualTo(original);
        assertThat(decoded.getWindowKind()).isEqualTo(WindowKind.MIN_1);
        assertThat(decoded.getNumChrEvents()).isEqualTo(1234L);
    }
}
