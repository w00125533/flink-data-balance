package com.fdb.common.avro;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

class PmStatSchemaTest {

    @Test
    void roundtrip_pm_stat() throws Exception {
        PmStat original = PmStat.newBuilder()
            .setSiteId("SITE-001")
            .setCellId("CELL-001")
            .setWindowStartTs(1_000L)
            .setWindowEndTs(11_000L)
            .setEventTs(6_000L)
            .setPrbUsageDl(0.65f)
            .setPrbUsageUl(0.25f)
            .setActiveUsers(42)
            .setAvgRsrp(-92.0f)
            .setAvgRsrq(-8.0f)
            .setAvgSinr(12.0f)
            .setAvgCqi(10.0f)
            .setAvgMcs(18.0f)
            .setAvgBler(0.02f)
            .setThroughputDlMbps(120.0f)
            .setThroughputUlMbps(30.0f)
            .setDroppedConnections(1)
            .setHandoverSuccess(18)
            .setHandoverFailure(2)
            .setPrachAttempt(9)
            .setPrachFailure(1)
            .setRrcEstabAttempt(20)
            .setRrcEstabSuccess(19)
            .setAvgLatencyMs(18.0f)
            .setPacketLossRate(0.001f)
            .build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        var encoder = EncoderFactory.get().binaryEncoder(out, null);
        new SpecificDatumWriter<>(PmStat.class).write(original, encoder);
        encoder.flush();

        PmStat decoded = new SpecificDatumReader<>(PmStat.class).read(null,
            DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(out.toByteArray()), null));

        assertThat(decoded).isEqualTo(original);
        assertThat(decoded.getCellId()).isEqualTo("CELL-001");
        assertThat(decoded.getEventTs()).isEqualTo(6_000L);
        assertThat(decoded.getPrbUsageDl()).isEqualTo(0.65f);
        assertThat(decoded.getActiveUsers()).isEqualTo(42);
    }
}
