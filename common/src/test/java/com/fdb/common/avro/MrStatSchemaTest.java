package com.fdb.common.avro;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

class MrStatSchemaTest {

    @Test
    void roundtrip_mr_stat_preserves_fields() throws Exception {
        MrStat original = MrStat.newBuilder()
            .setSiteId("SITE-002")
            .setCellId("CELL-002-3")
            .setWindowStartTs(1714387200000L)
            .setWindowEndTs(1714387210000L)
            .setPrbUsageDl(0.78f)
            .setPrbUsageUl(0.42f)
            .setActiveUsers(150)
            .setAvgRsrp(-92.0f)
            .setAvgRsrq(-10.5f)
            .setAvgSinr(8.5f)
            .setAvgCqi(11.0f)
            .setAvgMcs(20.0f)
            .setAvgBler(0.05f)
            .setThroughputDlMbps(123.5f)
            .setThroughputUlMbps(34.2f)
            .setDroppedConnections(2)
            .setHandoverSuccess(45)
            .setHandoverFailure(3)
            .setPrachAttempt(120)
            .setPrachFailure(5)
            .setRrcEstabAttempt(80)
            .setRrcEstabSuccess(78)
            .setAvgLatencyMs(12.0f)
            .setPacketLossRate(0.001f)
            .build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        var encoder = EncoderFactory.get().binaryEncoder(out, null);
        new SpecificDatumWriter<>(MrStat.class).write(original, encoder);
        encoder.flush();

        MrStat decoded = new SpecificDatumReader<>(MrStat.class).read(null,
            DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(out.toByteArray()), null));

        assertThat(decoded).isEqualTo(original);
        assertThat(decoded.getPrbUsageDl()).isEqualTo(0.78f);
        assertThat(decoded.getActiveUsers()).isEqualTo(150);
    }
}
