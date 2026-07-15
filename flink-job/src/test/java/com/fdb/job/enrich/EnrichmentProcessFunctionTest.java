package com.fdb.job.enrich;

import static org.assertj.core.api.Assertions.assertThat;

import com.fdb.common.avro.ChrEvent;
import com.fdb.common.avro.ChrEventType;
import com.fdb.common.avro.RatType;
import com.fdb.job.model.EnrichedChr;
import com.fdb.job.model.InputEnvelope;
import com.fdb.job.model.RoutedEnvelope;
import java.util.List;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class EnrichmentProcessFunctionTest {

    @Test
    void emits_enriched_chr_with_null_cfg_when_cfg_is_missing() throws Exception {
        try (var harness = harness()) {
            harness.open();
            ChrEvent chr = chr("chr-1");

            harness.processElement(new StreamRecord<>(
                new RoutedEnvelope(new InputEnvelope.ChrEnv(chr), 1), 1L));

            List<EnrichedChr> output = harness.extractOutputValues();
            assertThat(output).hasSize(1);
            assertThat(output.get(0).chrEvent()).isEqualTo(chr);
            assertThat(output.get(0).cfgConfig()).isNull();
            assertThat(harness.getSideOutput(EnrichmentProcessFunction.ENRICHMENT_LATE))
                .extracting(record -> record.getValue().getChrId().toString())
                .containsExactly("chr-1");
        }
    }

    private static KeyedOneInputStreamOperatorTestHarness<String, RoutedEnvelope, EnrichedChr> harness()
        throws Exception {
        KeyedOneInputStreamOperatorTestHarness<String, RoutedEnvelope, EnrichedChr> harness =
            new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new EnrichmentProcessFunction()),
                RoutedEnvelope::stateKey,
                Types.STRING);
        harness.setup(new KryoSerializer<>(EnrichedChr.class, new ExecutionConfig()));
        return harness;
    }

    private static ChrEvent chr(String id) {
        return ChrEvent.newBuilder()
            .setChrId(id)
            .setEventTs(1_000_000L)
            .setImsi("460001234567890")
            .setSiteId("SITE-001")
            .setCellId("CELL-001")
            .setEventType(ChrEventType.DATA_SESSION)
            .setRatType(RatType.LTE)
            .setPci(100)
            .setTac(40001)
            .setEci(1000L)
            .setMcc("460")
            .setMnc("00")
            .setResultCode(0)
            .setLatitude(39.9)
            .setLongitude(116.4)
            .build();
    }
}
