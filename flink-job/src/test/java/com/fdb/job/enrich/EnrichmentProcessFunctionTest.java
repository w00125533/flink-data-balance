package com.fdb.job.enrich;

import static org.assertj.core.api.Assertions.assertThat;

import com.fdb.common.avro.ChrEvent;
import com.fdb.common.avro.ChrEventType;
import com.fdb.common.avro.PmStat;
import com.fdb.common.avro.RatType;
import com.fdb.job.model.EnrichedChr;
import com.fdb.job.model.InputEnvelope;
import com.fdb.job.model.RoutedEnvelope;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class EnrichmentProcessFunctionTest {

    @Test
    void stores_only_latest_pm_in_value_state_instead_of_pm_ring_list_state() {
        List<Field> pmStateFields = Arrays.stream(EnrichmentProcessFunction.class.getDeclaredFields())
            .filter(field -> field.getName().toLowerCase(Locale.ROOT).contains("pm"))
            .toList();

        assertThat(pmStateFields)
            .noneMatch(field -> ListState.class.isAssignableFrom(field.getType()))
            .anyMatch(field -> ValueState.class.isAssignableFrom(field.getType()));
    }

    @Test
    void enrichment_keyed_state_descriptors_enable_ttl() {
        assertThat(EnrichmentProcessFunction.cfgStateDescriptor().getTtlConfig().isEnabled()).isTrue();
        assertThat(EnrichmentProcessFunction.latestPmStateDescriptor().getTtlConfig().isEnabled()).isTrue();
    }

    @Test
    void enriches_chr_with_latest_pm_state() throws Exception {
        try (var harness = harness()) {
            harness.open();
            PmStat older = pm(0L, 10_000L, 0.5f);
            PmStat latest = pm(10_000L, 20_000L, 0.7f);

            harness.processElement(new StreamRecord<>(
                new RoutedEnvelope(new InputEnvelope.PmEnv(older), 1), 1L));
            harness.processElement(new StreamRecord<>(
                new RoutedEnvelope(new InputEnvelope.PmEnv(latest), 1), 2L));
            harness.processElement(new StreamRecord<>(
                new RoutedEnvelope(new InputEnvelope.ChrEnv(chr("chr-with-pm")), 1), 3L));

            List<EnrichedChr> output = harness.extractOutputValues();
            assertThat(output).hasSize(1);
            assertThat(output.get(0).latestPm()).isEqualTo(latest);
        }
    }

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

    private static PmStat pm(long start, long end, float prbUsageDl) {
        return PmStat.newBuilder()
            .setSiteId("SITE-001")
            .setCellId("CELL-001")
            .setWindowStartTs(start)
            .setWindowEndTs(end)
            .setEventTs(start + ((end - start) / 2L))
            .setPrbUsageDl(prbUsageDl)
            .setPrbUsageUl(0.25f)
            .setActiveUsers(42)
            .setAvgRsrp(-92.0f)
            .setAvgRsrq(-8.0f)
            .setAvgSinr(12.0f)
            .setAvgCqi(10.0f)
            .setAvgMcs(18.0f)
            .setAvgBler(0.02f)
            .setThroughputDlMbps(100.0f)
            .setThroughputUlMbps(30.0f)
            .setDroppedConnections(0)
            .setHandoverSuccess(4)
            .setHandoverFailure(1)
            .setPrachAttempt(9)
            .setPrachFailure(1)
            .setRrcEstabAttempt(20)
            .setRrcEstabSuccess(19)
            .setAvgLatencyMs(18.0f)
            .setPacketLossRate(0.001f)
            .build();
    }
}
