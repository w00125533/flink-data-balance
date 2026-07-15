package com.fdb.job.anomaly;

import static org.assertj.core.api.Assertions.assertThat;

import com.fdb.common.avro.AnomalyEvent;
import com.fdb.common.avro.AnomalyType;
import com.fdb.common.avro.ChrEvent;
import com.fdb.common.avro.ChrEventType;
import com.fdb.common.avro.EntityType;
import com.fdb.common.avro.RatType;
import com.fdb.job.config.RuleConfig;
import com.fdb.job.model.EnrichedChr;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

class UserEventCepAnomalyDetectorTest {

    @Test
    void emits_user_failure_after_three_attach_failures_within_window() throws Exception {
        List<AnomalyEvent> output = run(List.of(
            enriched(chr("a", 0, ChrEventType.ATTACH, 1, -90f, 10f, 10f)),
            enriched(chr("b", 60_000, ChrEventType.ATTACH, 2, -90f, 10f, 10f)),
            enriched(chr("c", 120_000, ChrEventType.ATTACH, 3, -90f, 10f, 10f))));

        assertThat(output).hasSize(1);
        assertThat(output.get(0).getEntityType()).isEqualTo(EntityType.USER);
        assertThat(output.get(0).getEntityId()).isEqualTo("460001234567890");
        assertThat(output.get(0).getAnomalyType()).isEqualTo(AnomalyType.USER_FAILURE);
    }

    @Test
    void success_event_breaks_user_failure_streak() throws Exception {
        List<AnomalyEvent> output = run(List.of(
            enriched(chr("a", 0, ChrEventType.ATTACH, 1, -90f, 10f, 10f)),
            enriched(chr("b", 60_000, ChrEventType.ATTACH, 0, -90f, 10f, 10f)),
            enriched(chr("c", 120_000, ChrEventType.ATTACH, 3, -90f, 10f, 10f))));

        assertThat(output).isEmpty();
    }

    @Test
    void emits_user_qoe_for_three_high_latency_events() throws Exception {
        List<AnomalyEvent> output = run(List.of(
            enriched(chr("a", 0, ChrEventType.DATA_SESSION, 0, -90f, 10f, 600f)),
            enriched(chr("b", 60_000, ChrEventType.DATA_SESSION, 0, -90f, 10f, 700f)),
            enriched(chr("c", 120_000, ChrEventType.DATA_SESSION, 0, -90f, 10f, 800f))));

        assertThat(output).extracting(AnomalyEvent::getAnomalyType)
            .containsExactly(AnomalyType.USER_QOE_BAD);
    }

    @Test
    void skips_records_with_blank_imsi() throws Exception {
        ChrEvent event = ChrEvent.newBuilder(chr("a", 0, ChrEventType.ATTACH, 1, -90f, 10f, 10f))
            .setImsi("")
            .build();

        assertThat(run(List.of(enriched(event), enriched(event), enriched(event)))).isEmpty();
    }

    private static List<AnomalyEvent> run(List<EnrichedChr> input) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<AnomalyEvent> output = new ArrayList<>();
        try (CloseableIterator<AnomalyEvent> it = UserEventCepAnomalyDetector
            .detect(env.fromCollection(input, new GenericTypeInfo<>(EnrichedChr.class)), RuleConfig.defaults())
            .executeAndCollect()) {
            while (it.hasNext()) {
                output.add(it.next());
            }
        }
        return output;
    }

    private static EnrichedChr enriched(ChrEvent chr) {
        return new EnrichedChr(chr, null, null);
    }

    private static ChrEvent chr(
        String id, long offsetMs, ChrEventType type, int resultCode, Float rsrp, Float sinr, Float latencyMs) {
        ChrEvent.Builder builder = ChrEvent.newBuilder()
            .setChrId(id)
            .setEventTs(1_000_000L + offsetMs)
            .setImsi("460001234567890")
            .setSiteId("SITE-001")
            .setCellId("CELL-001")
            .setEventType(type)
            .setRatType(RatType.LTE)
            .setPci(100)
            .setTac(40001)
            .setEci(1000L)
            .setMcc("460")
            .setMnc("00")
            .setResultCode(resultCode)
            .setLatitude(39.9)
            .setLongitude(116.4)
            .setGridId("wx4g0e");
        if (rsrp != null) {
            builder.setRsrp(rsrp);
        }
        if (sinr != null) {
            builder.setSinr(sinr);
        }
        if (latencyMs != null) {
            builder.setLatencyMs(latencyMs);
        }
        return builder.build();
    }
}
