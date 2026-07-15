package com.fdb.job.anomaly;

import static org.assertj.core.api.Assertions.assertThat;

import com.fdb.common.avro.AnomalyEvent;
import com.fdb.common.avro.AnomalyType;
import com.fdb.common.avro.CellKpi;
import com.fdb.common.avro.EntityType;
import com.fdb.common.avro.JoinQuality;
import com.fdb.common.avro.WindowKind;
import com.fdb.job.config.RuleConfig;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

class CellKpiCepAnomalyDetectorTest {

    @Test
    void emits_after_three_consecutive_bad_one_minute_kpis() throws Exception {
        List<AnomalyEvent> output = run(List.of(
            kpi(0, -111f, 1f, 0.99f, 0.99f, 0.0f),
            kpi(60_000, -112f, 1f, 0.99f, 0.99f, 0.0f),
            kpi(120_000, -113f, 1f, 0.99f, 0.99f, 0.0f)));

        assertThat(output).hasSize(1);
        assertThat(output.get(0).getEntityType()).isEqualTo(EntityType.CELL);
        assertThat(output.get(0).getEntityId()).isEqualTo("CELL-001");
        assertThat(output.get(0).getAnomalyType()).isEqualTo(AnomalyType.CELL_RADIO_BAD);
    }

    @Test
    void normal_period_breaks_consecutive_cell_streak() throws Exception {
        List<AnomalyEvent> output = run(List.of(
            kpi(0, -111f, 1f, 0.99f, 0.99f, 0.0f),
            kpi(60_000, -90f, 10f, 0.99f, 0.99f, 0.0f),
            kpi(120_000, -113f, 1f, 0.99f, 0.99f, 0.0f)));

        assertThat(output).isEmpty();
    }

    @Test
    void active_cell_streak_emits_once_until_recovery() throws Exception {
        List<AnomalyEvent> output = run(List.of(
            kpi(0, -111f, 1f, 0.99f, 0.99f, 0.0f),
            kpi(60_000, -112f, 1f, 0.99f, 0.99f, 0.0f),
            kpi(120_000, -113f, 1f, 0.99f, 0.99f, 0.0f),
            kpi(180_000, -114f, 1f, 0.99f, 0.99f, 0.0f),
            kpi(240_000, -90f, 10f, 0.99f, 0.99f, 0.0f),
            kpi(300_000, -111f, 1f, 0.99f, 0.99f, 0.0f),
            kpi(360_000, -112f, 1f, 0.99f, 0.99f, 0.0f),
            kpi(420_000, -113f, 1f, 0.99f, 0.99f, 0.0f)));

        assertThat(output).hasSize(2);
    }

    private static List<AnomalyEvent> run(List<CellKpi> input) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<AnomalyEvent> output = new ArrayList<>();
        try (CloseableIterator<AnomalyEvent> it = CellKpiCepAnomalyDetector
            .detect(env.fromCollection(input, new GenericTypeInfo<>(CellKpi.class)), RuleConfig.defaults())
            .executeAndCollect()) {
            while (it.hasNext()) {
                output.add(it.next());
            }
        }
        return output;
    }

    private static CellKpi kpi(long startTs, float rsrp, float sinr, float attach, float ho, float drop) {
        return CellKpi.newBuilder()
            .setWindowStartTs(startTs)
            .setWindowEndTs(startTs + 60_000)
            .setWindowKind(WindowKind.MIN_1)
            .setJoinQuality(JoinQuality.JOINED)
            .setSiteId("SITE-001")
            .setCellId("CELL-001")
            .setGridId("wx4g0e")
            .setNumChrEvents(100)
            .setNumUsers(10)
            .setRsrpSampleCount(100)
            .setSinrSampleCount(100)
            .setAttachAttempts(100)
            .setAvgRsrp(rsrp)
            .setAvgSinr(sinr)
            .setAvgPrbUsageDl(0.5f)
            .setThroughputDlMbpsAvg(100f)
            .setDropRate(drop)
            .setHoSuccessRate(ho)
            .setAttachSuccessRate(attach)
            .build();
    }
}
