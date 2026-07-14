package com.fdb.job.kpi;

import com.fdb.job.model.PmMinuteFact;
import com.fdb.common.avro.PmStat;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class PmMinuteFactWindowFunctionTest {

    private static final long MINUTE = 60_000L;

    @Test
    void aggregates_pm_rows_using_event_time_window_start() throws Exception {
        PmMinuteFactWindowFunction function = new PmMinuteFactWindowFunction();
        TimeWindow window = new TimeWindow(MINUTE, 2 * MINUTE);
        List<PmMinuteFact> output = new ArrayList<>();

        function.process("cell-a", context(function, window), List.of(
            pm(0L, 10_000L, 0.5f, 100.0f, 1, 8, 2),
            pm(10_000L, 20_000L, 0.7f, 120.0f, 2, 9, 1)
        ), collector(output));

        assertThat(output).hasSize(1);
        PmMinuteFact fact = output.get(0);
        assertThat(fact.cellId()).isEqualTo("cell-a");
        assertThat(fact.siteId()).isEqualTo("site-a");
        assertThat(fact.minuteTs()).isEqualTo(60_000L);
        assertThat(fact.pmWindowCount()).isEqualTo(2L);
        assertThat(fact.prbUsageDlSum()).isCloseTo(1.20, within(0.00001));
        assertThat(fact.throughputDlMbpsSum()).isEqualTo(220.0);
        assertThat(fact.dropCount()).isEqualTo(3L);
        assertThat(fact.handoverSuccess()).isEqualTo(17L);
        assertThat(fact.handoverFailure()).isEqualTo(3L);
    }

    @Test
    void boundary_pm_row_ending_at_next_minute_is_assigned_to_source_minute() throws Exception {
        PmStat boundaryPm = pm(0L, MINUTE, 0.6f, 110.0f, 0, 4, 1);
        long assignedTs = MINUTE - 1;
        TimeWindow sourceMinute = new TimeWindow(0L, MINUTE);
        List<PmMinuteFact> output = new ArrayList<>();

        new PmMinuteFactWindowFunction().process(
            "cell-a",
            context(new PmMinuteFactWindowFunction(), windowFor(assignedTs)),
            List.of(boundaryPm),
            collector(output));

        assertThat(windowFor(assignedTs)).isEqualTo(sourceMinute);
        assertThat(output).hasSize(1);
        assertThat(output.get(0).minuteTs()).isEqualTo(0L);
    }

    private static TimeWindow windowFor(long timestamp) {
        long start = timestamp - Math.floorMod(timestamp, MINUTE);
        return new TimeWindow(start, start + MINUTE);
    }

    private static ProcessWindowFunction<PmStat, PmMinuteFact, String, TimeWindow>.Context context(
        PmMinuteFactWindowFunction function,
        TimeWindow window) {
        return function.new Context() {
            @Override
            public TimeWindow window() {
                return window;
            }

            @Override
            public long currentProcessingTime() {
                return 0L;
            }

            @Override
            public long currentWatermark() {
                return 0L;
            }

            @Override
            public KeyedStateStore windowState() {
                return null;
            }

            @Override
            public KeyedStateStore globalState() {
                return null;
            }

            @Override
            public <X> void output(OutputTag<X> outputTag, X value) {
            }
        };
    }

    private static Collector<PmMinuteFact> collector(List<PmMinuteFact> output) {
        return new Collector<>() {
            @Override
            public void collect(PmMinuteFact record) {
                output.add(record);
            }

            @Override
            public void close() {
            }
        };
    }

    private static PmStat pm(
        long start,
        long end,
        float prbUsageDl,
        float throughputDl,
        int dropped,
        int hoSuccess,
        int hoFailure) {
        return PmStat.newBuilder()
            .setSiteId("site-a")
            .setCellId("cell-a")
            .setWindowStartTs(start)
            .setWindowEndTs(end)
            .setPrbUsageDl(prbUsageDl)
            .setPrbUsageUl(0.25f)
            .setActiveUsers(42)
            .setAvgRsrp(-92.0f)
            .setAvgRsrq(-8.0f)
            .setAvgSinr(12.0f)
            .setAvgCqi(10.0f)
            .setAvgMcs(18.0f)
            .setAvgBler(0.02f)
            .setThroughputDlMbps(throughputDl)
            .setThroughputUlMbps(30.0f)
            .setDroppedConnections(dropped)
            .setHandoverSuccess(hoSuccess)
            .setHandoverFailure(hoFailure)
            .setPrachAttempt(9)
            .setPrachFailure(1)
            .setRrcEstabAttempt(20)
            .setRrcEstabSuccess(19)
            .setAvgLatencyMs(18.0f)
            .setPacketLossRate(0.001f)
            .build();
    }
}
