package com.fdb.job;

import com.fdb.common.avro.ChrEvent;
import com.fdb.common.avro.ChrEventType;
import com.fdb.common.avro.RatType;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ChrMinuteFactWindowFunctionTest {

    @Test
    void aggregates_chr_events_using_event_time_window_start() throws Exception {
        ChrMinuteFactWindowFunction function = new ChrMinuteFactWindowFunction();
        TimeWindow window = new TimeWindow(120_000L, 180_000L);
        List<ChrMinuteFact> output = new ArrayList<>();

        function.process("cell-a", context(function, window), List.of(
            chr("1", "imsi-1", ChrEventType.ATTACH, 0, -90.0f, 10.0f),
            chr("2", "imsi-1", ChrEventType.ATTACH, 7, null, 14.0f),
            chr("3", "imsi-2", ChrEventType.DATA_SESSION, 0, -100.0f, null)
        ), collector(output));

        assertThat(output).containsExactly(new ChrMinuteFact(
            "cell-a", "site-a", 120_000L, 3L, 2L, -190.0, 24.0, 2L, 1L, 2L, 2L));
    }

    private static ProcessWindowFunction<ChrEvent, ChrMinuteFact, String, TimeWindow>.Context context(
        ChrMinuteFactWindowFunction function,
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

    private static Collector<ChrMinuteFact> collector(List<ChrMinuteFact> output) {
        return new Collector<>() {
            @Override
            public void collect(ChrMinuteFact record) {
                output.add(record);
            }

            @Override
            public void close() {
            }
        };
    }

    private static ChrEvent chr(
        String chrId,
        String imsi,
        ChrEventType type,
        int resultCode,
        Float rsrp,
        Float sinr) {
        return ChrEvent.newBuilder()
            .setChrId(chrId)
            .setEventTs(120_001L)
            .setImsi(imsi)
            .setSiteId("site-a")
            .setCellId("cell-a")
            .setEventType(type)
            .setRatType(RatType.NR_SA)
            .setPci(123)
            .setTac(40001)
            .setEci(1234567890L)
            .setMcc("460")
            .setMnc("00")
            .setResultCode(resultCode)
            .setLatitude(39.9042)
            .setLongitude(116.4074)
            .setRsrp(rsrp)
            .setSinr(sinr)
            .build();
    }
}
