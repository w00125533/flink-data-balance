package com.fdb.job;

import com.fdb.job.coordinator.HeartbeatPayload;
import com.fdb.common.summary.SummarySwitch;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class VBucketLoadMeter extends KeyedProcessFunction<Integer, RoutedEnvelope, RoutedEnvelope> {
    private static final Logger log = LoggerFactory.getLogger(VBucketLoadMeter.class);
    public static final OutputTag<String> HEARTBEATS = new OutputTag<>("lb-heartbeats") {};
    private static final long INTERVAL_MS = 5_000;
    private transient ValueState<Long> count;
    private transient ValueState<Long> timer;
    private transient MapState<String, Long> routeKeyCounts;
    private transient boolean summaryEnabled;

    @Override
    public void open(Configuration parameters) {
        count = getRuntimeContext().getState(new ValueStateDescriptor<>("event-count", Long.class));
        timer = getRuntimeContext().getState(new ValueStateDescriptor<>("heartbeat-timer", Long.class));
        routeKeyCounts = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("route-key-counts", String.class, Long.class));
        summaryEnabled = SummarySwitch.enabled();
    }

    @Override
    public void processElement(RoutedEnvelope value, Context ctx, Collector<RoutedEnvelope> out) throws Exception {
        count.update((count.value() == null ? 0 : count.value()) + 1);
        incrementRouteKeyCount(routeKeyCounts, value);
        if (timer.value() == null) {
            long next = ctx.timerService().currentProcessingTime() + INTERVAL_MS;
            timer.update(next); ctx.timerService().registerProcessingTimeTimer(next);
        }
        out.collect(value);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RoutedEnvelope> out) throws Exception {
        long events = count.value() == null ? 0 : count.value();
        Map<String, Long> intervalRouteKeyCounts = routeKeyCountsSnapshot();
        HeartbeatPayload heartbeat = heartbeatPayload(getRuntimeContext().getIndexOfThisSubtask(),
            events, ctx.getCurrentKey(), timestamp, intervalRouteKeyCounts);
        ctx.output(HEARTBEATS, heartbeat.toCsv());
        if (summaryEnabled) {
            log.info(SummarySwitch.format("flink-heartbeat", "vbucket", ctx.getCurrentKey()));
            log.info(SummarySwitch.format("flink-heartbeat", "events_last_interval", events));
            log.info(SummarySwitch.format("flink-heartbeat", "eps", String.format("%.1f", heartbeat.getEps())));
            log.info(SummarySwitch.format("flink-heartbeat", "hotspot_site", heartbeat.getHotspotSiteId()));
        }
        count.update(0L);
        routeKeyCounts.clear();
        long next = timestamp + INTERVAL_MS;
        timer.update(next); ctx.timerService().registerProcessingTimeTimer(next);
    }

    static String hotspotRouteKey(RoutedEnvelope value) {
        return value.stateKey();
    }

    static void incrementRouteKeyCount(Map<String, Long> counts, RoutedEnvelope value) {
        counts.merge(hotspotRouteKey(value), 1L, Long::sum);
    }

    static HeartbeatPayload heartbeatPayload(int subtaskId, long events, int vbucketId, long timestamp,
                                             Map<String, Long> routeKeyCounts) {
        double eps = events / (INTERVAL_MS / 1000.0);
        double[] buckets = new double[1024];
        buckets[vbucketId] = eps;
        return new HeartbeatPayload(subtaskId, eps, buckets, timestamp, hottestRouteKey(routeKeyCounts), vbucketId);
    }

    static String hottestRouteKey(Map<String, Long> routeKeyCounts) {
        return routeKeyCounts.entrySet().stream()
            .max(Comparator.<Map.Entry<String, Long>>comparingLong(Map.Entry::getValue)
                .thenComparing(Map.Entry::getKey))
            .map(Map.Entry::getKey)
            .orElse(null);
    }

    private void incrementRouteKeyCount(MapState<String, Long> counts, RoutedEnvelope value) throws Exception {
        String routeKey = hotspotRouteKey(value);
        Long current = counts.get(routeKey);
        counts.put(routeKey, current == null ? 1L : current + 1L);
    }

    private Map<String, Long> routeKeyCountsSnapshot() throws Exception {
        Map<String, Long> snapshot = new HashMap<>();
        for (Map.Entry<String, Long> entry : routeKeyCounts.entries()) {
            snapshot.put(entry.getKey(), entry.getValue());
        }
        return snapshot;
    }
}
