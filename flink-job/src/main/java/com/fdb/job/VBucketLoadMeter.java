package com.fdb.job;

import com.fdb.job.coordinator.HeartbeatPayload;
import com.fdb.common.summary.SummarySwitch;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VBucketLoadMeter extends KeyedProcessFunction<Integer, RoutedEnvelope, RoutedEnvelope> {
    private static final Logger log = LoggerFactory.getLogger(VBucketLoadMeter.class);
    public static final OutputTag<String> HEARTBEATS = new OutputTag<>("lb-heartbeats") {};
    private static final long INTERVAL_MS = 5_000;
    private transient ValueState<Long> count;
    private transient ValueState<Long> timer;
    private transient ValueState<String> hotspotSite;
    private transient boolean summaryEnabled;

    @Override
    public void open(Configuration parameters) {
        count = getRuntimeContext().getState(new ValueStateDescriptor<>("event-count", Long.class));
        timer = getRuntimeContext().getState(new ValueStateDescriptor<>("heartbeat-timer", Long.class));
        hotspotSite = getRuntimeContext().getState(new ValueStateDescriptor<>("hotspot-site", String.class));
        summaryEnabled = SummarySwitch.enabled();
    }

    @Override
    public void processElement(RoutedEnvelope value, Context ctx, Collector<RoutedEnvelope> out) throws Exception {
        count.update((count.value() == null ? 0 : count.value()) + 1);
        hotspotSite.update(value.siteId());
        if (timer.value() == null) {
            long next = ctx.timerService().currentProcessingTime() + INTERVAL_MS;
            timer.update(next); ctx.timerService().registerProcessingTimeTimer(next);
        }
        out.collect(value);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RoutedEnvelope> out) throws Exception {
        long events = count.value() == null ? 0 : count.value();
        double eps = events / (INTERVAL_MS / 1000.0);
        double[] buckets = new double[1024];
        buckets[ctx.getCurrentKey()] = eps;
        ctx.output(HEARTBEATS, new HeartbeatPayload(getRuntimeContext().getIndexOfThisSubtask(),
            eps, buckets, timestamp, hotspotSite.value(), ctx.getCurrentKey()).toCsv());
        if (summaryEnabled) {
            log.info(SummarySwitch.format("flink-heartbeat", "vbucket", ctx.getCurrentKey()));
            log.info(SummarySwitch.format("flink-heartbeat", "events_last_interval", events));
            log.info(SummarySwitch.format("flink-heartbeat", "eps", String.format("%.1f", eps)));
            log.info(SummarySwitch.format("flink-heartbeat", "hotspot_site", hotspotSite.value()));
        }
        count.update(0L);
        long next = timestamp + INTERVAL_MS;
        timer.update(next); ctx.timerService().registerProcessingTimeTimer(next);
    }
}
