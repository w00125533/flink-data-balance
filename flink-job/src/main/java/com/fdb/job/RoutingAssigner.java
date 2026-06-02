package com.fdb.job;

import com.fdb.common.hash.Hashes;
import com.fdb.job.coordinator.RoutingEntry;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class RoutingAssigner extends BroadcastProcessFunction<InputEnvelope, String, RoutedEnvelope> {
    public static final MapStateDescriptor<String, RoutingEntry> ROUTING_STATE =
        new MapStateDescriptor<>("routing-broadcast", String.class, RoutingEntry.class);
    private static final int NUM_VBUCKETS = 1024;

    @Override
    public void processElement(InputEnvelope envelope, ReadOnlyContext ctx, Collector<RoutedEnvelope> out) throws Exception {
        String siteId = siteId(envelope);
        RoutingEntry route = ctx.getBroadcastState(ROUTING_STATE).get(siteId);
        int shift = route == null ? 0 : route.getSlotShift();
        out.collect(new RoutedEnvelope(envelope, Hashes.toVBucketWithShift(siteId, NUM_VBUCKETS, shift)));
    }

    @Override
    public void processBroadcastElement(String csv, Context ctx, Collector<RoutedEnvelope> out) throws Exception {
        RoutingEntry incoming = RoutingEntry.fromCsv(csv);
        BroadcastState<String, RoutingEntry> state = ctx.getBroadcastState(ROUTING_STATE);
        RoutingEntry existing = state.get(incoming.getSiteId());
        if (existing == null || incoming.getRoutingVersion() > existing.getRoutingVersion()) {
            state.put(incoming.getSiteId(), incoming);
        }
    }

    private static String siteId(InputEnvelope envelope) {
        if (envelope instanceof InputEnvelope.ChrEnv e) return e.chrEvent().getSiteId().toString();
        if (envelope instanceof InputEnvelope.MrEnv e) return e.mrStat().getSiteId().toString();
        return ((InputEnvelope.CmEnv) envelope).cmConfig().getSiteId().toString();
    }
}
