package com.fdb.job.balance;

import com.fdb.job.model.InputEnvelope;
import com.fdb.job.model.RoutedEnvelope;
import com.fdb.job.balance.coordinator.HeartbeatPayload;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class VBucketLoadMeterTest {

    @Test
    void hotspot_route_key_uses_routed_envelope_cell_state_key() {
        InputEnvelope envelope = new InputEnvelope(1_000L, "CELL-HOTSPOT") {};
        RoutedEnvelope routed = new RoutedEnvelope(envelope, 7);

        assertThat(VBucketLoadMeter.hotspotRouteKey(routed)).isEqualTo("CELL-HOTSPOT");
    }

    @Test
    void heartbeat_uses_hottest_route_key_not_last_route_key() {
        Map<String, Long> routeKeyCounts = new LinkedHashMap<>();
        for (int i = 0; i < 10; i++) {
            VBucketLoadMeter.incrementRouteKeyCount(routeKeyCounts,
                new RoutedEnvelope(new InputEnvelope(1_000L + i, "CELL-A") {}, 7));
        }
        VBucketLoadMeter.incrementRouteKeyCount(routeKeyCounts,
            new RoutedEnvelope(new InputEnvelope(2_000L, "CELL-B") {}, 7));

        HeartbeatPayload heartbeat = VBucketLoadMeter.heartbeatPayload(0, 11, 7, 5_000L, routeKeyCounts);

        assertThat(heartbeat.getHotspotSiteId()).isEqualTo("CELL-A");
        assertThat(heartbeat.getHotspotVbucketId()).isEqualTo(7);
    }
}
