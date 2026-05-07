package com.fdb.job.coordinator;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RebalancePolicyTest {

    @Test
    void no_rebalance_when_all_balanced() {
        RebalancePolicy policy = new RebalancePolicy(1.5, 60_000, 3, 1024);
        Map<Integer, HeartbeatPayload> hbs = new HashMap<>();
        hbs.put(0, new HeartbeatPayload(0, 100, new double[1024], 1000));
        hbs.put(1, new HeartbeatPayload(1, 110, new double[1024], 1000));
        hbs.put(2, new HeartbeatPayload(2, 105, new double[1024], 1000));
        hbs.put(3, new HeartbeatPayload(3, 95, new double[1024], 1000));

        Map<String, Long> overloadTimes = new HashMap<>();
        var decisions = policy.evaluate(hbs, overloadTimes, 200_000, 1);
        assertThat(decisions).isEmpty();
    }

    @Test
    void rebalance_when_subtask_overloaded() {
        RebalancePolicy policy = new RebalancePolicy(1.5, 60_000, 3, 1024);

        double[] vbEps0 = new double[1024];
        vbEps0[17] = 500;

        Map<Integer, HeartbeatPayload> hbs = new HashMap<>();
        hbs.put(0, new HeartbeatPayload(0, 500, vbEps0, 1000));
        hbs.put(1, new HeartbeatPayload(1, 100, null, 1000));
        hbs.put(2, new HeartbeatPayload(2, 95, null, 1000));
        hbs.put(3, new HeartbeatPayload(3, 50, null, 1000));  // idle subtask

        Map<String, Long> overloadTimes = new HashMap<>();
        overloadTimes.put("0", 1000L);

        var decisions = policy.evaluate(hbs, overloadTimes, 200_000, 1);
        assertThat(decisions).isNotEmpty();
    }

    @Test
    void no_rebalance_if_overload_duration_not_met() {
        RebalancePolicy policy = new RebalancePolicy(1.5, 60_000, 3, 1024);

        Map<Integer, HeartbeatPayload> hbs = new HashMap<>();
        hbs.put(0, new HeartbeatPayload(0, 500, null, 1000));
        hbs.put(1, new HeartbeatPayload(1, 100, null, 1000));

        Map<String, Long> overloadTimes = new HashMap<>();
        overloadTimes.put("0", 190_000L);

        var decisions = policy.evaluate(hbs, overloadTimes, 200_000, 1);
        assertThat(decisions).isEmpty();
    }

    @Test
    void median_computation() {
        RebalancePolicy policy = new RebalancePolicy();
        assertThat(policy.median(new double[]{1, 2, 3})).isEqualTo(2.0);
        assertThat(policy.median(new double[]{1, 2, 3, 4})).isEqualTo(2.5);
        assertThat(policy.median(new double[]{})).isEqualTo(0.0);
    }
}
