package com.fdb.simulator;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TopologyClientTest {

    @Test
    void waitsForExpectedCellCountAndQuietPeriodBeforeReady() {
        long now = 10_000L;

        assertThat(TopologyClient.isReady(1, now - 2_000L, now, 10_000)).isFalse();
        assertThat(TopologyClient.isReady(10_000, now - 500L, now, 10_000)).isFalse();
        assertThat(TopologyClient.isReady(10_000, now - 1_000L, now, 10_000)).isTrue();
        assertThat(TopologyClient.isReady(1, now - 1_000L, now, 0)).isTrue();
    }
}
