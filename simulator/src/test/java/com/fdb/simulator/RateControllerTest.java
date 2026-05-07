package com.fdb.simulator;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RateControllerTest {

    @Test
    void allows_burst_up_to_capacity() {
        RateController rc = new RateController(100);
        for (int i = 0; i < 100; i++) {
            assertThat(rc.tryAcquire("cell-1")).isTrue();
        }
    }

    @Test
    void denies_when_exhausted() {
        RateController rc = new RateController(10);
        for (int i = 0; i < 10; i++) rc.tryAcquire("cell-1");
        assertThat(rc.tryAcquire("cell-1")).isFalse();
    }

    @Test
    void cells_have_independent_buckets() {
        RateController rc = new RateController(5);
        for (int i = 0; i < 5; i++) rc.tryAcquire("cell-a");
        assertThat(rc.tryAcquire("cell-a")).isFalse();
        assertThat(rc.tryAcquire("cell-b")).isTrue();
    }
}
