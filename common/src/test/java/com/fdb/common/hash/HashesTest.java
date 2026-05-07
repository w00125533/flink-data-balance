package com.fdb.common.hash;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HashesTest {

    @Test
    void to_vbucket_is_deterministic() {
        int a = Hashes.toVBucket("SITE-000123", 1024);
        int b = Hashes.toVBucket("SITE-000123", 1024);
        assertThat(a).isEqualTo(b);
    }

    @Test
    void to_vbucket_is_in_range() {
        for (int i = 0; i < 5000; i++) {
            int v = Hashes.toVBucket("SITE-" + i, 1024);
            assertThat(v).isBetween(0, 1023);
        }
    }

    @Test
    void to_vbucket_distributes_across_buckets() {
        Map<Integer, Integer> counts = new HashMap<>();
        for (int i = 0; i < 10_000; i++) {
            int v = Hashes.toVBucket("SITE-" + i, 1024);
            counts.merge(v, 1, Integer::sum);
        }
        assertThat(counts.size()).isGreaterThan(820);
    }

    @Test
    void to_vbucket_rejects_non_positive_n() {
        assertThatThrownBy(() -> Hashes.toVBucket("x", 0))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Hashes.toVBucket("x", -1))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void to_vbucket_with_shift_xor() {
        int base = Hashes.toVBucket("SITE-001", 1024);
        int shifted = Hashes.toVBucketWithShift("SITE-001", 1024, 17);
        assertThat(shifted).isEqualTo((base ^ 17) & 1023);
    }
}
