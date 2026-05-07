package com.fdb.common.geo;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GeohashTest {

    @Test
    void encode_returns_string_of_requested_length() {
        String hash = Geohash.encode(39.9042, 116.4074, 7);
        assertThat(hash).hasSize(7);
    }

    @Test
    void encode_is_deterministic() {
        String a = Geohash.encode(39.9042, 116.4074, 7);
        String b = Geohash.encode(39.9042, 116.4074, 7);
        assertThat(a).isEqualTo(b);
    }

    @Test
    void encode_known_value_for_origin() {
        assertThat(Geohash.encode(0.0, 0.0, 5)).isEqualTo("s0000");
    }

    @Test
    void encode_known_value_for_san_francisco() {
        assertThat(Geohash.encode(37.7749, -122.4194, 5)).isEqualTo("9q8yy");
    }

    @Test
    void encode_rejects_invalid_precision() {
        assertThatThrownBy(() -> Geohash.encode(0.0, 0.0, 0))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Geohash.encode(0.0, 0.0, 13))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void encode_rejects_invalid_lat_lon() {
        assertThatThrownBy(() -> Geohash.encode(91.0, 0.0, 7))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Geohash.encode(0.0, 181.0, 7))
            .isInstanceOf(IllegalArgumentException.class);
    }
}
