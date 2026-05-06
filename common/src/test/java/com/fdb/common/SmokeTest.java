package com.fdb.common;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class SmokeTest {

    @Test
    void java_runtime_is_21_or_higher() {
        int major = Runtime.version().feature();
        assertThat(major).isGreaterThanOrEqualTo(21);
    }
}
