package com.fdb.job.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class RuleConfigTest {

    @Test
    void defaults_match_entity_anomaly_design() {
        RuleConfig rules = RuleConfig.defaults();

        assertThat(rules.cellConsecutiveMinutes()).isEqualTo(3);
        assertThat(rules.cellRsrpMin()).isEqualTo(-110f);
        assertThat(rules.cellSinrMin()).isEqualTo(-3f);
        assertThat(rules.cellAttachSuccessMin()).isEqualTo(0.95f);
        assertThat(rules.cellHoSuccessMin()).isEqualTo(0.90f);
        assertThat(rules.cellDropRateMax()).isEqualTo(0.05f);
        assertThat(rules.userConsecutiveEvents()).isEqualTo(3);
        assertThat(rules.userWindowMinutes()).isEqualTo(10);
        assertThat(rules.userRsrpMin()).isEqualTo(-110f);
        assertThat(rules.userSinrMin()).isEqualTo(-3f);
        assertThat(rules.userLatencyMsMax()).isEqualTo(500f);
        assertThat(rules.coverageHoleThreshold()).isEqualTo(50);
        assertThat(rules.ruleVersion()).isEqualTo("v1.0");
    }

    @Test
    void explicit_env_keys_override_yaml_defaults() throws Exception {
        var config = com.fdb.common.config.ConfigLoader.builder()
            .defaultResource("job-default.yaml")
            .envSource(Map.of())
            .build()
            .load();
        RuleConfig rules = JobConfig.rulesFrom(config, Map.of(
            "FDB_ANOMALY_CELL_CONSECUTIVE_MINUTES", "4",
            "FDB_ANOMALY_USER_LATENCY_MS_MAX", "750"
        ), new Properties());

        assertThat(rules.cellConsecutiveMinutes()).isEqualTo(4);
        assertThat(rules.userLatencyMsMax()).isEqualTo(750f);
    }
}
