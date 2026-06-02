package com.fdb.job;

import com.fdb.common.config.ConfigLoader;

import java.nio.file.Path;

final class JobConfig {
    private final ConfigLoader.Config config;

    private JobConfig(ConfigLoader.Config config) { this.config = config; }

    static JobConfig load() throws Exception {
        String path = System.getProperty("config", "job-default.yaml");
        return new JobConfig(ConfigLoader.builder().defaultResource("job-default.yaml")
            .overlayFile(Path.of(path)).envPrefix("FDB_").build().load());
    }

    RuleConfig rules() {
        return new RuleConfig(
            (float) config.getDouble("rules.lowSignal.rsrpThreshold", -110),
            (float) config.getDouble("rules.lowSignal.sinrThreshold", -3),
            config.getInt("rules.attachFailureBurst.threshold", 10),
            config.getInt("rules.coverageHole.threshold", 50));
    }
}
