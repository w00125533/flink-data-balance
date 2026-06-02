package com.fdb.simulator;

import com.fdb.common.config.ConfigLoader;

import java.nio.file.Path;

final class SimulatorConfig {
    private final ConfigLoader.Config config;

    private SimulatorConfig(ConfigLoader.Config config) {
        this.config = config;
    }

    static SimulatorConfig load(String resource, String overlay) throws Exception {
        return new SimulatorConfig(ConfigLoader.builder()
            .defaultResource(resource)
            .overlayFile(Path.of(overlay))
            .envPrefix("FDB_")
            .build()
            .load());
    }

    String bootstrap() {
        return config.getStringOrNull("kafka.bootstrap") == null
            ? "localhost:9092" : config.getString("kafka.bootstrap");
    }

    String topic(String defaultValue) {
        return config.getStringOrNull("kafka.topic") == null ? defaultValue : config.getString("kafka.topic");
    }

    String topologyTopic() {
        return config.getStringOrNull("kafka.topologyTopic") == null
            ? "topology" : config.getString("kafka.topologyTopic");
    }

    long getLong(String key, long defaultValue) { return config.getLong(key, defaultValue); }
    double getDouble(String key, double defaultValue) { return config.getDouble(key, defaultValue); }
}
