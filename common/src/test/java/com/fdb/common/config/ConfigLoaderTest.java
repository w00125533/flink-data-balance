package com.fdb.common.config;

import com.fdb.common.config.ConfigLoader.Config;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

import com.fdb.common.config.ConfigLoader.Config;

class ConfigLoaderTest {

    private static Path resource(String name) throws URISyntaxException {
        return Paths.get(ConfigLoaderTest.class.getClassLoader().getResource(name).toURI());
    }

    @Test
    void defaults_only_when_no_overlay_no_env() throws Exception {
        Config cfg = ConfigLoader.builder()
            .defaultResource("test-default.yaml")
            .envPrefix("FDB_TEST_")
            .envSource(Map.of())
            .build()
            .load();

        assertThat(cfg.getString("kafka.bootstrap")).isEqualTo("default-broker:9092");
        assertThat(cfg.getString("kafka.topic")).isEqualTo("chr-events");
        assertThat(cfg.getString("nested.deeply.value")).isEqualTo("original");
    }

    @Test
    void file_overlay_overrides_default() throws Exception {
        Config cfg = ConfigLoader.builder()
            .defaultResource("test-default.yaml")
            .overlayFile(resource("test-overlay.yaml"))
            .envPrefix("FDB_TEST_")
            .envSource(Map.of())
            .build()
            .load();

        assertThat(cfg.getString("kafka.bootstrap")).isEqualTo("file-broker:9092");
        assertThat(cfg.getString("kafka.topic")).isEqualTo("chr-events");
        assertThat(cfg.getString("nested.deeply.value")).isEqualTo("from-file");
    }

    @Test
    void env_var_overrides_file_and_default() throws Exception {
        Config cfg = ConfigLoader.builder()
            .defaultResource("test-default.yaml")
            .overlayFile(resource("test-overlay.yaml"))
            .envPrefix("FDB_TEST_")
            .envSource(Map.of(
                "FDB_TEST_KAFKA_BOOTSTRAP", "env-broker:9092",
                "FDB_TEST_NESTED_DEEPLY_VALUE", "from-env",
                "OTHER_VAR", "ignored"
            ))
            .build()
            .load();

        assertThat(cfg.getString("kafka.bootstrap")).isEqualTo("env-broker:9092");
        assertThat(cfg.getString("nested.deeply.value")).isEqualTo("from-env");
        assertThat(cfg.getString("kafka.topic")).isEqualTo("chr-events");
    }

    @Test
    void missing_key_returns_null_and_required_throws() throws Exception {
        Config cfg = ConfigLoader.builder()
            .defaultResource("test-default.yaml")
            .envPrefix("FDB_TEST_")
            .envSource(Map.of())
            .build()
            .load();

        assertThat(cfg.getStringOrNull("nope")).isNull();
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> cfg.getString("nope"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("nope");
    }
}
