package com.fdb.common.config;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class ConfigLoader {

    private final String defaultResource;
    private final Path overlayFile;
    private final String envPrefix;
    private final Map<String, String> envSource;

    private ConfigLoader(Builder b) {
        this.defaultResource = b.defaultResource;
        this.overlayFile = b.overlayFile;
        this.envPrefix = b.envPrefix == null ? "FDB_" : b.envPrefix;
        this.envSource = b.envSource == null ? System.getenv() : b.envSource;
    }

    public static Builder builder() { return new Builder(); }

    public Config load() throws IOException {
        Map<String, Object> merged = new LinkedHashMap<>();

        if (defaultResource != null) {
            try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(defaultResource)) {
                if (in == null) {
                    throw new IOException("default resource not on classpath: " + defaultResource);
                }
                Object loaded = new Yaml().load(in);
                if (loaded instanceof Map<?, ?> map) {
                    deepMerge(merged, castToStringKeyedMap(map));
                }
            }
        }

        if (overlayFile != null) {
            try (InputStream in = Files.newInputStream(overlayFile)) {
                Object loaded = new Yaml().load(in);
                if (loaded instanceof Map<?, ?> map) {
                    deepMerge(merged, castToStringKeyedMap(map));
                }
            }
        }

        applyEnv(merged, envPrefix, envSource);

        return new Config(merged);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castToStringKeyedMap(Map<?, ?> raw) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<?, ?> e : raw.entrySet()) {
            String k = String.valueOf(e.getKey());
            Object v = e.getValue();
            if (v instanceof Map<?, ?> nested) {
                out.put(k, castToStringKeyedMap(nested));
            } else {
                out.put(k, v);
            }
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private static void deepMerge(Map<String, Object> base, Map<String, Object> overlay) {
        for (Map.Entry<String, Object> e : overlay.entrySet()) {
            String k = e.getKey();
            Object oVal = e.getValue();
            Object bVal = base.get(k);
            if (bVal instanceof Map && oVal instanceof Map) {
                deepMerge((Map<String, Object>) bVal, (Map<String, Object>) oVal);
            } else {
                base.put(k, oVal);
            }
        }
    }

    private static void applyEnv(Map<String, Object> base, String prefix, Map<String, String> env) {
        for (Map.Entry<String, String> e : env.entrySet()) {
            String envKey = e.getKey();
            if (!envKey.startsWith(prefix)) continue;
            String dotted = envKey.substring(prefix.length()).toLowerCase().replace('_', '.');
            setByPath(base, dotted, e.getValue());
        }
    }

    @SuppressWarnings("unchecked")
    private static void setByPath(Map<String, Object> base, String dottedPath, Object value) {
        String[] parts = dottedPath.split("\\.");
        Map<String, Object> cur = base;
        for (int i = 0; i < parts.length - 1; i++) {
            Object next = cur.get(parts[i]);
            if (!(next instanceof Map)) {
                Map<String, Object> fresh = new LinkedHashMap<>();
                cur.put(parts[i], fresh);
                next = fresh;
            }
            cur = (Map<String, Object>) next;
        }
        cur.put(parts[parts.length - 1], value);
    }

    public static final class Builder {
        private String defaultResource;
        private Path overlayFile;
        private String envPrefix;
        private Map<String, String> envSource;

        public Builder defaultResource(String resource) { this.defaultResource = resource; return this; }
        public Builder overlayFile(Path file)           { this.overlayFile = file;        return this; }
        public Builder envPrefix(String prefix)          { this.envPrefix = prefix;        return this; }
        public Builder envSource(Map<String, String> env){ this.envSource = env;           return this; }

        public ConfigLoader build() { return new ConfigLoader(this); }
    }

    public static final class Config {
        private final Map<String, Object> root;

        Config(Map<String, Object> root) {
            this.root = Collections.unmodifiableMap(root);
        }

        @SuppressWarnings("unchecked")
        private Object resolve(String dottedPath) {
            String[] parts = dottedPath.split("\\.");
            Object cur = root;
            for (String p : parts) {
                if (cur instanceof Map<?, ?> m) {
                    cur = ((Map<String, Object>) m).get(p);
                } else {
                    return null;
                }
            }
            return cur;
        }

        public String getStringOrNull(String dottedPath) {
            Object v = resolve(dottedPath);
            return v == null ? null : String.valueOf(v);
        }

        public String getString(String dottedPath) {
            String v = getStringOrNull(dottedPath);
            if (v == null) {
                throw new IllegalArgumentException("missing required config key: " + dottedPath);
            }
            return v;
        }

        public Map<String, Object> raw() { return root; }
    }
}
