package com.fdb.common.summary;

import java.util.Map;

public final class SummarySwitch {
    private static final String[] ENABLE_KEYS = {
        "FDB_E2E_SUMMARY",
        "FDB_SIM_SUMMARY",
        "FDB_TOPOLOGY_SUMMARY",
        "FDB_FLINK_SUMMARY"
    };

    private SummarySwitch() {
    }

    public static boolean enabled() {
        return enabled(System.getenv());
    }

    public static boolean enabled(Map<String, String> env) {
        for (String key : ENABLE_KEYS) {
            if (truthy(env.get(key))) {
                return true;
            }
        }
        return false;
    }

    public static String format(String source, String metric, Object value) {
        return "[summary-code] " + source + " | " + metric + " | " + value;
    }

    private static boolean truthy(String value) {
        if (value == null || value.isBlank()) {
            return false;
        }
        return switch (value.trim().toLowerCase()) {
            case "1", "true", "yes", "on" -> true;
            default -> false;
        };
    }
}
