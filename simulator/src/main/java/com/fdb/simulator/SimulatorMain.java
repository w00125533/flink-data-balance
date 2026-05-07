package com.fdb.simulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimulatorMain {

    private static final Logger log = LoggerFactory.getLogger(SimulatorMain.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: simulator <chr|mr|cm> [--config <path>]");
            System.exit(1);
        }

        String mode = args[0];
        String configPath = null;
        for (int i = 1; i < args.length; i++) {
            if ("--config".equals(args[i]) && i + 1 < args.length) {
                configPath = args[i + 1];
                break;
            }
        }

        if (configPath == null) {
            configPath = "sim-" + mode + ".yaml";
        }

        log.info("Starting simulator in '{}' mode with config: {}", mode, configPath);

        switch (mode) {
            case "chr" -> new ChrSimulator(configPath).run();
            case "mr" -> new MrSimulator(configPath).run();
            case "cm" -> new CmSimulator(configPath).run();
            default -> {
                System.err.println("Unknown mode: " + mode + ". Expected: chr, mr, cm");
                System.exit(1);
            }
        }
    }
}
