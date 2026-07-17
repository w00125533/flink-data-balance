package com.fdb.topology;

import com.fdb.common.avro.TopologyRecord;
import com.fdb.common.config.ConfigLoader;
import com.fdb.common.summary.SummarySwitch;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TopologyMain {

    private static final Logger log = LoggerFactory.getLogger(TopologyMain.class);
    private static final ObjectMapper MAPPER = new ObjectMapper()
        .enable(SerializationFeature.INDENT_OUTPUT);

    public static void main(String[] args) throws Exception {
        long totalStartNanos = System.nanoTime();
        String configPath = System.getProperty("config", "topology-default.yaml");

        var configFile = Path.of(configPath);
        if (!configFile.isAbsolute()) {
            configFile = Path.of(System.getProperty("user.dir")).resolve(configPath).normalize();
        }

        var rawConfig = ConfigLoader.builder()
            .defaultResource("topology-default.yaml")
            .overlayFile(configFile)
            .envPrefix("FDB_")
            .build()
            .load();

        TopologyConfig topologyConfig = parseConfig(rawConfig);

        TopologyGenerator generator = new TopologyGenerator(topologyConfig);
        long generationStartNanos = System.nanoTime();
        List<TopologyRecord> records = generator.generate();
        long generationDurationMs = elapsedMs(generationStartNanos);

        log.info("Generated {} topology records from {} sites", records.size(), topologyConfig.getSites().getCount());
        logTopologySummary(records, topologyConfig);

        String bootstrap = rawConfig.getStringOrNull("kafka.bootstrap");
        if (bootstrap == null) {
            bootstrap = System.getenv().getOrDefault("FDB_KAFKA_BOOTSTRAP", "localhost:9092");
        }

        String topic = rawConfig.getStringOrNull("kafka.topologyTopic");
        if (topic == null) topic = "topology";

        KafkaTopologyPublisher publisher = new KafkaTopologyPublisher(bootstrap, topic);
        KafkaTopologyPublisher.PublishResult publishResult;
        long publishStartNanos = System.nanoTime();
        try {
            publishResult = publisher.publishAll(records);
        } finally {
            publisher.close();
        }
        long publishDurationMs = elapsedMs(publishStartNanos);
        TopologyMetrics metrics = TopologyMetrics.from(
            records,
            generationDurationMs,
            publishResult,
            publishDurationMs,
            elapsedMs(totalStartNanos));
        writeMetricsIfConfigured(metrics);

        log.info("Topology service completed successfully");
        System.exit(0);
    }

    private static void writeMetricsIfConfigured(TopologyMetrics metrics) throws IOException {
        String metricsFile = System.getenv("FDB_TOPOLOGY_METRICS_FILE");
        if (metricsFile == null || metricsFile.isBlank()) {
            return;
        }
        writeMetrics(Path.of(metricsFile), metrics);
    }

    static void writeMetrics(Path metricsFile, TopologyMetrics metrics) throws IOException {
        Path parent = metricsFile.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        MAPPER.writeValue(metricsFile.toFile(), metrics);
    }

    private static long elapsedMs(long startNanos) {
        return Math.max(0, (System.nanoTime() - startNanos) / 1_000_000L);
    }

    private static void logTopologySummary(List<TopologyRecord> records, TopologyConfig topologyConfig) {
        if (!SummarySwitch.enabled()) {
            return;
        }
        long siteCount = records.stream().map(r -> r.getSiteId().toString()).distinct().count();
        long bandCount = records.stream().map(r -> r.getFrequencyBand().toString()).distinct().count();
        int minCellsPerSite = topologyConfig.getSites().getCellsPerSite().getMin();
        int maxCellsPerSite = topologyConfig.getSites().getCellsPerSite().getMax();
        double minLat = records.stream().mapToDouble(TopologyRecord::getSiteLat).min().orElse(0.0);
        double maxLat = records.stream().mapToDouble(TopologyRecord::getSiteLat).max().orElse(0.0);
        double minLon = records.stream().mapToDouble(TopologyRecord::getSiteLon).min().orElse(0.0);
        double maxLon = records.stream().mapToDouble(TopologyRecord::getSiteLon).max().orElse(0.0);

        log.info(SummarySwitch.format("topology", "sites", siteCount));
        log.info(SummarySwitch.format("topology", "cells", records.size()));
        log.info(SummarySwitch.format("topology", "cells_per_site_config", minCellsPerSite + ".." + maxCellsPerSite));
        log.info(SummarySwitch.format("topology", "frequency_bands", bandCount));
        log.info(SummarySwitch.format("topology", "lat_range", String.format("%.6f..%.6f", minLat, maxLat)));
        log.info(SummarySwitch.format("topology", "lon_range", String.format("%.6f..%.6f", minLon, maxLon)));
    }

    static TopologyConfig parseConfig(ConfigLoader.Config raw) {
        TopologyConfig config = new TopologyConfig();
        config.setSeed(raw.getLong("seed", config.getSeed()));
        config.getSites().setCount(raw.getInt("sites.count", config.getSites().getCount()));
        int targetCells = raw.getInt("topology.target.cells",
            raw.getInt("sites.target.cells", config.getSites().getTargetCells()));
        config.getSites().setTargetCells(targetCells);
        config.getSites().getCellsPerSite().setMin(raw.getInt("sites.cellsPerSite.min", 3));
        config.getSites().getCellsPerSite().setMax(raw.getInt("sites.cellsPerSite.max", 9));
        config.getSites().getRegion().setLatRange(toDoubleList(raw.get("sites.region.latRange",
            config.getSites().getRegion().getLatRange())));
        config.getSites().getRegion().setLonRange(toDoubleList(raw.get("sites.region.lonRange",
            config.getSites().getRegion().getLonRange())));

        List<TopologyConfig.HotZoneConfig> hotZones = new ArrayList<>();
        for (Map<String, Object> values : raw.<List<Map<String, Object>>>get("sites.hotZones", List.of())) {
            TopologyConfig.HotZoneConfig hotZone = new TopologyConfig.HotZoneConfig();
            hotZone.setName(String.valueOf(values.getOrDefault("name", "")));
            hotZone.setCenter(toDoubleList(values.getOrDefault("center", hotZone.getCenter())));
            hotZone.setRadiusKm(Double.parseDouble(String.valueOf(values.getOrDefault("radiusKm", 3.0))));
            hotZone.setSiteWeightMultiplier(Double.parseDouble(
                String.valueOf(values.getOrDefault("siteWeightMultiplier", 1.0))));
            hotZones.add(hotZone);
        }
        config.getSites().setHotZones(hotZones);

        config.getCellDefaults().setCellType(raw.getStringOrNull("cellDefaults.cellType") == null
            ? config.getCellDefaults().getCellType() : raw.getString("cellDefaults.cellType"));
        config.getCellDefaults().setBandwidthMhzCandidates(toIntList(raw.get(
            "cellDefaults.bandwidthMhzCandidates", config.getCellDefaults().getBandwidthMhzCandidates())));
        config.getCellDefaults().setFrequencyBands(raw.get(
            "cellDefaults.frequencyBands", config.getCellDefaults().getFrequencyBands()));
        config.getCellDefaults().setMaxPowerDbm(raw.getDouble(
            "cellDefaults.maxPowerDbm", config.getCellDefaults().getMaxPowerDbm()));
        return config;
    }

    private static List<Double> toDoubleList(Object raw) {
        return ((List<?>) raw).stream().map(v -> Double.parseDouble(String.valueOf(v))).toList();
    }

    private static List<Integer> toIntList(Object raw) {
        return ((List<?>) raw).stream().map(v -> Integer.parseInt(String.valueOf(v))).toList();
    }
}
