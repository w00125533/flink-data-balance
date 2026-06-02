package com.fdb.simulator;

import com.fdb.common.avro.*;
import com.fdb.common.summary.SummarySwitch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class CmSimulator {

    private static final Logger log = LoggerFactory.getLogger(CmSimulator.class);

    private final String configPath;
    private final Random rng = new Random(44);

    public CmSimulator(String configPath) {
        this.configPath = configPath;
    }

    public void run() throws Exception {
        SimulatorConfig simConfig = SimulatorConfig.load("sim-cm.yaml", configPath);
        String bootstrap = simConfig.bootstrap();
        String topic = simConfig.topic("cm-config");

        TopologyClient topology = new TopologyClient(bootstrap, "sim-cm");
        topology.start(simConfig.topologyTopic());
        topology.awaitReady(Duration.ofSeconds(30));

        List<TopologyRecord> cells = new ArrayList<>(topology.getAllCells());
        log.info("Loaded {} cells from topology for CM simulator", cells.size());
        boolean summaryEnabled = SummarySwitch.enabled();
        if (summaryEnabled) {
            long sites = cells.stream().map(c -> c.getSiteId().toString()).distinct().count();
            log.info(SummarySwitch.format("sim-cm", "loaded_sites", sites));
            log.info(SummarySwitch.format("sim-cm", "loaded_cells", cells.size()));
        }

        try (KafkaPublisher<CmConfig> publisher = new KafkaPublisher<>(bootstrap, topic, CmConfig.class)) {
            long version = 1;

            log.info("Publishing baseline CM config for {} cells", cells.size());
            for (TopologyRecord cell : cells) {
                CmConfig config = baselineConfig(cell, version);
                publisher.publish(cell.getCellId().toString(), config);
            }
            publisher.flush();
            log.info("Baseline CM config published");
            if (summaryEnabled) {
                log.info(SummarySwitch.format("sim-cm", "baseline_records_published", cells.size()));
                log.info(SummarySwitch.format("sim-cm", "baseline_version", version));
            }

            version++;

            while (!Thread.currentThread().isInterrupted()) {
                long intervalMs = simConfig.getLong("updates.intervalMin", 30) * 60_000;
                Thread.sleep(intervalMs);

                int numChanges = Math.max(1, (int) (cells.size() * simConfig.getDouble("updates.changeRate", 0.005)));
                Collections.shuffle(cells.subList(0, Math.min(cells.size(), numChanges * 10)), rng);

                int changed = 0;
                for (int i = 0; i < numChanges && i < cells.size(); i++) {
                    TopologyRecord cell = cells.get(i);
                    CmConfig config = updatedConfig(cell, version);
                    publisher.publish(cell.getCellId().toString(), config);
                    changed++;
                }
                publisher.flush();
                version++;

                if (rng.nextDouble() < simConfig.getDouble("updates.tombstoneProb", 0.05) && changed > 0) {
                    TopologyRecord cell = cells.get(rng.nextInt(cells.size()));
                    CmConfig tombstone = CmConfig.newBuilder()
                        .setSiteId(cell.getSiteId().toString())
                        .setCellId(cell.getCellId().toString())
                        .setEffectiveTs(System.currentTimeMillis())
                        .setVersion(version++)
                        .setCellType(CellType.NR_SA)
                        .setBandwidthMhz(100)
                        .setFrequencyBand("n78")
                        .setArfcn(632448)
                        .setMaxPowerDbm(49.0f)
                        .setAzimuth(0)
                        .setCenterLat(cell.getSiteLat())
                        .setCenterLon(cell.getSiteLon())
                        .setCoverageRadiusM(500)
                        .setPci(cell.getPci())
                        .setTac(cell.getTac())
                        .setEci(cell.getEci())
                        .setMcc(cell.getMcc().toString())
                        .setMnc(cell.getMnc().toString())
                        .setAntennaPorts(4)
                        .setNssai(List.of(NssaiEntry.newBuilder().setSst(1).setSd("000001").build()))
                        .setNeighborCells(List.of())
                        .setTombstone(true)
                        .build();
                    publisher.publish(cell.getCellId().toString(), tombstone);
                    log.info("Published tombstone for {}", cell.getCellId());
                    if (summaryEnabled) {
                        log.info(SummarySwitch.format("sim-cm", "tombstone_published", cell.getCellId()));
                    }
                }

                log.info("Published {} CM config updates (version {})", changed, version - 1);
                if (summaryEnabled) {
                    log.info(SummarySwitch.format("sim-cm", "updates_published_last_batch", changed));
                    log.info(SummarySwitch.format("sim-cm", "latest_version", version - 1));
                }
            }
        }
    }

    private CmConfig baselineConfig(TopologyRecord cell, long version) {
        return CmConfig.newBuilder()
            .setSiteId(cell.getSiteId().toString())
            .setCellId(cell.getCellId().toString())
            .setEffectiveTs(System.currentTimeMillis())
            .setVersion(version)
            .setCellType(CellType.NR_SA)
            .setBandwidthMhz(cell.getBandwidthMhz())
            .setFrequencyBand(cell.getFrequencyBand().toString())
            .setArfcn(cell.getArfcn())
            .setMaxPowerDbm(cell.getMaxPowerDbm())
            .setAzimuth(cell.getAzimuth())
            .setCenterLat(cell.getSiteLat())
            .setCenterLon(cell.getSiteLon())
            .setCoverageRadiusM(cell.getCoverageRadiusM())
            .setPci(cell.getPci())
            .setTac(cell.getTac())
            .setEci(cell.getEci())
            .setMcc(cell.getMcc().toString())
            .setMnc(cell.getMnc().toString())
            .setAntennaPorts(4)
            .setNssai(List.of(NssaiEntry.newBuilder().setSst(1).setSd("000001").build()))
            .setNeighborCells(List.of("NEIGHBOR-1", "NEIGHBOR-2"))
            .setTombstone(false)
            .build();
    }

    private CmConfig updatedConfig(TopologyRecord cell, long version) {
        CmConfig base = baselineConfig(cell, version);
        if (rng.nextBoolean()) {
            return CmConfig.newBuilder(base)
                .setMaxPowerDbm(base.getMaxPowerDbm() + rng.nextFloat() * 3 - 1.5f)
                .setVersion(version)
                .build();
        }
        return base;
    }
}
