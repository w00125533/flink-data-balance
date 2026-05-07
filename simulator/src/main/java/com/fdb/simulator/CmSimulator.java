package com.fdb.simulator;

import com.fdb.common.avro.*;
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
        String bootstrap = System.getenv().getOrDefault("FDB_KAFKA_BOOTSTRAP", "localhost:9092");
        String topic = "cm-config";

        TopologyClient topology = new TopologyClient(bootstrap, "sim-cm");
        topology.start("topology");
        topology.awaitReady(Duration.ofSeconds(30));

        List<TopologyRecord> cells = topology.getAllCells();
        log.info("Loaded {} cells from topology for CM simulator", cells.size());

        try (KafkaPublisher<CmConfig> publisher = new KafkaPublisher<>(bootstrap, topic, CmConfig.class)) {
            long version = 1;

            log.info("Publishing baseline CM config for {} cells", cells.size());
            for (TopologyRecord cell : cells) {
                CmConfig config = baselineConfig(cell, version);
                publisher.publish(cell.getCellId().toString(), config);
            }
            publisher.flush();
            log.info("Baseline CM config published");

            version++;

            while (!Thread.currentThread().isInterrupted()) {
                long intervalMs = 30_000 + rng.nextInt(60_000);
                Thread.sleep(intervalMs);

                int numChanges = Math.max(1, cells.size() / 200);
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

                if (rng.nextInt(20) == 0 && changed > 0) {
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
                }

                log.info("Published {} CM config updates (version {})", changed, version - 1);
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
