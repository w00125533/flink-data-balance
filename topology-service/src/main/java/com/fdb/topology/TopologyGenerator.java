package com.fdb.topology;

import com.fdb.common.avro.TopoCellType;
import com.fdb.common.avro.TopologyRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TopologyGenerator {

    private final TopologyConfig config;

    public TopologyGenerator(TopologyConfig config) {
        this.config = config;
    }

    public List<TopologyRecord> generate() {
        Random rng = new Random(config.getSeed());
        TopologyConfig.SitesConfig sc = config.getSites();
        TopologyConfig.CellDefaultsConfig cd = config.getCellDefaults();

        List<TopologyRecord> records = new ArrayList<>();
        List<int[]> usedPcis = new ArrayList<>();
        List<Integer> usedTacs = new ArrayList<>();

        double latLo = sc.getRegion().getLatRange().get(0);
        double latHi = sc.getRegion().getLatRange().get(1);
        double lonLo = sc.getRegion().getLonRange().get(0);
        double lonHi = sc.getRegion().getLonRange().get(1);

        double degPerKmLat = 1.0 / 111.32;
        double degPerKmLon = 1.0 / (111.32 * Math.cos(Math.toRadians((latLo + latHi) / 2)));

        int mcc = 460;
        int mnc = 0;

        for (int siteIdx = 1; siteIdx <= sc.getCount(); siteIdx++) {
            double[] pos = samplePosition(rng, sc, degPerKmLat, degPerKmLon);
            double siteLat = pos[0];
            double siteLon = pos[1];

            int numCells = sc.getCellsPerSite().getMin()
                + rng.nextInt(sc.getCellsPerSite().getMax() - sc.getCellsPerSite().getMin() + 1);

            int tac = 40000 + rng.nextInt(1000);
            if (usedTacs.contains(tac)) tac = 40000 + ((siteIdx - 1) % 1000);
            usedTacs.add(tac);

            for (int cellIdx = 1; cellIdx <= numCells; cellIdx++) {
                int pci = allocatePci(rng, usedPcis);

                long eci = (long) (siteIdx - 1) * 100 + cellIdx;

                int azimuth = cellIdx == 1 ? 0 : (cellIdx - 1) * (360 / numCells);

                int bwIdx = rng.nextInt(cd.getBandwidthMhzCandidates().size());
                int bandwidth = cd.getBandwidthMhzCandidates().get(bwIdx);

                int fbIdx = rng.nextInt(cd.getFrequencyBands().size());
                String freqBand = cd.getFrequencyBands().get(fbIdx);

                int arfcn = freqToArfcn(freqBand, rng);

                String siteId = String.format("SITE-%06d", siteIdx);
                String cellId = String.format("%s-%d", siteId, cellIdx);

                TopoCellType cellType = parseCellType(cd.getCellType());

                records.add(TopologyRecord.newBuilder()
                    .setSiteId(siteId)
                    .setCellId(cellId)
                    .setSiteLat(siteLat)
                    .setSiteLon(siteLon)
                    .setCellType(cellType)
                    .setCellIndex(cellIdx)
                    .setPci(pci)
                    .setTac(tac)
                    .setEci(eci)
                    .setMcc(String.valueOf(mcc))
                    .setMnc(String.format("%02d", mnc))
                    .setFrequencyBand(freqBand)
                    .setArfcn(arfcn)
                    .setBandwidthMhz(bandwidth)
                    .setAzimuth(azimuth)
                    .setCoverageRadiusM(200 + rng.nextInt(801))
                    .setMaxPowerDbm((float) cd.getMaxPowerDbm())
                    .setVersion(1L)
                    .build());
            }
        }

        return records;
    }

    private double[] samplePosition(Random rng, TopologyConfig.SitesConfig sc,
                                     double degPerKmLat, double degPerKmLon) {
        double latLo = sc.getRegion().getLatRange().get(0);
        double latHi = sc.getRegion().getLatRange().get(1);
        double lonLo = sc.getRegion().getLonRange().get(0);
        double lonHi = sc.getRegion().getLonRange().get(1);

        List<TopologyConfig.HotZoneConfig> hotZones = sc.getHotZones();

        if (hotZones.isEmpty()) {
            double lat = latLo + rng.nextDouble() * (latHi - latLo);
            double lon = lonLo + rng.nextDouble() * (lonHi - lonLo);
            return new double[]{lat, lon};
        }

        for (int attempt = 0; attempt < 200; attempt++) {
            double lat = latLo + rng.nextDouble() * (latHi - latLo);
            double lon = lonLo + rng.nextDouble() * (lonHi - lonLo);

            double weight = 1.0;
            for (TopologyConfig.HotZoneConfig hz : hotZones) {
                double dz = (lat - hz.getCenter().get(0)) / degPerKmLat;
                double dl = (lon - hz.getCenter().get(1)) / degPerKmLon;
                double distKm = Math.sqrt(dz * dz + dl * dl);
                if (distKm < hz.getRadiusKm()) {
                    weight = hz.getSiteWeightMultiplier();
                }
            }

            if (weight >= 1.0 || rng.nextDouble() < weight) {
                return new double[]{lat, lon};
            }
        }

        double lat = latLo + rng.nextDouble() * (latHi - latLo);
        double lon = lonLo + rng.nextDouble() * (lonHi - lonLo);
        return new double[]{lat, lon};
    }

    private int allocatePci(Random rng, List<int[]> usedPcis) {
        for (int attempt = 0; attempt < 100; attempt++) {
            int pci = rng.nextInt(1008);
            boolean collides = false;
            for (int[] used : usedPcis) {
                int usedPci = used[0];
                int usedSite = used[1];
                if (usedPci == pci && Math.abs(usedSite - usedPcis.size()) < 3) {
                    collides = true;
                    break;
                }
            }
            if (!collides) {
                usedPcis.add(new int[]{pci, usedPcis.size()});
                return pci;
            }
        }
        int pci = rng.nextInt(1008);
        usedPcis.add(new int[]{pci, usedPcis.size()});
        return pci;
    }

    private int freqToArfcn(String freqBand, Random rng) {
        return switch (freqBand) {
            case "n78" -> 620000 + rng.nextInt(20000);
            case "n41" -> 518000 + rng.nextInt(20000);
            case "n28" -> 150000 + rng.nextInt(20000);
            default -> 300000 + rng.nextInt(300000);
        };
    }

    private TopoCellType parseCellType(String type) {
        return switch (type.toUpperCase()) {
            case "LTE" -> TopoCellType.LTE;
            case "NR_NSA" -> TopoCellType.NR_NSA;
            default -> TopoCellType.NR_SA;
        };
    }
}
