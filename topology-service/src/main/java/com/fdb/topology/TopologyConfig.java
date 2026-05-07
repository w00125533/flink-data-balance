package com.fdb.topology;

import java.util.List;
import java.util.Map;

public class TopologyConfig {

    private long seed = 42;

    private SitesConfig sites = new SitesConfig();
    private CellDefaultsConfig cellDefaults = new CellDefaultsConfig();

    public long getSeed() { return seed; }
    public void setSeed(long seed) { this.seed = seed; }

    public SitesConfig getSites() { return sites; }
    public void setSites(SitesConfig sites) { this.sites = sites; }

    public CellDefaultsConfig getCellDefaults() { return cellDefaults; }
    public void setCellDefaults(CellDefaultsConfig cellDefaults) { this.cellDefaults = cellDefaults; }

    public static class SitesConfig {
        private int count = 3000;
        private CellsPerSiteConfig cellsPerSite = new CellsPerSiteConfig();
        private RegionConfig region = new RegionConfig();
        private List<HotZoneConfig> hotZones = List.of();

        public int getCount() { return count; }
        public void setCount(int count) { this.count = count; }

        public CellsPerSiteConfig getCellsPerSite() { return cellsPerSite; }
        public void setCellsPerSite(CellsPerSiteConfig cfg) { this.cellsPerSite = cfg; }

        public RegionConfig getRegion() { return region; }
        public void setRegion(RegionConfig region) { this.region = region; }

        public List<HotZoneConfig> getHotZones() { return hotZones; }
        public void setHotZones(List<HotZoneConfig> zones) { this.hotZones = zones; }
    }

    public static class CellsPerSiteConfig {
        private int min = 3;
        private int max = 9;

        public int getMin() { return min; }
        public void setMin(int min) { this.min = min; }
        public int getMax() { return max; }
        public void setMax(int max) { this.max = max; }
    }

    public static class RegionConfig {
        private List<Double> latRange = List.of(39.7, 40.2);
        private List<Double> lonRange = List.of(116.0, 116.8);

        public List<Double> getLatRange() { return latRange; }
        public void setLatRange(List<Double> range) { this.latRange = range; }

        public List<Double> getLonRange() { return lonRange; }
        public void setLonRange(List<Double> range) { this.lonRange = range; }
    }

    public static class HotZoneConfig {
        private String name;
        private List<Double> center = List.of(39.91, 116.40);
        private double radiusKm = 3.0;
        private double siteWeightMultiplier = 5.0;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public List<Double> getCenter() { return center; }
        public void setCenter(List<Double> center) { this.center = center; }

        public double getRadiusKm() { return radiusKm; }
        public void setRadiusKm(double km) { this.radiusKm = km; }

        public double getSiteWeightMultiplier() { return siteWeightMultiplier; }
        public void setSiteWeightMultiplier(double m) { this.siteWeightMultiplier = m; }
    }

    public static class CellDefaultsConfig {
        private String cellType = "NR_SA";
        private List<Integer> bandwidthMhzCandidates = List.of(20, 40, 100);
        private List<String> frequencyBands = List.of("n78", "n41", "n28");
        private double maxPowerDbm = 49.0;
        private int numerology = 1;
        private String mimoMode = "MIMO_4x4";

        public String getCellType() { return cellType; }
        public void setCellType(String cellType) { this.cellType = cellType; }

        public List<Integer> getBandwidthMhzCandidates() { return bandwidthMhzCandidates; }
        public void setBandwidthMhzCandidates(List<Integer> b) { this.bandwidthMhzCandidates = b; }

        public List<String> getFrequencyBands() { return frequencyBands; }
        public void setFrequencyBands(List<String> f) { this.frequencyBands = f; }

        public double getMaxPowerDbm() { return maxPowerDbm; }
        public void setMaxPowerDbm(double p) { this.maxPowerDbm = p; }

        public int getNumerology() { return numerology; }
        public void setNumerology(int n) { this.numerology = n; }

        public String getMimoMode() { return mimoMode; }
        public void setMimoMode(String m) { this.mimoMode = m; }
    }
}
