package com.fdb.job.model;

import com.fdb.common.avro.ChrEvent;
import com.fdb.common.avro.CfgConfig;
import com.fdb.common.avro.PmStat;

public class EnrichedChr {
    private ChrEvent chrEvent;
    private CfgConfig cfgConfig;
    private PmStat latestPm;

    public EnrichedChr() {}

    public EnrichedChr(ChrEvent chrEvent, CfgConfig cfgConfig, PmStat latestPm) {
        this.chrEvent = chrEvent;
        this.cfgConfig = cfgConfig;
        this.latestPm = latestPm;
    }

    public ChrEvent chrEvent() { return chrEvent; }
    public CfgConfig cfgConfig() { return cfgConfig; }
    public PmStat latestPm() { return latestPm; }

    public void setChrEvent(ChrEvent chrEvent) { this.chrEvent = chrEvent; }
    public void setCfgConfig(CfgConfig cfgConfig) { this.cfgConfig = cfgConfig; }
    public void setLatestPm(PmStat latestPm) { this.latestPm = latestPm; }
}
