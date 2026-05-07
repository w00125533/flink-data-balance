package com.fdb.job;

import com.fdb.common.avro.ChrEvent;
import com.fdb.common.avro.CmConfig;
import com.fdb.common.avro.MrStat;

public class EnrichedChr {
    private ChrEvent chrEvent;
    private CmConfig cmConfig;
    private MrStat latestMr;

    public EnrichedChr() {}

    public EnrichedChr(ChrEvent chrEvent, CmConfig cmConfig, MrStat latestMr) {
        this.chrEvent = chrEvent;
        this.cmConfig = cmConfig;
        this.latestMr = latestMr;
    }

    public ChrEvent chrEvent() { return chrEvent; }
    public CmConfig cmConfig() { return cmConfig; }
    public MrStat latestMr() { return latestMr; }

    public void setChrEvent(ChrEvent chrEvent) { this.chrEvent = chrEvent; }
    public void setCmConfig(CmConfig cmConfig) { this.cmConfig = cmConfig; }
    public void setLatestMr(MrStat latestMr) { this.latestMr = latestMr; }
}
