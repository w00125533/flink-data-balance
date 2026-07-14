package com.fdb.job.model;

import com.fdb.common.avro.ChrEvent;
import com.fdb.common.avro.CfgConfig;
import com.fdb.common.avro.PmStat;

import java.io.Serializable;

public abstract class InputEnvelope implements Serializable {

    private long ts;
    private String cellId;

    protected InputEnvelope() {}

    protected InputEnvelope(long ts, String cellId) {
        this.ts = ts;
        this.cellId = cellId;
    }

    public long ts() {
        return ts;
    }

    public String cellId() {
        return cellId;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public void setCellId(String cellId) {
        this.cellId = cellId;
    }

    public static class ChrEnv extends InputEnvelope {
        private ChrEvent chrEvent;

        public ChrEnv() {}

        public ChrEnv(ChrEvent chr) {
            super(chr.getEventTs(), chr.getCellId().toString());
            this.chrEvent = chr;
        }

        public ChrEvent chrEvent() {
            return chrEvent;
        }

        public void setChrEvent(ChrEvent chrEvent) {
            this.chrEvent = chrEvent;
        }
    }

    public static class PmEnv extends InputEnvelope {
        private PmStat pmStat;

        public PmEnv() {}

        public PmEnv(PmStat pm) {
            super(pm.getWindowEndTs(), pm.getCellId().toString());
            this.pmStat = pm;
        }

        public PmStat pmStat() {
            return pmStat;
        }

        public void setPmStat(PmStat pmStat) {
            this.pmStat = pmStat;
        }
    }

    public static class CfgEnv extends InputEnvelope {
        private CfgConfig cfgConfig;

        public CfgEnv() {}

        public CfgEnv(CfgConfig cfg) {
            super(cfg.getEffectiveTs(), cfg.getCellId().toString());
            this.cfgConfig = cfg;
        }

        public CfgConfig cfgConfig() {
            return cfgConfig;
        }

        public void setCfgConfig(CfgConfig cfgConfig) {
            this.cfgConfig = cfgConfig;
        }
    }
}
