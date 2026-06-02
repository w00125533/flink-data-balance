package com.fdb.job;

import com.fdb.common.avro.ChrEvent;
import com.fdb.common.avro.CmConfig;
import com.fdb.common.avro.MrStat;

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

    public static class MrEnv extends InputEnvelope {
        private MrStat mrStat;

        public MrEnv() {}

        public MrEnv(MrStat mr) {
            super(mr.getWindowEndTs(), mr.getCellId().toString());
            this.mrStat = mr;
        }

        public MrStat mrStat() {
            return mrStat;
        }

        public void setMrStat(MrStat mrStat) {
            this.mrStat = mrStat;
        }
    }

    public static class CmEnv extends InputEnvelope {
        private CmConfig cmConfig;

        public CmEnv() {}

        public CmEnv(CmConfig cm) {
            super(cm.getEffectiveTs(), cm.getCellId().toString());
            this.cmConfig = cm;
        }

        public CmConfig cmConfig() {
            return cmConfig;
        }

        public void setCmConfig(CmConfig cmConfig) {
            this.cmConfig = cmConfig;
        }
    }
}
