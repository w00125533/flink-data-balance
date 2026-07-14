package com.fdb.job.model;

import com.fdb.common.avro.CfgConfig;

import java.io.Serializable;
import java.util.Objects;

public class MinuteFactEnvelope implements Serializable {

    public enum Kind {
        CHR,
        PM,
        CFG
    }

    private Kind kind;
    private ChrMinuteFact chrFact;
    private PmMinuteFact pmFact;
    private CfgConfig cfgConfig;

    public MinuteFactEnvelope() {
    }

    public MinuteFactEnvelope(Kind kind, ChrMinuteFact chrFact, PmMinuteFact pmFact, CfgConfig cfgConfig) {
        this.kind = kind;
        this.chrFact = chrFact;
        this.pmFact = pmFact;
        this.cfgConfig = cfgConfig;
    }

    public static MinuteFactEnvelope chr(ChrMinuteFact fact) {
        return new MinuteFactEnvelope(Kind.CHR, fact, null, null);
    }

    public static MinuteFactEnvelope pm(PmMinuteFact fact) {
        return new MinuteFactEnvelope(Kind.PM, null, fact, null);
    }

    public static MinuteFactEnvelope cfg(CfgConfig cfg) {
        return new MinuteFactEnvelope(Kind.CFG, null, null, cfg);
    }

    public Kind kind() {
        return kind;
    }

    public Kind getKind() {
        return kind;
    }

    public void setKind(Kind kind) {
        this.kind = kind;
    }

    public ChrMinuteFact chrFact() {
        return chrFact;
    }

    public ChrMinuteFact getChrFact() {
        return chrFact;
    }

    public void setChrFact(ChrMinuteFact chrFact) {
        this.chrFact = chrFact;
    }

    public PmMinuteFact pmFact() {
        return pmFact;
    }

    public PmMinuteFact getPmFact() {
        return pmFact;
    }

    public void setPmFact(PmMinuteFact pmFact) {
        this.pmFact = pmFact;
    }

    public CfgConfig cfgConfig() {
        return cfgConfig;
    }

    public CfgConfig getCfgConfig() {
        return cfgConfig;
    }

    public void setCfgConfig(CfgConfig cfgConfig) {
        this.cfgConfig = cfgConfig;
    }

    public String cellId() {
        return switch (kind) {
            case CHR -> chrFact.cellId();
            case PM -> pmFact.cellId();
            case CFG -> cfgConfig.getCellId().toString();
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MinuteFactEnvelope that)) {
            return false;
        }
        return kind == that.kind
            && Objects.equals(chrFact, that.chrFact)
            && Objects.equals(pmFact, that.pmFact)
            && Objects.equals(cfgConfig, that.cfgConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, chrFact, pmFact, cfgConfig);
    }

    @Override
    public String toString() {
        return "MinuteFactEnvelope{"
            + "kind=" + kind
            + ", chrFact=" + chrFact
            + ", pmFact=" + pmFact
            + ", cfgConfig=" + cfgConfig
            + '}';
    }
}
