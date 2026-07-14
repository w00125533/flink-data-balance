package com.fdb.job;

import com.fdb.common.avro.CfgConfig;

import java.io.Serializable;

public record MinuteFactEnvelope(
    Kind kind,
    ChrMinuteFact chrFact,
    PmMinuteFact pmFact,
    CfgConfig cfgConfig) implements Serializable {

    public enum Kind {
        CHR,
        PM,
        CFG
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

    public String cellId() {
        return switch (kind) {
            case CHR -> chrFact.cellId();
            case PM -> pmFact.cellId();
            case CFG -> cfgConfig.getCellId().toString();
        };
    }
}
