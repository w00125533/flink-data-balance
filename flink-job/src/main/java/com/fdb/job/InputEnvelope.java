package com.fdb.job;

import com.fdb.common.avro.ChrEvent;
import com.fdb.common.avro.CmConfig;
import com.fdb.common.avro.MrStat;

public sealed interface InputEnvelope {

    long ts();
    String cellId();

    record ChrEnv(long ts, String cellId, ChrEvent chrEvent) implements InputEnvelope {
        public ChrEnv(ChrEvent chr) {
            this(chr.getEventTs(), chr.getCellId().toString(), chr);
        }
    }

    record MrEnv(long ts, String cellId, MrStat mrStat) implements InputEnvelope {
        public MrEnv(MrStat mr) {
            this(mr.getWindowEndTs(), mr.getCellId().toString(), mr);
        }
    }

    record CmEnv(long ts, String cellId, CmConfig cmConfig) implements InputEnvelope {
        public CmEnv(CmConfig cm) {
            this(cm.getEffectiveTs(), cm.getCellId().toString(), cm);
        }
    }
}
