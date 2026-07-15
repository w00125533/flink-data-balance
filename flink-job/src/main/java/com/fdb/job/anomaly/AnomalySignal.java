package com.fdb.job.anomaly;

import java.io.Serializable;
import java.util.List;

public record AnomalySignal(
    SignalType type,
    String key,
    AnomalyRuleEvaluation current,
    List<AnomalyRuleEvaluation> streak
) implements Serializable {
    public enum SignalType {
        TRIGGER,
        RECOVERY
    }

    public static AnomalySignal trigger(List<AnomalyRuleEvaluation> streak) {
        AnomalyRuleEvaluation last = streak.get(streak.size() - 1);
        return new AnomalySignal(SignalType.TRIGGER, last.key(), last, List.copyOf(streak));
    }

    public static AnomalySignal recovery(AnomalyRuleEvaluation evaluation) {
        return new AnomalySignal(SignalType.RECOVERY, evaluation.key(), evaluation, List.of(evaluation));
    }
}
