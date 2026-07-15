package com.fdb.job.anomaly;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class AnomalySignal implements Serializable {
    public enum SignalType {
        TRIGGER,
        RECOVERY
    }

    private SignalType type;
    private String key;
    private AnomalyRuleEvaluation current;
    private List<AnomalyRuleEvaluation> streak;

    public AnomalySignal() {}

    public AnomalySignal(
        SignalType type,
        String key,
        AnomalyRuleEvaluation current,
        List<AnomalyRuleEvaluation> streak) {
        this.type = type;
        this.key = key;
        this.current = current;
        this.streak = new ArrayList<>(streak);
    }

    public static AnomalySignal trigger(List<AnomalyRuleEvaluation> streak) {
        AnomalyRuleEvaluation last = streak.get(streak.size() - 1);
        return new AnomalySignal(SignalType.TRIGGER, last.key(), last, streak);
    }

    public static AnomalySignal recovery(AnomalyRuleEvaluation evaluation) {
        return new AnomalySignal(SignalType.RECOVERY, evaluation.key(), evaluation, List.of(evaluation));
    }

    public SignalType type() { return type; }

    public String key() { return key; }

    public AnomalyRuleEvaluation current() { return current; }

    public List<AnomalyRuleEvaluation> streak() { return streak; }
}
