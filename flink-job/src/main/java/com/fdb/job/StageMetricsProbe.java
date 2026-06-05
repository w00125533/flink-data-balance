package com.fdb.job;

import com.fdb.common.metrics.StageMetricSample;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class StageMetricsProbe<T> extends ProcessFunction<T, T> {
    private static final Logger log = LoggerFactory.getLogger(StageMetricsProbe.class);

    private final String stageId;
    private final String displayName;
    private final String status;
    private final long emitIntervalMs;

    private transient KafkaProducer<String, String> producer;
    private transient String topic;
    private transient Counter eventCounter;
    private long eventsSinceLastEmit;
    private long lastEmitAtMs = -1L;
    private double lastEps;

    public StageMetricsProbe(String stageId, String displayName, String status, long emitIntervalMs) {
        this.stageId = stageId;
        this.displayName = displayName;
        this.status = status;
        this.emitIntervalMs = emitIntervalMs > 0 ? emitIntervalMs : 5_000L;
    }

    @Override
    public void open(Configuration parameters) {
        eventCounter = getRuntimeContext().getMetricGroup()
            .addGroup("fdb")
            .addGroup("stage", stageId)
            .counter("records_total");
        getRuntimeContext().getMetricGroup()
            .addGroup("fdb")
            .addGroup("stage", stageId)
            .gauge("eps", (Gauge<Double>) () -> lastEps);

        String bootstrap = System.getenv().getOrDefault("FDB_KAFKA_BOOTSTRAP", "localhost:9092");
        topic = System.getenv().getOrDefault("FDB_METRICS_TOPIC", "fdb-stage-metrics");
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrap);
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());
        producer = new KafkaProducer<>(properties);
    }

    @Override
    public void processElement(T value, Context ctx, Collector<T> out) {
        record(value, ctx.timerService().currentProcessingTime());
        publish(drainDueSamples(ctx.timerService().currentProcessingTime()));
        out.collect(value);
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.flush();
            producer.close();
        }
    }

    T record(T value, long nowMs) {
        if (lastEmitAtMs < 0L) {
            lastEmitAtMs = nowMs;
        }
        eventsSinceLastEmit++;
        if (eventCounter != null) {
            eventCounter.inc();
        }
        return value;
    }

    List<StageMetricSample> drainDueSamples(long nowMs) {
        if (lastEmitAtMs < 0L || nowMs - lastEmitAtMs < emitIntervalMs) {
            return List.of();
        }
        double elapsedSeconds = Math.max((nowMs - lastEmitAtMs) / 1000.0d, 0.001d);
        lastEps = eventsSinceLastEmit / elapsedSeconds;
        StageMetricSample sample = StageMetricSample.stage(stageId, displayName, status,
            lastEps, lastEps, 0L, 0L, 0L, nowMs);
        eventsSinceLastEmit = 0L;
        lastEmitAtMs = nowMs;
        List<StageMetricSample> samples = new ArrayList<>();
        samples.add(sample);
        return samples;
    }

    private void publish(List<StageMetricSample> samples) {
        if (producer == null || samples.isEmpty()) {
            return;
        }
        for (StageMetricSample sample : samples) {
            try {
                producer.send(new ProducerRecord<>(topic, sample.stageId(), sample.toJson()));
            } catch (Exception e) {
                log.warn("Failed to publish stage metric sample for {}", sample.stageId(), e);
            }
        }
    }
}
