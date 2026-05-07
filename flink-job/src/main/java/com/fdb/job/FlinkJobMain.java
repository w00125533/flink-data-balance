package com.fdb.job;

import com.fdb.common.avro.*;
import com.fdb.job.coordinator.HeartbeatParser;
import com.fdb.job.coordinator.HeartbeatPayload;
import com.fdb.job.coordinator.LoadCoordinator;
import com.fdb.job.coordinator.RoutingEntry;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class FlinkJobMain {

    private static final Logger log = LoggerFactory.getLogger(FlinkJobMain.class);

    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv().getOrDefault("FDB_KAFKA_BOOTSTRAP", "localhost:9092");
        String groupId = "fdb-flink-job";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000);
        env.setParallelism(4);

        // ── Main pipeline: CHR + MR + CM ──

        KafkaSource<ChrEvent> chrSource = KafkaSource.<ChrEvent>builder()
            .setBootstrapServers(bootstrap)
            .setTopics("chr-events")
            .setGroupId(groupId + "-chr")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setDeserializer(new FlinkAvroDeserializer<>(ChrEvent.class))
            .build();

        KafkaSource<MrStat> mrSource = KafkaSource.<MrStat>builder()
            .setBootstrapServers(bootstrap)
            .setTopics("mr-stats")
            .setGroupId(groupId + "-mr")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setDeserializer(new FlinkAvroDeserializer<>(MrStat.class))
            .build();

        KafkaSource<CmConfig> cmSource = KafkaSource.<CmConfig>builder()
            .setBootstrapServers(bootstrap)
            .setTopics("cm-config")
            .setGroupId(groupId + "-cm")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setDeserializer(new FlinkAvroDeserializer<>(CmConfig.class))
            .build();

        DataStream<ChrEvent> chrStream = env.fromSource(chrSource,
            WatermarkStrategy.<ChrEvent>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withIdleness(Duration.ofMinutes(1))
                .withTimestampAssigner((event, ts) -> event.getEventTs()),
            "chr-source");

        DataStream<MrStat> mrStream = env.fromSource(mrSource,
            WatermarkStrategy.<MrStat>forMonotonousTimestamps()
                .withIdleness(Duration.ofMinutes(1)),
            "mr-source");

        DataStream<CmConfig> cmStream = env.fromSource(cmSource,
            WatermarkStrategy.<CmConfig>forMonotonousTimestamps()
                .withIdleness(Duration.ofMinutes(1)),
            "cm-source");

        // ── Enrichment pipeline: unify CHR + MR + CM → enrich → detect anomalies + KPI ──

        DataStream<InputEnvelope> chrEnv = chrStream
            .map(chr -> (InputEnvelope) new InputEnvelope.ChrEnv(chr))
            .name("to-chr-env");
        DataStream<InputEnvelope> mrEnv = mrStream
            .map(mr -> (InputEnvelope) new InputEnvelope.MrEnv(mr))
            .name("to-mr-env");
        DataStream<InputEnvelope> cmEnv = cmStream
            .map(cm -> (InputEnvelope) new InputEnvelope.CmEnv(cm))
            .name("to-cm-env");

        DataStream<InputEnvelope> mergedInput = chrEnv.union(mrEnv, cmEnv);

        DataStream<EnrichedChr> enriched = mergedInput
            .keyBy(InputEnvelope::cellId)
            .process(new EnrichmentProcessFunction())
            .name("enrichment")
            .uid("enrichment");

        // ── Anomaly detection ──

        DataStream<AnomalyEvent> anomalies = enriched
            .keyBy(ec -> ec.chrEvent().getCellId().toString())
            .process(new AnomalyDetector())
            .name("anomaly-detector")
            .uid("anomaly-detector");

        KafkaSink<AnomalyEvent> anomalySink = KafkaSink.<AnomalyEvent>builder()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("anomaly-events")
                .setValueSerializationSchema(new FlinkAvroSerializationSchema<>(AnomalyEvent.class))
                .build())
            .build();

        anomalies.sinkTo(anomalySink)
            .name("anomaly-kafka-sink");

        anomalies.sinkTo(JdbcSinks.anomalySink())
            .name("anomaly-jdbc-sink");

        // ── KPI aggregation (1-minute window) ──

        DataStream<CellKpi> cellKpi1m = enriched
            .keyBy(ec -> ec.chrEvent().getCellId().toString())
            .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
            .process(new CellKpiWindowFunction(WindowKind.MIN_1))
            .name("kpi-1m")
            .uid("kpi-1m");

        KafkaSink<CellKpi> cellKpiSink = KafkaSink.<CellKpi>builder()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("cell-kpi-1m")
                .setValueSerializationSchema(new FlinkAvroSerializationSchema<>(CellKpi.class))
                .build())
            .build();

        cellKpi1m.sinkTo(cellKpiSink)
            .name("cell-kpi-kafka-sink");

        cellKpi1m.sinkTo(JdbcSinks.cellKpiSink())
            .name("cell-kpi-jdbc-sink");

        cellKpi1m.sinkTo(HiveSinks.cellKpi1mSink())
            .name("cell-kpi-hive-sink");

        // ── Coordinator pipeline: lb-heartbeat → LoadCoordinator → lb-routing ──

        KafkaSource<String> heartbeatSource = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrap)
            .setTopics("lb-heartbeat")
            .setGroupId(groupId + "-coordinator")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<RoutingEntry> routingStream = env
            .fromSource(heartbeatSource,
                WatermarkStrategy.<String>forMonotonousTimestamps()
                    .withIdleness(Duration.ofMinutes(1)),
                "lb-heartbeat-source")
            .map(new HeartbeatParser())
            .name("heartbeat-parser")
            .keyBy(hb -> "coordinator")
            .process(new LoadCoordinator())
            .name("load-coordinator")
            .setParallelism(1);

        KafkaSink<String> routingSink = KafkaSink.<String>builder()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("lb-routing")
                .setValueSerializationSchema(new SimpleStringSchema())
                .build())
            .build();

        routingStream
            .map(RoutingEntry::toCsv)
            .name("routing-to-csv")
            .sinkTo(routingSink)
            .name("lb-routing-sink");

        env.execute("fdb-flink-job");
    }
}
