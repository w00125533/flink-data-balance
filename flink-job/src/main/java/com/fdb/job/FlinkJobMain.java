package com.fdb.job;

import com.fdb.common.avro.*;
import com.fdb.job.coordinator.HeartbeatParser;
import com.fdb.job.coordinator.HeartbeatPayload;
import com.fdb.job.coordinator.LoadCoordinator;
import com.fdb.job.coordinator.RoutingEntry;
import com.fdb.job.coordinator.RoutingCsvSerializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fdb.common.geo.Geohash;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

public class FlinkJobMain {

    private static final Logger log = LoggerFactory.getLogger(FlinkJobMain.class);

    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv().getOrDefault("FDB_KAFKA_BOOTSTRAP", "localhost:9092");
        String groupId = "fdb-flink-job";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(resolveCheckpointIntervalMs(System.getenv(), System.getProperties()));
        env.getCheckpointConfig().setCheckpointStorage(resolveCheckpointStorage(System.getenv(), System.getProperties()));
        env.setParallelism(resolveParallelism(System.getenv(), System.getProperties()));

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
            .returns(new GenericTypeInfo<>(InputEnvelope.class))
            .name("to-chr-env");
        DataStream<InputEnvelope> mrEnv = mrStream
            .map(mr -> (InputEnvelope) new InputEnvelope.MrEnv(mr))
            .returns(new GenericTypeInfo<>(InputEnvelope.class))
            .name("to-mr-env");
        DataStream<InputEnvelope> cmEnv = cmStream
            .map(cm -> (InputEnvelope) new InputEnvelope.CmEnv(cm))
            .returns(new GenericTypeInfo<>(InputEnvelope.class))
            .name("to-cm-env");

        DataStream<InputEnvelope> mergedInput = chrEnv.union(mrEnv, cmEnv);

        KafkaSource<String> routingSource = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrap).setTopics("lb-routing")
            .setGroupId(groupId + "-routing").setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema()).build();
        BroadcastStream<String> routingBroadcast = env.fromSource(routingSource,
            WatermarkStrategy.noWatermarks(), "lb-routing-source")
            .broadcast(RoutingAssigner.ROUTING_STATE);
        SingleOutputStreamOperator<RoutedEnvelope> metered = mergedInput.connect(routingBroadcast)
            .process(new RoutingAssigner(), new GenericTypeInfo<>(RoutedEnvelope.class)).name("routing-assigner")
            .keyBy(RoutedEnvelope::vbucketId)
            .process(new VBucketLoadMeter(), new GenericTypeInfo<>(RoutedEnvelope.class))
            .name("vbucket-load-meter");

        KafkaSink<String> heartbeatKafkaSink = KafkaSink.<String>builder()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("lb-heartbeat").setValueSerializationSchema(new SimpleStringSchema()).build())
            .build();
        metered.getSideOutput(VBucketLoadMeter.HEARTBEATS)
            .sinkTo(heartbeatKafkaSink).name("lb-heartbeat-sink");

        SingleOutputStreamOperator<EnrichedChr> enriched = metered
            .keyBy(RoutedEnvelope::stateKey)
            .process(new EnrichmentProcessFunction(), new GenericTypeInfo<>(EnrichedChr.class))
            .name("enrichment")
            .uid("enrichment");

        KafkaSink<ChrEvent> chrDlqSink = KafkaSink.<ChrEvent>builder()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("chr-dlq")
                .setValueSerializationSchema(new FlinkAvroSerializationSchema<>(ChrEvent.class))
                .build())
            .build();
        enriched.getSideOutput(EnrichmentProcessFunction.CHR_DLQ)
            .sinkTo(chrDlqSink).name("chr-dlq-sink");

        // ── Anomaly detection ──

        RuleConfig rules = JobConfig.load().rules();
        DataStream<AnomalyEvent> cellAnomalies = enriched
            .keyBy(ec -> ec.chrEvent().getCellId().toString())
            .process(new AnomalyDetector(rules), new GenericTypeInfo<>(AnomalyEvent.class))
            .name("anomaly-detector")
            .uid("anomaly-detector");
        DataStream<AnomalyEvent> coverageAnomalies = enriched
            .keyBy(ec -> Geohash.encode(ec.chrEvent().getLatitude(), ec.chrEvent().getLongitude(), 6))
            .process(new CoverageHoleDetector(rules), new GenericTypeInfo<>(AnomalyEvent.class))
            .name("coverage-hole-detector")
            .uid("coverage-hole-detector");
        DataStream<AnomalyEvent> anomalies = cellAnomalies.union(coverageAnomalies);

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
            .process(new CellKpiWindowFunction(WindowKind.MIN_1), new GenericTypeInfo<>(CellKpi.class))
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

        cellKpi1m.sinkTo(HiveSinks.cellKpiSink("MIN_1"))
            .name("cell-kpi-hive-sink");

        DataStream<CellKpi> cellKpi5m = enriched
            .keyBy(ec -> ec.chrEvent().getCellId().toString())
            .window(TumblingProcessingTimeWindows.of(Time.minutes(5)))
            .process(new CellKpiWindowFunction(WindowKind.MIN_5), new GenericTypeInfo<>(CellKpi.class))
            .name("kpi-5m")
            .uid("kpi-5m");

        KafkaSink<CellKpi> cellKpi5mSink = KafkaSink.<CellKpi>builder()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("cell-kpi-5m")
                .setValueSerializationSchema(new FlinkAvroSerializationSchema<>(CellKpi.class))
                .build())
            .build();

        cellKpi5m.sinkTo(cellKpi5mSink).name("cell-kpi-5m-kafka-sink");
        cellKpi5m.sinkTo(JdbcSinks.cellKpiSink()).name("cell-kpi-5m-jdbc-sink");
        cellKpi5m.sinkTo(HiveSinks.cellKpiSink("MIN_5")).name("cell-kpi-5m-hive-sink");

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
            .returns(new GenericTypeInfo<>(HeartbeatPayload.class))
            .name("heartbeat-parser")
            .keyBy(hb -> "coordinator")
            .process(new LoadCoordinator(), new GenericTypeInfo<>(RoutingEntry.class))
            .name("load-coordinator")
            .setParallelism(1);

        KafkaSink<String> routingSink = KafkaSink.<String>builder()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(new RoutingCsvSerializationSchema("lb-routing"))
            .build();

        routingStream
            .map(RoutingEntry::toCsv)
            .name("routing-to-csv")
            .sinkTo(routingSink)
            .name("lb-routing-sink");

        env.execute("fdb-flink-job");
    }

    static int resolveParallelism(Map<String, String> env, Properties properties) {
        String configured = env.get("FDB_FLINK_PARALLELISM");
        if (configured == null || configured.isBlank()) {
            configured = properties.getProperty("fdb.flink.parallelism");
        }
        if (configured == null || configured.isBlank()) {
            return 1;
        }
        try {
            int parallelism = Integer.parseInt(configured.trim());
            return parallelism > 0 ? parallelism : 1;
        } catch (NumberFormatException e) {
            log.warn("Invalid Flink parallelism '{}', falling back to 1", configured);
            return 1;
        }
    }

    static long resolveCheckpointIntervalMs(Map<String, String> env, Properties properties) {
        String configured = env.get("FDB_FLINK_CHECKPOINT_INTERVAL_MS");
        if (configured == null || configured.isBlank()) {
            configured = properties.getProperty("fdb.flink.checkpoint.interval.ms");
        }
        if (configured == null || configured.isBlank()) {
            return 60_000L;
        }
        try {
            long intervalMs = Long.parseLong(configured.trim());
            return intervalMs > 0 ? intervalMs : 60_000L;
        } catch (NumberFormatException e) {
            log.warn("Invalid Flink checkpoint interval '{}', falling back to 60000 ms", configured);
            return 60_000L;
        }
    }

    static String resolveCheckpointStorage(Map<String, String> env, Properties properties) {
        String configured = env.get("FDB_FLINK_CHECKPOINT_DIR");
        if (configured == null || configured.isBlank()) {
            configured = properties.getProperty("fdb.flink.checkpoint.dir");
        }
        if (configured == null || configured.isBlank()) {
            return "file:///tmp/fdb-checkpoints";
        }
        return configured.trim();
    }
}
