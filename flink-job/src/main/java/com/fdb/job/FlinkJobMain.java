package com.fdb.job;

import com.fdb.common.avro.*;
import com.fdb.job.coordinator.HeartbeatParser;
import com.fdb.job.coordinator.HeartbeatPayload;
import com.fdb.job.coordinator.LoadCoordinator;
import com.fdb.job.coordinator.RoutingEntry;
import com.fdb.job.coordinator.RoutingCsvSerializationSchema;
import com.fdb.common.hash.Hashes;
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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fdb.common.geo.Geohash;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

public class FlinkJobMain {

    private static final Logger log = LoggerFactory.getLogger(FlinkJobMain.class);
    private static final int DIRECT_ROUTE_VBUCKETS = 1024;
    private static final int DEFAULT_PARALLELISM = 4;

    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv().getOrDefault("FDB_KAFKA_BOOTSTRAP", "localhost:9092");
        String groupId = "fdb-flink-job";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(resolveCheckpointIntervalMs(System.getenv(), System.getProperties()));
        env.getCheckpointConfig().setCheckpointStorage(resolveCheckpointStorage(System.getenv(), System.getProperties()));
        env.setParallelism(resolveParallelism(System.getenv(), System.getProperties()));
        IcebergConfig icebergConfig = resolveIcebergConfig(System.getenv(), System.getProperties());
        boolean dynamicBalancingEnabled = resolveDynamicBalancingEnabled(System.getenv(), System.getProperties());

        // Main pipeline: CHR + PM + CFG

        KafkaSource<ChrEvent> chrSource = KafkaSource.<ChrEvent>builder()
            .setBootstrapServers(bootstrap)
            .setTopics("chr-events")
            .setGroupId(groupId + "-chr")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setDeserializer(new FlinkAvroDeserializer<>(ChrEvent.class))
            .build();

        KafkaSource<PmStat> pmSource = KafkaSource.<PmStat>builder()
            .setBootstrapServers(bootstrap)
            .setTopics("pm-stats")
            .setGroupId(groupId + "-pm")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setDeserializer(new FlinkAvroDeserializer<>(PmStat.class))
            .build();

        KafkaSource<CfgConfig> cfgSource = KafkaSource.<CfgConfig>builder()
            .setBootstrapServers(bootstrap)
            .setTopics("cfg-config")
            .setGroupId(groupId + "-cfg")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setDeserializer(new FlinkAvroDeserializer<>(CfgConfig.class))
            .build();

        DataStream<ChrEvent> chrStream = env.fromSource(chrSource,
            WatermarkStrategy.<ChrEvent>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withIdleness(Duration.ofMinutes(1))
                .withTimestampAssigner((event, ts) -> event.getEventTs()),
            "chr-source")
            .process(new StageMetricsProbe<>("chr-source", "CHR Source", "healthy", 5_000L))
            .name("chr-source-metrics");

        DataStream<PmStat> pmStream = env.fromSource(pmSource,
            WatermarkStrategy.<PmStat>forBoundedOutOfOrderness(Duration.ofMinutes(2))
                .withIdleness(Duration.ofMinutes(1))
                .withTimestampAssigner((pm, ts) -> pmEventTimestamp(pm)),
            "pm-source")
            .process(new StageMetricsProbe<>("pm-source", "PM Source", "healthy", 5_000L))
            .name("pm-source-metrics");

        DataStream<CfgConfig> cfgStream = env.fromSource(cfgSource,
            WatermarkStrategy.<CfgConfig>forMonotonousTimestamps()
                .withIdleness(Duration.ofMinutes(1)),
            "cfg-source")
            .process(new StageMetricsProbe<>("cfg-source", "CFG Source", "healthy", 5_000L))
            .name("cfg-source-metrics");

        // Enrichment pipeline: unify CHR + PM + CFG, enrich, detect anomalies and KPI

        DataStream<InputEnvelope> chrEnv = chrStream
            .map(chr -> (InputEnvelope) new InputEnvelope.ChrEnv(chr))
            .returns(new GenericTypeInfo<>(InputEnvelope.class))
            .name("to-chr-env");
        DataStream<InputEnvelope> pmEnv = pmStream
            .map(pm -> (InputEnvelope) new InputEnvelope.PmEnv(pm))
            .returns(new GenericTypeInfo<>(InputEnvelope.class))
            .name("to-pm-env");
        DataStream<InputEnvelope> cfgEnv = cfgStream
            .map(cfg -> (InputEnvelope) new InputEnvelope.CfgEnv(cfg))
            .returns(new GenericTypeInfo<>(InputEnvelope.class))
            .name("to-cfg-env");

        DataStream<InputEnvelope> mergedInput = chrEnv.union(pmEnv, cfgEnv)
            .process(new StageMetricsProbe<>("kafka", "Kafka Topics", "healthy", 5_000L))
            .name("kafka-topics-metrics");

        DataStream<RoutedEnvelope> assigned;
        if (dynamicBalancingEnabled) {
            assigned = buildDynamicallyAssignedStream(env, mergedInput, bootstrap, groupId);
        } else {
            assigned = mergedInput
                .map(FlinkJobMain::directRoute)
                .returns(new GenericTypeInfo<>(RoutedEnvelope.class))
                .name("direct-cell-routing");
        }

        SingleOutputStreamOperator<EnrichedChr> enrichedRaw = assigned
            .keyBy(RoutedEnvelope::stateKey)
            .process(new EnrichmentProcessFunction(), new GenericTypeInfo<>(EnrichedChr.class))
            .name("enrichment")
            .uid("enrichment");
        DataStream<EnrichedChr> enriched = enrichedRaw
            .process(new StageMetricsProbe<>("enrichment", "Enrichment Process", "healthy", 5_000L))
            .name("enrichment-metrics");

        KafkaSink<ChrEvent> chrDlqSink = KafkaSink.<ChrEvent>builder()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("chr-dlq")
                .setValueSerializationSchema(new FlinkAvroSerializationSchema<>(ChrEvent.class))
                .build())
            .build();
        enrichedRaw.getSideOutput(EnrichmentProcessFunction.CHR_DLQ)
            .sinkTo(chrDlqSink).name("chr-dlq-sink");

        // Anomaly detection

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
        cellAnomalies
            .process(new SinkLatencyProbe<>("kafka-cell-anomaly", "Cell Anomaly Kafka Sink", "kafka",
                "cell_anomaly_events", "ANOMALY", 100), new GenericTypeInfo<>(AnomalyEvent.class))
            .startNewChain()
            .name("kafka-cell-anomaly")
            .sinkTo(anomalySink(bootstrap, "cell-anomaly-events"))
            .name("cell-anomaly-kafka-sink");
        coverageAnomalies
            .process(new SinkLatencyProbe<>("kafka-grid-anomaly", "Grid Anomaly Kafka Sink", "kafka",
                "grid_anomaly_events", "ANOMALY", 100), new GenericTypeInfo<>(AnomalyEvent.class))
            .startNewChain()
            .name("kafka-grid-anomaly")
            .sinkTo(anomalySink(bootstrap, "grid-anomaly-events"))
            .name("grid-anomaly-kafka-sink");

        cellAnomalies
            .process(new SinkLatencyProbe<>("starrocks-cell-anomaly", "Cell Anomaly StarRocks Sink", "starrocks",
                "cell_anomaly_events", "ANOMALY", 100), new GenericTypeInfo<>(AnomalyEvent.class))
            .startNewChain()
            .name("starrocks-cell-anomaly")
            .sinkTo(StarRocksSinks.cellAnomalySink())
            .name("cell-anomaly-starrocks-sink");
        coverageAnomalies
            .process(new SinkLatencyProbe<>("starrocks-grid-anomaly", "Grid Anomaly StarRocks Sink", "starrocks",
                "grid_anomaly_events", "ANOMALY", 100), new GenericTypeInfo<>(AnomalyEvent.class))
            .startNewChain()
            .name("starrocks-grid-anomaly")
            .sinkTo(StarRocksSinks.gridAnomalySink())
            .name("grid-anomaly-starrocks-sink");

        // KPI aggregation (1-minute CHR/PM event-time full join)

        DataStream<ChrMinuteFact> chrMinuteFacts = chrStream
            .keyBy(chr -> chr.getCellId().toString())
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            .process(new ChrMinuteFactWindowFunction(), new GenericTypeInfo<>(ChrMinuteFact.class))
            .name("chr-1m-fact");

        DataStream<PmMinuteFact> pmMinuteFacts = pmStream
            .keyBy(pm -> pm.getCellId().toString())
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            .process(new PmMinuteFactWindowFunction(), new GenericTypeInfo<>(PmMinuteFact.class))
            .name("pm-1m-fact");

        DataStream<MinuteFactEnvelope> chrFactEnv = chrMinuteFacts
            .map(MinuteFactEnvelope::chr)
            .returns(new GenericTypeInfo<>(MinuteFactEnvelope.class))
            .name("to-chr-minute-fact-env");
        DataStream<MinuteFactEnvelope> pmFactEnv = pmMinuteFacts
            .map(MinuteFactEnvelope::pm)
            .returns(new GenericTypeInfo<>(MinuteFactEnvelope.class))
            .name("to-pm-minute-fact-env");
        DataStream<MinuteFactEnvelope> cfgMinuteEnv = cfgStream
            .map(MinuteFactEnvelope::cfg)
            .returns(new GenericTypeInfo<>(MinuteFactEnvelope.class))
            .name("to-cfg-minute-fact-env");

        DataStream<CellKpi> cellKpi1m = chrFactEnv.union(pmFactEnv, cfgMinuteEnv)
            .keyBy(MinuteFactEnvelope::cellId)
            .process(new MinuteKpiJoinFunction(Duration.ofMinutes(2)), new GenericTypeInfo<>(CellKpi.class))
            .name("kpi-1m-full-join")
            .uid("kpi-1m-full-join");

        KafkaSink<CellKpi> cellKpiSink = KafkaSink.<CellKpi>builder()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("cell-kpi-1m")
                .setValueSerializationSchema(new FlinkAvroSerializationSchema<>(CellKpi.class))
                .build())
            .build();

        cellKpi1m
            .process(new SinkLatencyProbe<>("kafka-kpi-1m", "Cell KPI 1m Kafka Sink", "kafka",
                "kpi_1m", "MIN_1", 100), new GenericTypeInfo<>(CellKpi.class))
            .startNewChain()
            .name("kafka-kpi-1m")
            .sinkTo(cellKpiSink)
            .name("cell-kpi-kafka-sink");

        cellKpi1m
            .process(new SinkLatencyProbe<>("starrocks-kpi-1m", "Cell KPI 1m StarRocks Sink", "starrocks",
                "kpi_1m", "MIN_1", 100), new GenericTypeInfo<>(CellKpi.class))
            .startNewChain()
            .name("starrocks-kpi-1m")
            .sinkTo(JdbcSinks.cellKpiSink())
            .name("cell-kpi-jdbc-sink");

        cellKpi1m
            .process(new SinkLatencyProbe<>("hive-kpi-1m", "Cell KPI 1m Hive Sink", "hive",
                "kpi_1m", "MIN_1", 100), new GenericTypeInfo<>(CellKpi.class))
            .startNewChain()
            .name("hive-kpi-1m")
            .sinkTo(HiveSinks.cellKpiSink("MIN_1"))
            .name("cell-kpi-hive-sink");

        DataStream<CellKpi> cellKpi5m = cellKpi1m
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<CellKpi>forBoundedOutOfOrderness(Duration.ofMinutes(2))
                    .withIdleness(Duration.ofMinutes(1))
                    .withTimestampAssigner((kpi, ts) -> Math.subtractExact(kpi.getWindowEndTs(), 1L)))
            .keyBy(kpi -> kpi.getCellId().toString())
            .window(TumblingEventTimeWindows.of(Time.minutes(5)))
            .process(new CellKpiRollupAggregator(), new GenericTypeInfo<>(CellKpi.class))
            .name("kpi-5m-rollup")
            .uid("kpi-5m-rollup");

        KafkaSink<CellKpi> cellKpi5mSink = KafkaSink.<CellKpi>builder()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("cell-kpi-5m")
                .setValueSerializationSchema(new FlinkAvroSerializationSchema<>(CellKpi.class))
                .build())
            .build();

        cellKpi5m
            .process(new SinkLatencyProbe<>("kafka-kpi-5m", "Cell KPI 5m Kafka Sink", "kafka",
                "kpi_5m", "MIN_5", 100), new GenericTypeInfo<>(CellKpi.class))
            .startNewChain()
            .name("kafka-kpi-5m")
            .sinkTo(cellKpi5mSink)
            .name("cell-kpi-5m-kafka-sink");
        cellKpi5m
            .process(new SinkLatencyProbe<>("starrocks-kpi-5m", "Cell KPI 5m StarRocks Sink", "starrocks",
                "kpi_5m", "MIN_5", 100), new GenericTypeInfo<>(CellKpi.class))
            .startNewChain()
            .name("starrocks-kpi-5m")
            .sinkTo(JdbcSinks.cellKpiSink())
            .name("cell-kpi-5m-jdbc-sink");
        cellKpi5m
            .process(new SinkLatencyProbe<>("hive-kpi-5m", "Cell KPI 5m Hive Sink", "hive",
                "kpi_5m", "MIN_5", 100), new GenericTypeInfo<>(CellKpi.class))
            .startNewChain()
            .name("hive-kpi-5m")
            .sinkTo(HiveSinks.cellKpiSink("MIN_5"))
            .name("cell-kpi-5m-hive-sink");

        if (icebergConfig.enabled()) {
            DataStream<RowData> icebergKpi1m = cellKpi1m
                .process(new SinkLatencyProbe<>("iceberg-kpi-1m", "Cell KPI 1m Iceberg Sink", "iceberg",
                    "kpi_1m", "MIN_1", 100), new GenericTypeInfo<>(CellKpi.class))
                .startNewChain()
                .name("iceberg-kpi-1m")
                .map(new CellKpiIcebergMapper())
                .returns(new GenericTypeInfo<>(RowData.class))
                .name("cell-kpi-iceberg-map");
            DataStream<RowData> icebergKpi5m = cellKpi5m
                .process(new SinkLatencyProbe<>("iceberg-kpi-5m", "Cell KPI 5m Iceberg Sink", "iceberg",
                    "kpi_5m", "MIN_5", 100), new GenericTypeInfo<>(CellKpi.class))
                .startNewChain()
                .name("iceberg-kpi-5m")
                .map(new CellKpiIcebergMapper())
                .returns(new GenericTypeInfo<>(RowData.class))
                .name("cell-kpi-5m-iceberg-map");
            IcebergSinks.appendCellKpiSink(icebergKpi1m.union(icebergKpi5m), icebergConfig)
                .name("cell-kpi-iceberg-sink");
        }

        env.execute("fdb-flink-job");
    }

    private static DataStream<RoutedEnvelope> buildDynamicallyAssignedStream(
        StreamExecutionEnvironment env,
        DataStream<InputEnvelope> mergedInput,
        String bootstrap,
        String groupId) {
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
        DataStream<RoutedEnvelope> assigned = metered
            .process(new StageMetricsProbe<>("assigner", "VBucket Assigner", "healthy", 5_000L))
            .name("vbucket-assigner-metrics");

        KafkaSink<String> heartbeatKafkaSink = KafkaSink.<String>builder()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("lb-heartbeat").setValueSerializationSchema(new SimpleStringSchema()).build())
            .build();
        metered.getSideOutput(VBucketLoadMeter.HEARTBEATS)
            .sinkTo(heartbeatKafkaSink).name("lb-heartbeat-sink");

        // Coordinator pipeline: lb-heartbeat to LoadCoordinator to lb-routing

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
            .setParallelism(1)
            .process(new StageMetricsProbe<>("load-coordinator", "Load Coordinator", "healthy", 5_000L))
            .name("load-coordinator-metrics");

        KafkaSink<String> routingSink = KafkaSink.<String>builder()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(new RoutingCsvSerializationSchema("lb-routing"))
            .build();

        routingStream
            .map(RoutingEntry::toCsv)
            .name("routing-to-csv")
            .sinkTo(routingSink)
            .name("lb-routing-sink");

        return assigned;
    }

    static RoutedEnvelope directRoute(InputEnvelope envelope) {
        return new RoutedEnvelope(envelope, Hashes.toVBucket(envelope.cellId(), DIRECT_ROUTE_VBUCKETS));
    }

    private static KafkaSink<AnomalyEvent> anomalySink(String bootstrap, String topic) {
        return KafkaSink.<AnomalyEvent>builder()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(topic)
                .setValueSerializationSchema(new FlinkAvroSerializationSchema<>(AnomalyEvent.class))
                .build())
            .build();
    }

    static boolean resolveDynamicBalancingEnabled(Map<String, String> env, Properties properties) {
        String configured = env.get("FDB_DYNAMIC_BALANCING_ENABLED");
        if (configured == null || configured.isBlank()) {
            configured = properties.getProperty("fdb.dynamic.balancing.enabled");
        }
        return configured != null && "true".equalsIgnoreCase(configured.trim());
    }

    static int resolveParallelism(Map<String, String> env, Properties properties) {
        String configured = env.get("FDB_FLINK_PARALLELISM");
        if (configured == null || configured.isBlank()) {
            configured = properties.getProperty("fdb.flink.parallelism");
        }
        if (configured == null || configured.isBlank()) {
            return DEFAULT_PARALLELISM;
        }
        try {
            int parallelism = Integer.parseInt(configured.trim());
            return parallelism > 0 ? parallelism : DEFAULT_PARALLELISM;
        } catch (NumberFormatException e) {
            log.warn("Invalid Flink parallelism '{}', falling back to {}", configured, DEFAULT_PARALLELISM);
            return DEFAULT_PARALLELISM;
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

    static IcebergConfig resolveIcebergConfig(Map<String, String> env, Properties properties) {
        return IcebergConfig.resolve(env, properties);
    }

    static long pmEventTimestamp(PmStat pm) {
        long windowStartTs = pm.getWindowStartTs();
        long windowEndTs = pm.getWindowEndTs();
        if (windowEndTs == Long.MIN_VALUE || windowEndTs <= 0L) {
            throw new IllegalArgumentException("Invalid PM windowEndTs: " + windowEndTs);
        }
        if (windowEndTs <= windowStartTs) {
            throw new IllegalArgumentException(
                "Invalid PM windowStartTs/windowEndTs: " + windowStartTs + ".." + windowEndTs);
        }
        return Math.subtractExact(windowEndTs, 1L);
    }
}
