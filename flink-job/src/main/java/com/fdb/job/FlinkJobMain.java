package com.fdb.job;

import com.fdb.job.anomaly.CellKpiCepAnomalyDetector;
import com.fdb.job.anomaly.CoverageHoleDetector;
import com.fdb.job.anomaly.UserEventCepAnomalyDetector;
import com.fdb.job.balance.RoutingAssigner;
import com.fdb.job.balance.VBucketLoadMeter;
import com.fdb.job.config.JobConfig;
import com.fdb.job.config.ResultSinkConfig;
import com.fdb.job.config.ResultSinkType;
import com.fdb.job.config.RuleConfig;
import com.fdb.job.enrich.EnrichmentProcessFunction;
import com.fdb.job.kpi.CellKpiRollupAggregator;
import com.fdb.job.kpi.ChrMinuteFactAccumulator;
import com.fdb.job.kpi.ChrMinuteFactAggregateFunction;
import com.fdb.job.kpi.ChrMinuteFactWindowFunction;
import com.fdb.job.kpi.MinuteKpiJoinFunction;
import com.fdb.job.kpi.PmMinuteFactWindowFunction;
import com.fdb.job.metrics.StageMetricsProbe;
import com.fdb.job.metrics.LatencyTimestampExtractor;
import com.fdb.job.metrics.MetricRuntimeConfig;
import com.fdb.job.metrics.WindowMaterializationProbe;
import com.fdb.job.model.ChrMinuteFact;
import com.fdb.job.model.EnrichedChr;
import com.fdb.job.model.InputEnvelope;
import com.fdb.job.model.MinuteFactEnvelope;
import com.fdb.job.model.PmMinuteFact;
import com.fdb.job.model.RoutedEnvelope;
import com.fdb.job.sink.IcebergConfig;
import com.fdb.job.sink.ResultSinks;
import com.fdb.job.source.FlinkAvroDeserializer;
import com.fdb.job.source.FlinkAvroSerializationSchema;
import com.fdb.common.avro.*;
import com.fdb.job.balance.coordinator.HeartbeatParser;
import com.fdb.job.balance.coordinator.HeartbeatPayload;
import com.fdb.job.balance.coordinator.LoadCoordinator;
import com.fdb.job.balance.coordinator.RoutingEntry;
import com.fdb.job.balance.coordinator.RoutingCsvSerializationSchema;
import com.fdb.common.hash.Hashes;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
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
        Map<String, String> envVars = System.getenv();
        Properties systemProperties = System.getProperties();
        ResultSinkConfig resultSinkConfig = ResultSinkConfig.resolve(envVars, systemProperties);
        String bootstrap = envVars.getOrDefault("FDB_KAFKA_BOOTSTRAP", "localhost:9092");
        String groupId = "fdb-flink-job";
        IcebergConfig icebergConfig = resolveIcebergConfig(envVars, systemProperties);
        Properties kafkaConsumerProperties = resolveKafkaConsumerProperties(envVars, systemProperties);
        Duration chrWatermarkOutOfOrderness = resolveChrWatermarkOutOfOrderness(envVars, systemProperties);
        Duration pmWatermarkOutOfOrderness = resolvePmWatermarkOutOfOrderness(envVars, systemProperties);
        Duration kpiJoinWait = resolveKpiJoinWait(envVars, systemProperties);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(effectiveCheckpointIntervalMs(
            resultSinkConfig.resultSink(),
            resolveCheckpointIntervalMs(envVars, systemProperties)));
        env.getCheckpointConfig().setCheckpointStorage(resolveCheckpointStorage(envVars, systemProperties));
        int parallelism = resolveParallelism(envVars, systemProperties);
        env.setParallelism(parallelism);
        int enrichmentParallelism = resolveStageParallelism(envVars, systemProperties,
            "FDB_FLINK_ENRICHMENT_PARALLELISM", "fdb.flink.enrichment.parallelism", parallelism);
        int userAnomalyParallelism = resolveStageParallelism(envVars, systemProperties,
            "FDB_FLINK_USER_ANOMALY_PARALLELISM", "fdb.flink.user.anomaly.parallelism", parallelism);
        int gridAnomalyParallelism = resolveStageParallelism(envVars, systemProperties,
            "FDB_FLINK_GRID_ANOMALY_PARALLELISM", "fdb.flink.grid.anomaly.parallelism", parallelism);
        int kpiParallelism = resolveStageParallelism(envVars, systemProperties,
            "FDB_FLINK_KPI_PARALLELISM", "fdb.flink.kpi.parallelism", parallelism);
        boolean dynamicBalancingEnabled = resolveDynamicBalancingEnabled(envVars, systemProperties);
        boolean diagnosticChainingEnabled = resolveDiagnosticChainingEnabled(envVars, systemProperties);
        MetricRuntimeConfig metricConfig = MetricRuntimeConfig.from(resultSinkConfig, parallelism);

        // Main pipeline: CHR + PM + CFG

        KafkaSource<ChrEvent> chrSource = KafkaSource.<ChrEvent>builder()
            .setBootstrapServers(bootstrap)
            .setTopics("chr-events")
            .setGroupId(groupId + "-chr")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setDeserializer(new FlinkAvroDeserializer<>(ChrEvent.class))
            .setProperties(kafkaConsumerProperties)
            .build();

        KafkaSource<PmStat> pmSource = KafkaSource.<PmStat>builder()
            .setBootstrapServers(bootstrap)
            .setTopics("pm-stats")
            .setGroupId(groupId + "-pm")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setDeserializer(new FlinkAvroDeserializer<>(PmStat.class))
            .setProperties(kafkaConsumerProperties)
            .build();

        KafkaSource<CfgConfig> cfgSource = KafkaSource.<CfgConfig>builder()
            .setBootstrapServers(bootstrap)
            .setTopics("cfg-config")
            .setGroupId(groupId + "-cfg")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setDeserializer(new FlinkAvroDeserializer<>(CfgConfig.class))
            .setProperties(kafkaConsumerProperties)
            .build();

        DataStream<ChrEvent> chrStream = env.fromSource(chrSource,
            WatermarkStrategy.<ChrEvent>forBoundedOutOfOrderness(chrWatermarkOutOfOrderness)
                .withIdleness(Duration.ofMinutes(1))
                .withTimestampAssigner((event, ts) -> event.getEventTs()),
            "chr-source")
            .process(stageMetricsProbe("chr-source", "CHR Source", "healthy", resultSinkConfig, metricConfig,
                ChrEvent::getEventTs))
            .name("chr-source-metrics");

        DataStream<PmStat> pmStream = env.fromSource(pmSource,
            WatermarkStrategy.<PmStat>forBoundedOutOfOrderness(pmWatermarkOutOfOrderness)
                .withIdleness(Duration.ofMinutes(1))
                .withTimestampAssigner((pm, ts) -> pmEventTimestamp(pm)),
            "pm-source")
            .process(stageMetricsProbe("pm-source", "PM Source", "healthy", resultSinkConfig, metricConfig,
                PmStat::getEventTs))
            .name("pm-source-metrics");

        DataStream<CfgConfig> cfgStream = env.fromSource(cfgSource,
            WatermarkStrategy.<CfgConfig>forMonotonousTimestamps()
                .withIdleness(Duration.ofMinutes(1)),
            "cfg-source")
            .process(stageMetricsProbe("cfg-source", "CFG Source", "healthy", resultSinkConfig, metricConfig,
                CfgConfig::getEffectiveTs))
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
            .process(stageMetricsProbe("kafka", "Kafka Topics", "healthy", resultSinkConfig, metricConfig,
                FlinkJobMain::inputEnvelopeLatencyTimestamp))
            .name("kafka-topics-metrics");

        DataStream<RoutedEnvelope> assigned;
        if (dynamicBalancingEnabled) {
            assigned = buildDynamicallyAssignedStream(env, mergedInput, bootstrap, groupId,
                resultSinkConfig, metricConfig);
        } else {
            SingleOutputStreamOperator<RoutedEnvelope> directAssigned = mergedInput
                .map(FlinkJobMain::directRoute)
                .returns(new GenericTypeInfo<>(RoutedEnvelope.class))
                .name("direct-cell-routing");
            assigned = disableChainingIfDiagnostic(directAssigned, diagnosticChainingEnabled);
        }

        SingleOutputStreamOperator<EnrichedChr> enrichedRaw = disableChainingIfDiagnostic(assigned
            .keyBy(RoutedEnvelope::stateKey)
            .process(new EnrichmentProcessFunction(), new GenericTypeInfo<>(EnrichedChr.class))
            .setParallelism(enrichmentParallelism)
            .name("enrichment")
            .uid("enrichment"), diagnosticChainingEnabled);
        SingleOutputStreamOperator<EnrichedChr> enrichedWithMetrics = splitChainIfDiagnostic(enrichedRaw
            .process(stageMetricsProbe("enrichment", "Enrichment Process", "healthy", resultSinkConfig, metricConfig,
                enrichedChr -> enrichedChr.chrEvent().getEventTs()))
            .setParallelism(enrichmentParallelism)
            .name("enrichment-metrics"), diagnosticChainingEnabled);
        DataStream<EnrichedChr> enriched = disableChainingIfDiagnostic(enrichedWithMetrics, diagnosticChainingEnabled);

        if (resultSinkConfig.dlqEnabled()) {
            KafkaSink<ChrEvent> enrichmentLateSink = KafkaSink.<ChrEvent>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                    .setTopic("enrichment-late")
                    .setValueSerializationSchema(new FlinkAvroSerializationSchema<>(ChrEvent.class))
                    .build())
                .build();
            DataStreamSink<ChrEvent> lateSink = enrichedRaw.getSideOutput(EnrichmentProcessFunction.ENRICHMENT_LATE)
                .sinkTo(enrichmentLateSink).name("enrichment-late-sink");
            if (diagnosticChainingEnabled) {
                lateSink.disableChaining();
            }
        }

        // Anomaly detection

        RuleConfig rules = JobConfig.load().rules();
        DataStream<AnomalyEvent> userAnomalies = UserEventCepAnomalyDetector
            .detect(enriched, rules, diagnosticChainingEnabled, userAnomalyParallelism);
        SingleOutputStreamOperator<AnomalyEvent> userAnomaliesWithMetrics = splitChainIfDiagnostic(userAnomalies
            .process(stageMetricsProbe("user-anomaly", "User Anomaly Detection", "healthy",
                resultSinkConfig, metricConfig, FlinkJobMain::anomalyEventLatencyTimestamp))
            .setParallelism(userAnomalyParallelism)
            .name("user-anomaly-metrics"), diagnosticChainingEnabled);
        userAnomalies = disableChainingIfDiagnostic(userAnomaliesWithMetrics, diagnosticChainingEnabled);
        SingleOutputStreamOperator<EnrichedChr> coverageCandidates = splitChainIfDiagnostic(enriched
            .filter(candidate -> isCoverageCandidate(candidate, rules))
            .setParallelism(gridAnomalyParallelism)
            .name("coverage-low-signal-filter"), diagnosticChainingEnabled);
        SingleOutputStreamOperator<AnomalyEvent> coverageDetected = coverageCandidates
            .keyBy(ec -> Geohash.encode(ec.chrEvent().getLatitude(), ec.chrEvent().getLongitude(), 6))
            .process(new CoverageHoleDetector(rules), new GenericTypeInfo<>(AnomalyEvent.class))
            .setParallelism(gridAnomalyParallelism)
            .name("coverage-hole-detector")
            .uid("coverage-hole-detector");
        coverageDetected = disableChainingIfDiagnostic(coverageDetected, diagnosticChainingEnabled);
        SingleOutputStreamOperator<AnomalyEvent> coverageAnomaliesWithMetrics = splitChainIfDiagnostic(coverageDetected
            .process(stageMetricsProbe("grid-anomaly", "Grid Anomaly Detection", "healthy",
                resultSinkConfig, metricConfig, FlinkJobMain::anomalyEventLatencyTimestamp))
            .setParallelism(gridAnomalyParallelism)
            .name("grid-anomaly-metrics"), diagnosticChainingEnabled);
        DataStream<AnomalyEvent> coverageAnomalies =
            disableChainingIfDiagnostic(coverageAnomaliesWithMetrics, diagnosticChainingEnabled);

        // KPI aggregation (1-minute CHR/PM event-time full join)

        DataStream<ChrMinuteFact> chrMinuteFacts = chrStream
            .keyBy(chr -> chr.getCellId().toString())
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            .aggregate(
                new ChrMinuteFactAggregateFunction(),
                new ChrMinuteFactWindowFunction(),
                new GenericTypeInfo<>(ChrMinuteFactAccumulator.class),
                new GenericTypeInfo<>(ChrMinuteFactAccumulator.class),
                new GenericTypeInfo<>(ChrMinuteFact.class))
            .setParallelism(kpiParallelism)
            .name("chr-1m-fact")
            .process(windowMaterializationProbe(
                "window-chr-1m", "CHR 1m Materialization", "chr-1m", "MIN_1",
                fact -> fact.minuteTs() + 60_000L, ChrMinuteFact::sourceEventTsAvg, metricConfig))
            .setParallelism(kpiParallelism)
            .name("window-chr-1m-materialization");

        DataStream<PmMinuteFact> pmMinuteFacts = pmStream
            .keyBy(pm -> pm.getCellId().toString())
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            .process(new PmMinuteFactWindowFunction(), new GenericTypeInfo<>(PmMinuteFact.class))
            .setParallelism(kpiParallelism)
            .name("pm-1m-fact")
            .process(windowMaterializationProbe(
                "window-pm-1m", "PM 1m Materialization", "pm-1m", "MIN_1",
                fact -> fact.minuteTs() + 60_000L, PmMinuteFact::sourceEventTsAvg, metricConfig))
            .setParallelism(kpiParallelism)
            .name("window-pm-1m-materialization");

        DataStream<MinuteFactEnvelope> chrFactEnv = chrMinuteFacts
            .process(new ProcessFunction<ChrMinuteFact, MinuteFactEnvelope>() {
                @Override
                public void processElement(ChrMinuteFact value, Context ctx, Collector<MinuteFactEnvelope> out) {
                    out.collect(MinuteFactEnvelope.chr(value));
                }
            }, new GenericTypeInfo<>(MinuteFactEnvelope.class))
            .setParallelism(kpiParallelism)
            .name("to-chr-minute-fact-env");
        DataStream<MinuteFactEnvelope> pmFactEnv = pmMinuteFacts
            .process(new ProcessFunction<PmMinuteFact, MinuteFactEnvelope>() {
                @Override
                public void processElement(PmMinuteFact value, Context ctx, Collector<MinuteFactEnvelope> out) {
                    out.collect(MinuteFactEnvelope.pm(value));
                }
            }, new GenericTypeInfo<>(MinuteFactEnvelope.class))
            .setParallelism(kpiParallelism)
            .name("to-pm-minute-fact-env");
        DataStream<MinuteFactEnvelope> cfgMinuteEnv = cfgStream
            .process(new ProcessFunction<CfgConfig, MinuteFactEnvelope>() {
                @Override
                public void processElement(CfgConfig value, Context ctx, Collector<MinuteFactEnvelope> out) {
                    out.collect(MinuteFactEnvelope.cfg(value));
                }
            }, new GenericTypeInfo<>(MinuteFactEnvelope.class))
            .setParallelism(kpiParallelism)
            .name("to-cfg-minute-fact-env");

        DataStream<CellKpi> cellKpi1m = chrFactEnv.union(pmFactEnv, cfgMinuteEnv)
            .keyBy(MinuteFactEnvelope::cellId)
            .process(new MinuteKpiJoinFunction(kpiJoinWait), new GenericTypeInfo<>(CellKpi.class))
            .setParallelism(kpiParallelism)
            .name("kpi-1m-full-join")
            .uid("kpi-1m-full-join")
            .process(windowMaterializationProbe(
                "window-kpi-1m", "KPI 1m Materialization", "kpi-1m", "MIN_1",
                CellKpi::getWindowEndTs, CellKpi::getSourceEventTsAvg, metricConfig))
            .setParallelism(kpiParallelism)
            .name("window-kpi-1m-materialization")
            .process(stageMetricsProbe("kpi-1m", "KPI 1m Full Join", "healthy", resultSinkConfig, metricConfig,
                CellKpi::getSourceEventTsAvg))
            .setParallelism(kpiParallelism)
            .name("kpi-1m-metrics");
        DataStream<AnomalyEvent> cellAnomalies = CellKpiCepAnomalyDetector
            .detect(cellKpi1m, rules)
            .process(stageMetricsProbe("cell-anomaly", "Cell Anomaly Detection", "healthy",
                resultSinkConfig, metricConfig, FlinkJobMain::anomalyEventLatencyTimestamp))
            .setParallelism(kpiParallelism)
            .name("cell-anomaly-metrics");

        DataStream<CellKpi> cellKpi5m = cellKpi1m
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<CellKpi>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                    .withIdleness(Duration.ofMinutes(1))
                    .withTimestampAssigner((kpi, ts) -> Math.subtractExact(kpi.getWindowEndTs(), 1L)))
            .keyBy(kpi -> kpi.getCellId().toString())
            .window(TumblingEventTimeWindows.of(Time.minutes(5)))
            .process(new CellKpiRollupAggregator(), new GenericTypeInfo<>(CellKpi.class))
            .setParallelism(kpiParallelism)
            .name("kpi-5m-rollup")
            .uid("kpi-5m-rollup")
            .process(stageMetricsProbe("kpi-5m", "KPI 5m Rollup", "healthy", resultSinkConfig, metricConfig,
                CellKpi::getSourceEventTsAvg))
            .setParallelism(kpiParallelism)
            .name("kpi-5m-metrics");

        ResultSinks.attachBusinessResultSinks(
            cellKpi1m, cellKpi5m, cellAnomalies, userAnomalies, coverageAnomalies,
            resultSinkConfig, bootstrap, icebergConfig, metricConfig);

        env.execute("fdb-flink-job");
    }

    private static DataStream<RoutedEnvelope> buildDynamicallyAssignedStream(
        StreamExecutionEnvironment env,
        DataStream<InputEnvelope> mergedInput,
        String bootstrap,
        String groupId,
        ResultSinkConfig resultSinkConfig,
        MetricRuntimeConfig metricConfig) {
        KafkaSource<String> routingSource = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrap).setTopics("lb-routing")
            .setGroupId(groupId + "-routing").setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .setProperties(resolveKafkaConsumerProperties(System.getenv(), System.getProperties()))
            .build();
        BroadcastStream<String> routingBroadcast = env.fromSource(routingSource,
            WatermarkStrategy.noWatermarks(), "lb-routing-source")
            .broadcast(RoutingAssigner.ROUTING_STATE);
        SingleOutputStreamOperator<RoutedEnvelope> metered = mergedInput.connect(routingBroadcast)
            .process(new RoutingAssigner(), new GenericTypeInfo<>(RoutedEnvelope.class)).name("routing-assigner")
            .keyBy(RoutedEnvelope::vbucketId)
            .process(new VBucketLoadMeter(), new GenericTypeInfo<>(RoutedEnvelope.class))
            .name("vbucket-load-meter");
        DataStream<RoutedEnvelope> assigned = metered
            .process(stageMetricsProbe("assigner", "VBucket Assigner", "healthy", resultSinkConfig, metricConfig,
                FlinkJobMain::routedEnvelopeLatencyTimestamp))
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
            .setProperties(resolveKafkaConsumerProperties(System.getenv(), System.getProperties()))
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
            .process(new LoadCoordinator(metricConfig), new GenericTypeInfo<>(RoutingEntry.class))
            .name("load-coordinator")
            .setParallelism(1)
            .process(stageMetricsProbe("load-coordinator", "Load Coordinator", "healthy",
                resultSinkConfig, metricConfig))
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

    static <T> StageMetricsProbe<T> stageMetricsProbe(String stageId, String displayName, String status,
                                                      ResultSinkConfig resultSinkConfig,
                                                      MetricRuntimeConfig metricConfig) {
        return stageMetricsProbe(stageId, displayName, status, resultSinkConfig, metricConfig, null);
    }

    static <T> StageMetricsProbe<T> stageMetricsProbe(String stageId, String displayName, String status,
                                                      ResultSinkConfig resultSinkConfig,
                                                      MetricRuntimeConfig metricConfig,
                                                      LatencyTimestampExtractor<T> latencyTimestampExtractor) {
        return new StageMetricsProbe<>(
            stageId, displayName, status, resultSinkConfig.metricsEmitIntervalMs(), metricConfig,
            latencyTimestampExtractor);
    }

    static RoutedEnvelope directRoute(InputEnvelope envelope) {
        return new RoutedEnvelope(envelope, Hashes.toVBucket(envelope.cellId(), DIRECT_ROUTE_VBUCKETS));
    }

    static long inputEnvelopeLatencyTimestamp(InputEnvelope envelope) {
        return envelope == null ? Long.MIN_VALUE : envelope.ts();
    }

    static long routedEnvelopeLatencyTimestamp(RoutedEnvelope routed) {
        return routed == null ? Long.MIN_VALUE : inputEnvelopeLatencyTimestamp(routed.envelope());
    }

    static long anomalyEventLatencyTimestamp(AnomalyEvent anomaly) {
        return anomaly == null || anomaly.getSourceEventTsAvg() <= 0L
            ? Long.MIN_VALUE
            : anomaly.getSourceEventTsAvg();
    }

    static boolean isCoverageCandidate(EnrichedChr enriched, RuleConfig rules) {
        if (enriched == null || enriched.chrEvent() == null || rules == null) {
            return false;
        }
        Float rsrp = enriched.chrEvent().getRsrp();
        return rsrp != null && rsrp < rules.rsrpThreshold();
    }

    static boolean resolveDynamicBalancingEnabled(Map<String, String> env, Properties properties) {
        String configured = env.get("FDB_DYNAMIC_BALANCING_ENABLED");
        if (configured == null || configured.isBlank()) {
            configured = properties.getProperty("fdb.dynamic.balancing.enabled");
        }
        return configured != null && "true".equalsIgnoreCase(configured.trim());
    }

    static boolean resolveDiagnosticChainingEnabled(Map<String, String> env, Properties properties) {
        String configured = env.get("FDB_FLINK_DIAGNOSTIC_CHAINING");
        if (configured == null || configured.isBlank()) {
            configured = properties.getProperty("fdb.flink.diagnostic.chaining");
        }
        return configured != null && "true".equalsIgnoreCase(configured.trim());
    }

    private static <T> SingleOutputStreamOperator<T> disableChainingIfDiagnostic(
        SingleOutputStreamOperator<T> operator,
        boolean diagnosticChainingEnabled) {
        if (diagnosticChainingEnabled) {
            return operator.disableChaining();
        }
        return operator;
    }

    private static <T> SingleOutputStreamOperator<T> splitChainIfDiagnostic(
        SingleOutputStreamOperator<T> operator,
        boolean diagnosticChainingEnabled) {
        if (diagnosticChainingEnabled) {
            return operator.startNewChain().disableChaining();
        }
        return operator;
    }

    static Properties resolveKafkaConsumerProperties(Map<String, String> env, Properties properties) {
        Properties consumerProperties = new Properties();
        putPositiveIntegerKafkaProperty(consumerProperties, "fetch.max.bytes",
            configuredValue(env, properties, "FDB_KAFKA_FETCH_MAX_BYTES", "fdb.kafka.fetch.max.bytes"));
        putPositiveIntegerKafkaProperty(consumerProperties, "max.partition.fetch.bytes",
            configuredValue(env, properties, "FDB_KAFKA_MAX_PARTITION_FETCH_BYTES",
                "fdb.kafka.max.partition.fetch.bytes"));
        putPositiveIntegerKafkaProperty(consumerProperties, "max.poll.records",
            configuredValue(env, properties, "FDB_KAFKA_MAX_POLL_RECORDS", "fdb.kafka.max.poll.records"));
        return consumerProperties;
    }

    private static String configuredValue(Map<String, String> env, Properties properties,
                                          String envKey, String propertyKey) {
        String configured = env.get(envKey);
        if (configured == null || configured.isBlank()) {
            configured = properties.getProperty(propertyKey);
        }
        return configured;
    }

    private static void putPositiveIntegerKafkaProperty(Properties target, String kafkaKey, String configured) {
        if (configured == null || configured.isBlank()) {
            return;
        }
        try {
            int value = Integer.parseInt(configured.trim());
            if (value > 0) {
                target.setProperty(kafkaKey, Integer.toString(value));
            } else {
                log.warn("Invalid Kafka consumer property {}='{}', ignoring", kafkaKey, configured);
            }
        } catch (NumberFormatException e) {
            log.warn("Invalid Kafka consumer property {}='{}', ignoring", kafkaKey, configured);
        }
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

    static int resolveStageParallelism(Map<String, String> env, Properties properties,
                                       String envKey, String propertyKey, int fallbackParallelism) {
        String configured = env.get(envKey);
        if (configured == null || configured.isBlank()) {
            configured = properties.getProperty(propertyKey);
        }
        if (configured == null || configured.isBlank()) {
            return fallbackParallelism;
        }
        try {
            int parallelism = Integer.parseInt(configured.trim());
            return parallelism > 0 ? parallelism : fallbackParallelism;
        } catch (NumberFormatException e) {
            log.warn("Invalid Flink stage parallelism {}='{}', falling back to {}", envKey, configured,
                fallbackParallelism);
            return fallbackParallelism;
        }
    }

    static long resolveCheckpointIntervalMs(Map<String, String> env, Properties properties) {
        String configured = env.get("FDB_FLINK_CHECKPOINT_INTERVAL_MS");
        if (configured == null || configured.isBlank()) {
            configured = properties.getProperty("fdb.flink.checkpoint.interval.ms");
        }
        if (configured == null || configured.isBlank()) {
            return 30_000L;
        }
        try {
            long intervalMs = Long.parseLong(configured.trim());
            return intervalMs > 0 ? intervalMs : 30_000L;
        } catch (NumberFormatException e) {
            log.warn("Invalid Flink checkpoint interval '{}', falling back to 30000 ms", configured);
            return 30_000L;
        }
    }

    static Duration resolveChrWatermarkOutOfOrderness(Map<String, String> env, Properties properties) {
        return resolvePositiveDuration(env, properties,
            "FDB_CHR_WATERMARK_OUT_OF_ORDER_MS", "fdb.chr.watermark.out.of.order.ms", 2_000L);
    }

    static Duration resolvePmWatermarkOutOfOrderness(Map<String, String> env, Properties properties) {
        return resolvePositiveDuration(env, properties,
            "FDB_PM_WATERMARK_OUT_OF_ORDER_MS", "fdb.pm.watermark.out.of.order.ms", 2_000L);
    }

    static Duration resolveKpiJoinWait(Map<String, String> env, Properties properties) {
        return resolvePositiveDuration(env, properties,
            "FDB_KPI_JOIN_WAIT_MS", "fdb.kpi.join.wait.ms", 10_000L);
    }

    private static Duration resolvePositiveDuration(Map<String, String> env, Properties properties,
                                                    String envKey, String propertyKey, long defaultMs) {
        String configured = env.get(envKey);
        if (configured == null || configured.isBlank()) {
            configured = properties.getProperty(propertyKey);
        }
        if (configured == null || configured.isBlank()) {
            return Duration.ofMillis(defaultMs);
        }
        try {
            long millis = Long.parseLong(configured.trim());
            return millis > 0 ? Duration.ofMillis(millis) : Duration.ofMillis(defaultMs);
        } catch (NumberFormatException e) {
            log.warn("Invalid duration {}='{}', falling back to {} ms", envKey, configured, defaultMs);
            return Duration.ofMillis(defaultMs);
        }
    }

    static long effectiveCheckpointIntervalMs(ResultSinkType resultSink, long configuredIntervalMs) {
        return ResultSinkConfig.effectiveCheckpointIntervalMs(resultSink, configuredIntervalMs);
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

    static <T> WindowMaterializationProbe<T> windowMaterializationProbe(
        String stageId,
        String displayName,
        String dataset,
        String windowKind,
        WindowMaterializationProbe.WindowEndTimestampExtractor<T> windowEndTimestampExtractor,
        LatencyTimestampExtractor<T> latencyTimestampExtractor,
        MetricRuntimeConfig metricConfig) {
        return new WindowMaterializationProbe<>(
            stageId, displayName, dataset, windowKind, windowEndTimestampExtractor, latencyTimestampExtractor,
            metricConfig);
    }
}
