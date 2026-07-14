package com.fdb.job.maintenance;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public final class IcebergRetentionTool {

    static final long MIN_MANUAL_ORPHAN_DELETE_AGE_MS = 3_600_000L;

    private IcebergRetentionTool() {}

    public static void main(String[] args) throws Exception {
        run(Options.parse(args), Clock.systemUTC());
    }

    static void run(Options options, Clock clock) throws IOException {
        long cutoffMillis = expirationTimestampMillis(clock.millis(), options.olderThanMs());
        TableIdentifier identifier = TableIdentifier.of(options.database(), options.table());

        Configuration hadoopConf = new Configuration();
        HadoopCatalog catalog = new HadoopCatalog();
        catalog.setConf(hadoopConf);
        catalog.initialize("fdb_retention", Map.of("warehouse", options.warehouse()));
        try {
            Table table = catalog.loadTable(identifier);
            System.out.printf("[retention] Iceberg table | %s | location=%s%n", identifier, table.location());
            table.expireSnapshots()
                .expireOlderThan(cutoffMillis)
                .retainLast(1)
                .cleanExpiredFiles(true)
                .commit();
            table.refresh();

            OrphanDeleteResult orphanDeleteResult = deleteOrphanFiles(
                options, table, hadoopConf, cutoffMillis, identifier.toString());
            long approximateBytes = approximateDataBytes(table, identifier.toString());
            System.out.printf("[retention] Iceberg table | %s | approximate_data_bytes=%d | orphan_delete_mode=%s | orphan_candidates=%d | deleted_orphan_files=%d%n",
                identifier, approximateBytes, options.orphanDeleteMode().cliName(),
                orphanDeleteResult.candidateFiles(), orphanDeleteResult.deletedFiles());
            verifyWithinMaxBytes(approximateBytes, options.maxBytes(), identifier.toString());
        } finally {
            catalog.close();
        }
    }

    static long expirationTimestampMillis(long nowMillis, long olderThanMs) {
        if (olderThanMs <= 0) {
            throw new IllegalArgumentException("--older-than-ms must be positive");
        }
        return nowMillis - olderThanMs;
    }

    static long approximateDataBytes(Table table) {
        return approximateDataBytes(table, "unknown");
    }

    static long approximateDataBytes(Table table, String tableName) {
        long bytes = 0L;
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                bytes = addDataBytes(bytes, task.sizeBytes(), tableName);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to scan Iceberg table files", e);
        }
        return bytes;
    }

    static OrphanDeleteResult deleteOrphanFiles(
            Options options,
            Table table,
            Configuration hadoopConf,
            long cutoffMillis,
            String tableName) throws IOException {
        return switch (options.orphanDeleteMode()) {
            case SKIP -> new OrphanDeleteResult(0, 0);
            case ICEBERG_ACTION -> throw new UnsupportedOperationException(
                "--orphan-delete-mode iceberg-action is not available in iceberg-flink-runtime-1.20:1.11.0; "
                    + "org.apache.iceberg.flink.actions.Actions exposes rewriteDataFiles only");
            case MANUAL_SAFE -> deleteManualSafeOrphans(options, table, hadoopConf, cutoffMillis, tableName);
        };
    }

    static OrphanDeleteResult deleteManualSafeOrphans(
            Options options,
            Table table,
            Configuration hadoopConf,
            long cutoffMillis,
            String tableName) throws IOException {
        validateManualOrphanDeleteGuard(options, tableName);
        System.out.printf("[retention] Iceberg table | %s | manual-safe orphan delete enabled; "
                + "risk=physical-delete opt-in=true cutoff_ms=%d guards=data-location,referenced-files,completed-file-extensions%n",
            tableName, cutoffMillis);

        Set<String> referencedLocations = retainedSnapshotFileReferences(table, tableName);
        String dataLocation = appendPath(table.location(), "data");
        Path dataPath = new Path(dataLocation);
        FileSystem fs = dataPath.getFileSystem(hadoopConf);
        if (!fs.exists(dataPath)) {
            return new OrphanDeleteResult(0, 0);
        }

        return deleteManualSafeOrphansInDirectory(fs, dataPath, table.location(), cutoffMillis, referencedLocations);
    }

    private static OrphanDeleteResult deleteManualSafeOrphansInDirectory(
            FileSystem fs,
            Path directory,
            String tableLocation,
            long cutoffMillis,
            Set<String> referencedLocations) throws IOException {
        int candidateFiles = 0;
        int deletedFiles = 0;
        RemoteIterator<FileStatus> files = fs.listStatusIterator(directory);
        while (files.hasNext()) {
            FileStatus status = files.next();
            if (status.isDirectory()) {
                OrphanDeleteResult nested = deleteManualSafeOrphansInDirectory(
                    fs, status.getPath(), tableLocation, cutoffMillis, referencedLocations);
                candidateFiles += nested.candidateFiles();
                deletedFiles += nested.deletedFiles();
            } else if (isManualSafeOrphanCandidate(
                    tableLocation, status.getPath().toString(), status.getModificationTime(), cutoffMillis, referencedLocations)) {
                candidateFiles++;
                if (fs.delete(status.getPath(), false)) {
                    deletedFiles++;
                }
            }
        }
        return new OrphanDeleteResult(candidateFiles, deletedFiles);
    }

    static Set<String> retainedSnapshotFileReferences(Table table, String tableName) {
        Set<String> referencedLocations = new HashSet<>();
        try {
            for (Snapshot snapshot : table.snapshots()) {
                for (ManifestFile manifest : snapshot.allManifests(table.io())) {
                    addManifestReferences(table, manifest, referencedLocations);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to scan retained Iceberg snapshot file references for " + tableName, e);
        }
        return referencedLocations;
    }

    private static void addManifestReferences(Table table, ManifestFile manifest, Set<String> referencedLocations)
            throws IOException {
        if (manifest.content() == ManifestContent.DATA) {
            try (CloseableIterable<String> paths = ManifestFiles.readPaths(manifest, table.io(), table.specs())) {
                for (String path : paths) {
                    referencedLocations.add(normalizeLocation(path));
                }
            }
        } else if (manifest.content() == ManifestContent.DELETES) {
            try (CloseableIterable<DeleteFile> deleteFiles =
                     ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
                for (DeleteFile deleteFile : deleteFiles) {
                    referencedLocations.add(normalizeLocation(deleteFile.path().toString()));
                }
            }
        }
    }

    static void validateManualOrphanDeleteGuard(Options options, String tableName) {
        if (options.olderThanMs() < MIN_MANUAL_ORPHAN_DELETE_AGE_MS) {
            throw new IllegalArgumentException("Manual orphan delete for " + tableName
                + " requires --older-than-ms to be at least " + MIN_MANUAL_ORPHAN_DELETE_AGE_MS);
        }
        if (!options.allowManualOrphanDelete()) {
            throw new IllegalArgumentException("Manual orphan delete for " + tableName
                + " requires --allow-manual-orphan-delete true");
        }
    }

    static boolean isManualSafeOrphanCandidate(
            String tableLocation,
            String fileLocation,
            long modificationTimeMillis,
            long cutoffMillis,
            Set<String> referencedLocations) {
        String normalizedFile = normalizeLocation(fileLocation);
        if (!isUnderDataLocation(tableLocation, normalizedFile)) {
            return false;
        }
        if (modificationTimeMillis >= cutoffMillis) {
            return false;
        }
        if (referencedLocations.contains(normalizedFile)) {
            return false;
        }
        if (!hasCompletedDataFileExtension(normalizedFile)) {
            return false;
        }
        return !hasUnsafePathSegment(normalizedFile);
    }

    private static boolean isUnderDataLocation(String tableLocation, String normalizedFile) {
        String dataLocation = normalizeLocation(appendPath(tableLocation, "data"));
        return normalizedFile.startsWith(dataLocation + "/");
    }

    private static boolean hasCompletedDataFileExtension(String normalizedFile) {
        String lower = normalizedFile.toLowerCase(Locale.ROOT);
        return lower.endsWith(".parquet") || lower.endsWith(".avro") || lower.endsWith(".orc");
    }

    private static boolean hasUnsafePathSegment(String normalizedFile) {
        String lower = normalizedFile.toLowerCase(Locale.ROOT);
        if (lower.contains(".tmp") || lower.contains("_temporary")
                || lower.contains("staging") || lower.contains(".inprogress")) {
            return true;
        }
        String[] segments = lower.split("/");
        for (String segment : segments) {
            if (segment.startsWith(".") || segment.startsWith("_")) {
                return true;
            }
        }
        return false;
    }

    private static String appendPath(String location, String child) {
        return normalizeLocation(location) + "/" + child;
    }

    private static String normalizeLocation(String location) {
        String normalized = location.trim().replace('\\', '/');
        while (normalized.endsWith("/") && normalized.length() > 1) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    static long addDataBytes(long currentBytes, long nextBytes, String tableName) {
        try {
            return Math.addExact(currentBytes, nextBytes);
        } catch (ArithmeticException e) {
            throw new IllegalStateException("Iceberg table " + tableName
                + " approximate_data_bytes overflow while adding data file sizes: current_bytes="
                + formatLong(currentBytes) + ", next_bytes=" + formatLong(nextBytes), e);
        }
    }

    private static String formatLong(long value) {
        if (value == Long.MAX_VALUE) {
            return "Long.MAX_VALUE";
        }
        if (value == Long.MIN_VALUE) {
            return "Long.MIN_VALUE";
        }
        return Long.toString(value);
    }

    static void verifyWithinMaxBytes(long approximateBytes, long maxBytes, String tableName) {
        if (approximateBytes > maxBytes) {
            throw new IllegalStateException("Iceberg table " + tableName
                + " remains above retention byte budget: approximate_data_bytes="
                + approximateBytes + ", max_bytes=" + maxBytes);
        }
    }

    enum OrphanDeleteMode {
        SKIP("skip"),
        ICEBERG_ACTION("iceberg-action"),
        MANUAL_SAFE("manual-safe");

        private final String cliName;

        OrphanDeleteMode(String cliName) {
            this.cliName = cliName;
        }

        String cliName() {
            return cliName;
        }

        static OrphanDeleteMode parse(String value) {
            for (OrphanDeleteMode mode : values()) {
                if (mode.cliName.equals(value)) {
                    return mode;
                }
            }
            throw new IllegalArgumentException("--orphan-delete-mode must be one of skip, iceberg-action, manual-safe");
        }
    }

    record OrphanDeleteResult(int candidateFiles, int deletedFiles) {}

    public record Options(
            String warehouse,
            String database,
            String table,
            long olderThanMs,
            long maxBytes,
            OrphanDeleteMode orphanDeleteMode,
            boolean allowManualOrphanDelete) {

        public static Options parse(String... args) {
            Map<String, String> values = parseArgs(args);
            String warehouse = required(values, "--warehouse");
            String database = required(values, "--database");
            String table = required(values, "--table");
            long olderThanMs = positiveLong(required(values, "--older-than-ms"), "--older-than-ms");
            long maxBytes = positiveLong(required(values, "--max-bytes"), "--max-bytes");
            OrphanDeleteMode orphanDeleteMode = OrphanDeleteMode.parse(
                values.getOrDefault("--orphan-delete-mode", OrphanDeleteMode.SKIP.cliName()));
            boolean allowManualOrphanDelete = booleanOption(
                values.getOrDefault("--allow-manual-orphan-delete", "false"),
                "--allow-manual-orphan-delete");
            return new Options(warehouse, database, table, olderThanMs, maxBytes,
                orphanDeleteMode, allowManualOrphanDelete);
        }

        private static Map<String, String> parseArgs(String[] args) {
            Map<String, String> values = new HashMap<>();
            for (int i = 0; i < args.length; i += 2) {
                String name = args[i];
                if (!isSupportedOption(name)) {
                    throw new IllegalArgumentException("Unsupported option: " + name);
                }
                if (i + 1 >= args.length || args[i + 1].startsWith("--")) {
                    throw new IllegalArgumentException("Missing value for " + name);
                }
                if (values.put(name, args[i + 1]) != null) {
                    throw new IllegalArgumentException("Duplicate option " + name);
                }
            }
            return values;
        }

        private static boolean isSupportedOption(String name) {
            return "--warehouse".equals(name)
                || "--database".equals(name)
                || "--table".equals(name)
                || "--older-than-ms".equals(name)
                || "--max-bytes".equals(name)
                || "--orphan-delete-mode".equals(name)
                || "--allow-manual-orphan-delete".equals(name);
        }

        private static String required(Map<String, String> values, String name) {
            String value = values.get(name);
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("Missing required option " + name);
            }
            return value.trim();
        }

        private static long positiveLong(String value, String name) {
            try {
                long parsed = Long.parseLong(value);
                if (parsed <= 0) {
                    throw new IllegalArgumentException(name + " must be positive");
                }
                return parsed;
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(name + " must be a positive integer", e);
            }
        }

        private static boolean booleanOption(String value, String name) {
            if ("true".equalsIgnoreCase(value)) {
                return true;
            }
            if ("false".equalsIgnoreCase(value)) {
                return false;
            }
            throw new IllegalArgumentException(name + " must be true or false");
        }
    }
}
