package com.fdb.job.maintenance;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IcebergRetentionToolTest {

    @Test
    void parses_required_options() {
        IcebergRetentionTool.Options options = IcebergRetentionTool.Options.parse(
            "--warehouse", "hdfs://namenode:8020/warehouse/iceberg",
            "--database", "fdb",
            "--table", "cell_kpi",
            "--older-than-ms", "3600000",
            "--max-bytes", "10737418240",
            "--orphan-delete-mode", "manual-safe",
            "--allow-manual-orphan-delete", "true");

        assertThat(options.warehouse()).isEqualTo("hdfs://namenode:8020/warehouse/iceberg");
        assertThat(options.database()).isEqualTo("fdb");
        assertThat(options.table()).isEqualTo("cell_kpi");
        assertThat(options.olderThanMs()).isEqualTo(3_600_000L);
        assertThat(options.maxBytes()).isEqualTo(10_737_418_240L);
        assertThat(options.orphanDeleteMode()).isEqualTo(IcebergRetentionTool.OrphanDeleteMode.MANUAL_SAFE);
        assertThat(options.allowManualOrphanDelete()).isTrue();
    }

    @Test
    void defaults_orphan_delete_mode_to_skip() {
        IcebergRetentionTool.Options options = IcebergRetentionTool.Options.parse(
            "--warehouse", "hdfs://namenode:8020/warehouse/iceberg",
            "--database", "fdb",
            "--table", "cell_kpi",
            "--older-than-ms", "3600000",
            "--max-bytes", "10737418240");

        assertThat(options.orphanDeleteMode()).isEqualTo(IcebergRetentionTool.OrphanDeleteMode.SKIP);
        assertThat(options.allowManualOrphanDelete()).isFalse();
    }

    @Test
    void rejects_unknown_orphan_delete_mode() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> IcebergRetentionTool.Options.parse(
                "--warehouse", "hdfs://namenode:8020/warehouse/iceberg",
                "--database", "fdb",
                "--table", "cell_kpi",
                "--older-than-ms", "3600000",
                "--max-bytes", "10737418240",
                "--orphan-delete-mode", "unsafe"))
            .withMessageContaining("--orphan-delete-mode");
    }

    @Test
    void rejects_duplicate_orphan_delete_mode_option() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> IcebergRetentionTool.Options.parse(
                "--warehouse", "hdfs://namenode:8020/warehouse/iceberg",
                "--database", "fdb",
                "--table", "cell_kpi",
                "--older-than-ms", "3600000",
                "--max-bytes", "10737418240",
                "--orphan-delete-mode", "skip",
                "--orphan-delete-mode", "manual-safe"))
            .withMessageContaining("Duplicate option --orphan-delete-mode");
    }

    @Test
    void rejects_missing_required_option() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> IcebergRetentionTool.Options.parse(
                "--warehouse", "hdfs://namenode:8020/warehouse/iceberg",
                "--database", "fdb",
                "--table", "cell_kpi",
                "--older-than-ms", "3600000"))
            .withMessageContaining("--max-bytes");
    }

    @Test
    void rejects_non_positive_retention_window() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> IcebergRetentionTool.Options.parse(
                "--warehouse", "hdfs://namenode:8020/warehouse/iceberg",
                "--database", "fdb",
                "--table", "cell_kpi",
                "--older-than-ms", "0",
                "--max-bytes", "10737418240"))
            .withMessageContaining("--older-than-ms must be positive");
    }

    @Test
    void rejects_duplicate_option() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> IcebergRetentionTool.Options.parse(
                "--warehouse", "hdfs://namenode:8020/warehouse/iceberg",
                "--warehouse", "hdfs://namenode:8020/other",
                "--database", "fdb",
                "--table", "cell_kpi",
                "--older-than-ms", "3600000",
                "--max-bytes", "10737418240"))
            .withMessageContaining("Duplicate option --warehouse");
    }

    @Test
    void rejects_invalid_max_bytes() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> IcebergRetentionTool.Options.parse(
                "--warehouse", "hdfs://namenode:8020/warehouse/iceberg",
                "--database", "fdb",
                "--table", "cell_kpi",
                "--older-than-ms", "3600000",
                "--max-bytes", "not-a-number"))
            .withMessageContaining("--max-bytes must be a positive integer");
    }

    @Test
    void computes_expiration_cutoff_from_current_time() {
        assertThat(IcebergRetentionTool.expirationTimestampMillis(1_700_000_000_000L, 3_600_000L))
            .isEqualTo(1_699_996_400_000L);
    }

    @Test
    void accepts_data_bytes_at_or_under_limit() {
        IcebergRetentionTool.verifyWithinMaxBytes(10_737_418_240L, 10_737_418_240L, "fdb.cell_kpi");
        IcebergRetentionTool.verifyWithinMaxBytes(42L, 10_737_418_240L, "fdb.cell_kpi");
    }

    @Test
    void rejects_data_bytes_above_limit_with_table_context() {
        assertThatThrownBy(() ->
            IcebergRetentionTool.verifyWithinMaxBytes(10_737_418_241L, 10_737_418_240L, "fdb.cell_kpi"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("fdb.cell_kpi")
            .hasMessageContaining("10737418241")
            .hasMessageContaining("10737418240");
    }

    @Test
    void data_byte_addition_overflow_has_context() {
        assertThatThrownBy(() -> IcebergRetentionTool.addDataBytes(Long.MAX_VALUE, 1L, "fdb.cell_kpi"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("fdb.cell_kpi")
            .hasMessageContaining("overflow")
            .hasMessageContaining("Long.MAX_VALUE");
    }

    @Test
    void rejects_manual_orphan_delete_when_retention_window_is_under_one_hour() {
        IcebergRetentionTool.Options options = IcebergRetentionTool.Options.parse(
            "--warehouse", "hdfs://namenode:8020/warehouse/iceberg",
            "--database", "fdb",
            "--table", "cell_kpi",
            "--older-than-ms", "3599999",
            "--max-bytes", "10737418240",
            "--orphan-delete-mode", "manual-safe",
            "--allow-manual-orphan-delete", "true");

        assertThatIllegalArgumentException()
            .isThrownBy(() -> IcebergRetentionTool.validateManualOrphanDeleteGuard(options, "fdb.cell_kpi"))
            .withMessageContaining("at least 3600000");
    }

    @Test
    void rejects_manual_orphan_delete_without_explicit_allow_flag() {
        IcebergRetentionTool.Options options = IcebergRetentionTool.Options.parse(
            "--warehouse", "hdfs://namenode:8020/warehouse/iceberg",
            "--database", "fdb",
            "--table", "cell_kpi",
            "--older-than-ms", "3600000",
            "--max-bytes", "10737418240",
            "--orphan-delete-mode", "manual-safe");

        assertThatIllegalArgumentException()
            .isThrownBy(() -> IcebergRetentionTool.validateManualOrphanDeleteGuard(options, "fdb.cell_kpi"))
            .withMessageContaining("--allow-manual-orphan-delete true");
    }

    @Test
    void manual_safe_candidate_requires_table_data_location_age_extension_and_no_reference() {
        Set<String> referenced = Set.of("hdfs://nn:8020/warehouse/iceberg/fdb/cell_kpi/data/dt=2026-07-04/live.parquet");
        long cutoff = 1_000_000L;

        assertThat(IcebergRetentionTool.isManualSafeOrphanCandidate(
            "hdfs://nn:8020/warehouse/iceberg/fdb/cell_kpi",
            "hdfs://nn:8020/warehouse/iceberg/fdb/cell_kpi/data/dt=2026-07-04/orphan.parquet",
            999_999L,
            cutoff,
            referenced)).isTrue();

        assertThat(IcebergRetentionTool.isManualSafeOrphanCandidate(
            "hdfs://nn:8020/warehouse/iceberg/fdb/cell_kpi",
            "hdfs://nn:8020/warehouse/iceberg/fdb/cell_kpi/data/dt=2026-07-04/live.parquet",
            999_999L,
            cutoff,
            referenced)).isFalse();
        assertThat(IcebergRetentionTool.isManualSafeOrphanCandidate(
            "hdfs://nn:8020/warehouse/iceberg/fdb/cell_kpi",
            "hdfs://nn:8020/warehouse/iceberg/fdb/cell_kpi/metadata/old.parquet",
            999_999L,
            cutoff,
            referenced)).isFalse();
        assertThat(IcebergRetentionTool.isManualSafeOrphanCandidate(
            "hdfs://nn:8020/warehouse/iceberg/fdb/cell_kpi",
            "hdfs://nn:8020/warehouse/iceberg/fdb/cell_kpi/data/dt=2026-07-04/new.parquet",
            1_000_000L,
            cutoff,
            referenced)).isFalse();
        assertThat(IcebergRetentionTool.isManualSafeOrphanCandidate(
            "hdfs://nn:8020/warehouse/iceberg/fdb/cell_kpi",
            "hdfs://nn:8020/warehouse/iceberg/fdb/cell_kpi/data/dt=2026-07-04/file.tmp",
            999_999L,
            cutoff,
            referenced)).isFalse();
        assertThat(IcebergRetentionTool.isManualSafeOrphanCandidate(
            "hdfs://nn:8020/warehouse/iceberg/fdb/cell_kpi",
            "hdfs://nn:8020/warehouse/iceberg/fdb/cell_kpi/data/.tmp/file.parquet",
            999_999L,
            cutoff,
            referenced)).isFalse();
    }
}
