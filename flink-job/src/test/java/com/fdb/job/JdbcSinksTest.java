package com.fdb.job;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.junit.jupiter.api.Test;

class JdbcSinksTest {

    @Test
    void defaultsToSharedStarRocksEndpoint() throws Exception {
        assertThat(invokePrivateString("jdbcUrl"))
            .isEqualTo("jdbc:mysql://starrocks-fe:9030/fdb?rewriteBatchedStatements=true&useServerPrepStmts=false");
        assertThat(invokePrivateString("jdbcUser")).isEqualTo("root");
        assertThat(invokePrivateString("jdbcPassword")).isEmpty();
    }

    @Test
    void appendsMysqlBatchRewriteParameters() throws Exception {
        assertThat(invokePrivateString("appendJdbcBatchParameters", "jdbc:mysql://host:9030/fdb"))
            .isEqualTo("jdbc:mysql://host:9030/fdb?rewriteBatchedStatements=true&useServerPrepStmts=false");
        assertThat(invokePrivateString("appendJdbcBatchParameters", "jdbc:mysql://host:9030/fdb?connectTimeout=5000"))
            .isEqualTo("jdbc:mysql://host:9030/fdb?connectTimeout=5000&rewriteBatchedStatements=true&useServerPrepStmts=false");
        assertThat(invokePrivateString("appendJdbcBatchParameters", "jdbc:mysql://host:9030/fdb?rewriteBatchedStatements=true"))
            .isEqualTo("jdbc:mysql://host:9030/fdb?rewriteBatchedStatements=true&useServerPrepStmts=false");
    }

    @Test
    void defaultsToConservativeStarRocksBatching() throws Exception {
        JdbcExecutionOptions options = invokePrivateExecutionOptions("execOpts");

        assertThat(options.getBatchSize()).isEqualTo(100_000);
        assertThat(options.getBatchIntervalMs()).isEqualTo(60_000L);
        assertThat(options.getMaxRetries()).isEqualTo(1);
    }

    private static String invokePrivateString(String methodName) throws Exception {
        Method method = JdbcSinks.class.getDeclaredMethod(methodName);
        method.setAccessible(true);
        return (String) method.invoke(null);
    }

    private static String invokePrivateString(String methodName, String value) throws Exception {
        Method method = JdbcSinks.class.getDeclaredMethod(methodName, String.class);
        method.setAccessible(true);
        return (String) method.invoke(null, value);
    }

    private static JdbcExecutionOptions invokePrivateExecutionOptions(String methodName) throws Exception {
        Method method = JdbcSinks.class.getDeclaredMethod(methodName);
        method.setAccessible(true);
        return (JdbcExecutionOptions) method.invoke(null);
    }
}
