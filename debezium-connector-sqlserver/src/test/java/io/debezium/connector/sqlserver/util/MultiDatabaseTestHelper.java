/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.sqlserver.util;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SourceTimestampMode;
import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.connector.sqlserver.SqlServerValueConverters;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.history.FileDatabaseHistory;
import io.debezium.util.Clock;
import io.debezium.util.IoUtil;
import io.debezium.util.Strings;
import io.debezium.util.Testing;

public class MultiDatabaseTestHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiDatabaseTestHelper.class);

    public static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-connect.txt").toAbsolutePath();
    public static final String TEST_DATABASES = "testdb,testdb1";
    public static final String TEST_REAL_DATABASES = "testDB,testDB1";
    private static final String STATEMENTS_PLACEHOLDER = "#";
    private static final String DATABASE_NAME_PLACEHOLDER = "[#db]";

    private static final String ENABLE_DB_CDC = "IF EXISTS(select 1 from #.sys.databases where name='#' AND is_cdc_enabled=0)\n"
            + "EXEC #.sys.sp_cdc_enable_db";
    private static final String DISABLE_DB_CDC = "IF EXISTS(select 1 from #.sys.databases where name='#' AND is_cdc_enabled=1)\n"
            + "EXEC #.sys.sp_cdc_disable_db";
    private static final String ENABLE_TABLE_CDC = "IF EXISTS(select 1 from [#db].sys.tables where name = '#' AND is_tracked_by_cdc=0)\n"
            + "EXEC [#db].sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'#', @role_name = NULL, @supports_net_changes = 0";
    private static final String IS_CDC_ENABLED = "SELECT COUNT(1) FROM #.sys.databases WHERE name = '#' AND is_cdc_enabled=1";
    private static final String ENABLE_TABLE_CDC_WITH_CUSTOM_CAPTURE = "EXEC [#db].sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'%s', @capture_instance = N'%s', @role_name = NULL, @supports_net_changes = 0, @captured_column_list = %s";
    private static final String CDC_WRAPPERS_DML;

    static {
        try {
            ClassLoader classLoader = MultiDatabaseTestHelper.class.getClassLoader();
            CDC_WRAPPERS_DML = IoUtil.read(classLoader.getResourceAsStream("generate_per_db_cdc_wrappers.sql"));
        }
        catch (Exception e) {
            throw new RuntimeException("Cannot load SQL Server statements", e);
        }
    }

    public static JdbcConfiguration adminJdbcConfig() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                .withDefault(JdbcConfiguration.DATABASE, "master")
                .withDefault(JdbcConfiguration.HOSTNAME, "localhost")
                .withDefault(JdbcConfiguration.PORT, 1433)
                .withDefault(JdbcConfiguration.USER, "sa")
                .withDefault(JdbcConfiguration.PASSWORD, "Password!")
                .build();
    }

    public static JdbcConfiguration defaultJdbcConfig() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                .withDefault("dbnames", TEST_DATABASES)
                .withDefault(JdbcConfiguration.HOSTNAME, "localhost")
                .withDefault(JdbcConfiguration.PORT, 1433)
                .withDefault(JdbcConfiguration.USER, "sa")
                .withDefault(JdbcConfiguration.PASSWORD, "Password!")
                .build();
    }

    /**
     * Returns a default configuration suitable for most test cases. Can be amended/overridden in individual tests as
     * needed.
     */
    public static Configuration.Builder defaultConfig() {
        JdbcConfiguration jdbcConfiguration = defaultJdbcConfig();
        Configuration.Builder builder = Configuration.create();

        jdbcConfiguration.forEach(
                (field, value) -> builder.with(SqlServerConnectorConfig.DATABASE_CONFIG_PREFIX + field, value));

        return builder.with(RelationalDatabaseConnectorConfig.SERVER_NAME, "server1")
                .with(SqlServerConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                .with(RelationalDatabaseConnectorConfig.INCLUDE_SCHEMA_CHANGES, false);
    }

    public static void createTestDatabase() {
        // NOTE: you cannot enable CDC for the "master" db (the default one) so
        // all tests must use a separate database...
        try (SqlServerConnection connection = adminConnection()) {
            connection.connect();
            for (String databaseName : TEST_REAL_DATABASES.split(",")) {
                dropTestDatabase(connection, databaseName);
                String sql = String.format("CREATE DATABASE %s\n", databaseName);
                connection.execute(sql);
                connection.execute("USE " + databaseName);
                connection.execute(String.format("ALTER DATABASE %s SET ALLOW_SNAPSHOT_ISOLATION ON", databaseName));
                // NOTE: you cannot enable CDC on master
                enableDbCdc(connection, databaseName);
            }
        }
        catch (SQLException e) {
            LOGGER.error("Error while initiating test database", e);
            throw new IllegalStateException("Error while initiating test database", e);
        }
    }

    private static void dropTestDatabase(SqlServerConnection connection, String databaseName) throws SQLException {
        try {
            Awaitility.await("Disabling CDC").atMost(60, TimeUnit.SECONDS).until(() -> {
                try {
                    connection.execute("USE " + databaseName);
                }
                catch (SQLException e) {
                    // if the database doesn't yet exist, there is no need to disable CDC
                    return true;
                }
                try {
                    disableDbCdc(connection, databaseName);
                    return true;
                }
                catch (SQLException e) {
                    return false;
                }
            });
        }
        catch (ConditionTimeoutException e) {
            throw new IllegalArgumentException("Failed to disable CDC on " + databaseName, e);
        }

        connection.execute("USE master");

        try {
            Awaitility.await("Dropping database " + databaseName).atMost(60, TimeUnit.SECONDS).until(() -> {
                try {
                    String sql = "IF EXISTS(select 1 from sys.databases where name = '#') DROP DATABASE #"
                            .replace("#", databaseName);
                    connection.execute(sql);
                    return true;
                }
                catch (SQLException e) {
                    LOGGER.warn("DROP DATABASE {} failed (will be retried): {}", databaseName, e.getMessage());
                    try {
                        connection.execute(String.format("ALTER DATABASE %s SET SINGLE_USER WITH ROLLBACK IMMEDIATE;", databaseName));
                    }
                    catch (SQLException e2) {
                        LOGGER.error("Failed to rollback immediately", e2);
                    }
                    return false;
                }
            });
        }
        catch (ConditionTimeoutException e) {
            throw new IllegalStateException("Failed to drop test database", e);
        }
    }

    public static SqlServerConnection adminConnection() {
        return new SqlServerConnection(MultiDatabaseTestHelper.adminJdbcConfig(), Clock.system(), SourceTimestampMode.getDefaultMode(),
                new SqlServerValueConverters(JdbcValueConverters.DecimalMode.PRECISE, TemporalPrecisionMode.ADAPTIVE, null));
    }

    public static SqlServerConnection testConnection() {
        return new SqlServerConnection(MultiDatabaseTestHelper.defaultJdbcConfig(), Clock.system(), SourceTimestampMode.getDefaultMode(),
                new SqlServerValueConverters(JdbcValueConverters.DecimalMode.PRECISE, TemporalPrecisionMode.ADAPTIVE, null));
    }

    /**
     * Enables CDC for a given database, if not already enabled.
     *
     * @param databaseName
     *            the name of the DB, may not be {@code null}
     * @throws SQLException
     *             if anything unexpected fails
     */
    public static void enableDbCdc(SqlServerConnection connection, String databaseName) throws SQLException {
        try {
            Objects.requireNonNull(databaseName);
            connection.execute(ENABLE_DB_CDC.replace(STATEMENTS_PLACEHOLDER, databaseName));

            // make sure database has cdc-enabled before proceeding; throwing exception if it fails
            Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> {
                final String sql = IS_CDC_ENABLED.replace(STATEMENTS_PLACEHOLDER, databaseName);
                return connection.queryAndMap(sql, connection.singleResultMapper(rs -> rs.getLong(1), "")) == 1L;
            });
        }
        catch (SQLException e) {
            LOGGER.error("Failed to enable CDC on database " + databaseName);
            throw e;
        }
    }

    /**
     * Disables CDC for a given database, if not already disabled.
     *
     * @param datebaseName
     *            the name of the DB, may not be {@code null}
     * @throws SQLException
     *             if anything unexpected fails
     */
    protected static void disableDbCdc(SqlServerConnection connection, String datebaseName) throws SQLException {
        Objects.requireNonNull(datebaseName);
        connection.execute(DISABLE_DB_CDC.replace(STATEMENTS_PLACEHOLDER, datebaseName));
    }

    /**
     * Enables CDC for a table if not already enabled and generates the wrapper
     * functions for that table.
     *
     * @param connection
     *            sql connection
     * @param databaseName
     * @param tableName
     *            the name of the table, may not be {@code null}
     * @throws SQLException if anything unexpected fails
     */
    public static void enableTableCdc(SqlServerConnection connection, String databaseName, String tableName) throws SQLException {
        Objects.requireNonNull(tableName);
        String enableCdcForTableStmt = ENABLE_TABLE_CDC
                .replace(DATABASE_NAME_PLACEHOLDER, databaseName)
                .replace(STATEMENTS_PLACEHOLDER, tableName);
        String generateWrapperFunctionsStmts = CDC_WRAPPERS_DML
                .replace(DATABASE_NAME_PLACEHOLDER, databaseName)
                .replaceAll(STATEMENTS_PLACEHOLDER, tableName.replaceAll("\\$", "\\\\\\$"));
        connection.execute(enableCdcForTableStmt, generateWrapperFunctionsStmts);
    }

    /**
     * Enables CDC for a table with a custom capture name
     * functions for that table.
     *
     * @param connection
     *            sql connection
     * @param databaseName
     * @param tableName
     *            the name of the table, may not be {@code null}
     * @param captureName
     *            the name of the capture instance, may not be {@code null}
     *
     * @throws SQLException if anything unexpected fails
     */
    public static void enableTableCdc(SqlServerConnection connection, String databaseName, String tableName, String captureName) throws SQLException {
        Objects.requireNonNull(databaseName);
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(captureName);
        String tableEnabledStmt = ENABLE_TABLE_CDC_WITH_CUSTOM_CAPTURE.replace(DATABASE_NAME_PLACEHOLDER, databaseName);
        String enableCdcForTableStmt = String.format(tableEnabledStmt, tableName, captureName, "NULL");
        connection.execute(enableCdcForTableStmt);
    }

    /**
     * Enables CDC for a table with a custom capture name
     * functions for that table.
     *
     * @param connection
     *            sql connection
     * @param databaseName
     * @param tableName
     *            the name of the table, may not be {@code null}
     * @param captureName
     *            the name of the capture instance, may not be {@code null}
     * @param captureColumnList
     *            the source table columns that are to be included in the change table, may not be {@code null}
     * @throws SQLException if anything unexpected fails
     */
    public static void enableTableCdc(SqlServerConnection connection, String databaseName, String tableName, String captureName, List<String> captureColumnList)
            throws SQLException {
        Objects.requireNonNull(databaseName);
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(captureName);
        Objects.requireNonNull(captureColumnList);
        String captureColumnListParam = String.format("N'%s'", Strings.join(",", captureColumnList));
        String tableEnabledStmt = ENABLE_TABLE_CDC_WITH_CUSTOM_CAPTURE.replace(DATABASE_NAME_PLACEHOLDER, databaseName);
        String enableCdcForTableStmt = String.format(tableEnabledStmt, tableName, captureName, captureColumnListParam);
        connection.execute(enableCdcForTableStmt);
    }

    public static void waitForSnapshotToBeCompleted() {
        final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
        try {
            Awaitility.await("Snapshot not completed").atMost(Duration.ofSeconds(60)).until(() -> {
                try {
                    return (boolean) mbeanServer.getAttribute(getObjectName("snapshot", "server1"), "SnapshotCompleted");
                }
                catch (InstanceNotFoundException e) {
                    // Metrics has not started yet
                    return false;
                }
            });
        }
        catch (ConditionTimeoutException e) {
            throw new IllegalArgumentException("Snapshot did not complete", e);
        }
    }

    private static ObjectName getObjectName(String context, String serverName) throws MalformedObjectNameException {
        return new ObjectName("debezium.sql_server:type=connector-metrics,context=" + context + ",server=" + serverName);
    }
}
