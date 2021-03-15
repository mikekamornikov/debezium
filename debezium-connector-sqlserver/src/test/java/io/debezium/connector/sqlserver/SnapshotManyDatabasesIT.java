/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.fest.assertions.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.util.MultiDatabaseTestHelper;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.embedded.AbstractConnectorTest;

public class SnapshotManyDatabasesIT extends AbstractConnectorTest {

    private static final int RECORDS_PER_TABLE = 10;

    private SqlServerConnection connection;

    @Before
    public void before() throws SQLException {
        MultiDatabaseTestHelper.createTestDatabase();
        connection = MultiDatabaseTestHelper.testConnection();
        for (String databaseName : MultiDatabaseTestHelper.TEST_REAL_DATABASES.split(",")) {
            connection.execute("USE " + databaseName);
            connection.execute(
                    "CREATE TABLE table1 (id int, name varchar(30), price decimal(8,2), ts datetime2(0), primary key(id))");

            // Populate database
            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                connection.execute(
                        String.format("INSERT INTO table1 VALUES(%s, '%s', %s, '%s')", i, "name" + i, new BigDecimal(i + ".23"), "2018-07-18 13:28:56"));
            }

            MultiDatabaseTestHelper.enableTableCdc(connection, databaseName, "table1");
        }

        initializeConnectorTestFramework();
        Files.delete(TestHelper.DB_HISTORY_PATH);
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void takeSnapshot() throws Exception {
        // It's not possible now to make snapshots for 2 databases. The 1st snapshot switches to streaming mode
        // blocking the 2nd snapshot
        final Configuration config = MultiDatabaseTestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SqlServerConnectorConfig.SnapshotMode.INITIAL_ONLY)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        String[] databases = MultiDatabaseTestHelper.TEST_REAL_DATABASES.split(",");

        SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * databases.length);
        for (String databaseName : databases) {
            final List<SourceRecord> table1 = records.recordsForTopic(String.format("server1.%s.table1", databaseName));
            table1.subList(0, RECORDS_PER_TABLE - 1).forEach(record -> {
                assertThat(((Struct) record.value()).getStruct("source").getString("snapshot")).isEqualTo("true");
            });
            assertThat(((Struct) table1.get(RECORDS_PER_TABLE - 1).value()).getStruct("source").getString("snapshot")).isEqualTo("last");
        }

        stopConnector();

        // offsets should exist here, so connector will try to recreate internal schema
        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        records = consumeRecordsByTopic(RECORDS_PER_TABLE * databases.length * 2);
        assertThat(records.allRecordsInOrder().size() == RECORDS_PER_TABLE * databases.length);
    }
}
