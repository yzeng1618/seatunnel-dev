/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.commons.lang3.tuple.Pair;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerLoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcVerticaIT extends AbstractJdbcIT {

    // Static initialization block for system environment diagnostics
    static {
        System.out.println("========== JdbcVerticaIT System Environment Diagnosis ==========");
        System.out.println("Java Version: " + System.getProperty("java.version"));
        System.out.println(
                "Operating System: "
                        + System.getProperty("os.name")
                        + " "
                        + System.getProperty("os.version"));
        System.out.println(
                "Available Memory: " + (Runtime.getRuntime().maxMemory() / 1024 / 1024) + "MB");
        System.out.println("Docker Environment Variable: " + System.getenv("DOCKER_HOST"));
        System.out.println("User Home Directory: " + System.getProperty("user.home"));
        System.out.println("Current Working Directory: " + System.getProperty("user.dir"));

        // Check if in CI environment
        String ciEnv = System.getenv("CI");
        if (ciEnv != null) {
            System.out.println("CI Environment Detected: " + ciEnv);
            System.out.println("CI Build ID: " + System.getenv("BUILD_ID"));
            System.out.println("CI Build URL: " + System.getenv("BUILD_URL"));
        }
        System.out.println("===============================================");
    }

    private static final String VERTICA_IMAGE = "vertica/vertica-ce:latest";
    private static final String VERTICA_CONTAINER_HOST = "e2e_vertica";

    private static final String VERTICA_DATABASE = "VMart";
    private static final String VERTICA_SCHEMA = "public";
    private static final String VERTICA_SOURCE = "e2e_table_source";
    private static final String VERTICA_SINK = "e2e_table_sink";
    private static final String VERTICA_USERNAME = "DBADMIN";
    private static final String VERTICA_PASSWORD = "";
    private static final int VERTICA_PORT = 5433;
    private static final String VERTICA_URL = "jdbc:vertica://" + HOST + ":%s/%s";

    private static final String DRIVER_CLASS = "com.vertica.jdbc.Driver";

    private static final List<String> CONFIG_FILE =
            Arrays.asList("/jdbc_vertica_source_and_sink.conf");
    private static final String CREATE_SQL =
            "create table if not exists %s\n"
                    + "(\n"
                    + "   id int,\n"
                    + "   name varchar,\n"
                    + "   age int\n"
                    + ");";

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        containerEnv.put("TZ", "UTC");
        containerEnv.put("MALLOC_ARENA_MAX", "2");
        containerEnv.put("VERTICA_MEMDEBUG", "1");

        String jdbcUrl = String.format(VERTICA_URL, VERTICA_PORT, VERTICA_DATABASE);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(VERTICA_SCHEMA, VERTICA_SOURCE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(VERTICA_IMAGE)
                .networkAliases(VERTICA_CONTAINER_HOST)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(VERTICA_PORT)
                .localPort(VERTICA_PORT)
                .jdbcTemplate(VERTICA_URL)
                .jdbcUrl(jdbcUrl)
                .userName(VERTICA_USERNAME)
                .password(VERTICA_PASSWORD)
                .database(VERTICA_SCHEMA)
                .sourceTable(VERTICA_SOURCE)
                .sinkTable(VERTICA_SINK)
                .createSql(CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .build();
    }

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/com/vertica/jdbc/vertica-jdbc/12.0.3-0/vertica-jdbc-12.0.3-0.jar";
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames = new String[] {"id", "name", "age"};

        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                i, // INT
                                String.format("f1_%s", i), // VARCHAR
                                i
                            });
            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }

    @Override
    protected GenericContainer<?> initContainer() {
        try {
            System.out.println("Initializing Vertica container: " + VERTICA_IMAGE);

            // Critical configuration based on project experience
            Map<String, String> containerEnv = new HashMap<>();
            containerEnv.put("TZ", "UTC");
            containerEnv.put("MALLOC_ARENA_MAX", "2");
            containerEnv.put("VERTICA_MEMDEBUG", "1");

            GenericContainer<?> container =
                    new GenericContainer<>(VERTICA_IMAGE)
                            .withEnv(containerEnv)
                            .withNetwork(NETWORK)
                            .withNetworkAliases(VERTICA_CONTAINER_HOST)
                            .withLogConsumer(
                                    new Slf4jLogConsumer(
                                            DockerLoggerFactory.getLogger(VERTICA_IMAGE)))
                            // Key: Configure memory and privileged mode based on experience
                            .withSharedMemorySize(2L * 1024 * 1024 * 1024) // 2GB shared memory
                            .withPrivilegedMode(true) // Enable privileged mode
                            .withStartupTimeout(java.time.Duration.ofMinutes(10))
                            // Add wait strategy
                            .waitingFor(
                                    org.testcontainers.containers.wait.strategy.Wait.forLogMessage(
                                                    ".*Vertica is now running.*", 1)
                                            .withStartupTimeout(java.time.Duration.ofMinutes(8)));

            container.setPortBindings(
                    Arrays.asList(String.format("%s:%s", VERTICA_PORT, VERTICA_PORT)));

            System.out.println("Vertica container configured successfully");
            return container;

        } catch (Exception e) {
            System.err.println(
                    "Container Initialization Exception: "
                            + e.getClass().getSimpleName()
                            + ": "
                            + e.getMessage());
            if (e.getCause() != null) {
                System.err.println(
                        "Root Cause: "
                                + e.getCause().getClass().getSimpleName()
                                + ": "
                                + e.getCause().getMessage());
            }
            e.printStackTrace();
            throw new RuntimeException("Vertica Container Initialization Failed", e);
        }
    }

    @Override
    public String quoteIdentifier(String field) {
        return "\"" + field + "\"";
    }

    @Override
    protected void beforeStartUP() {
        System.out.println("========== Vertica Container Pre-Startup Check ==========");
        System.out.println("Docker Environment: " + System.getenv("DOCKER_HOST"));

        // Check CI environment
        String ciEnv = System.getenv("CI");
        if (ciEnv != null) {
            System.out.println("CI Environment: " + ciEnv);
            System.out.println("Build ID: " + System.getenv("BUILD_ID"));
        }

        System.out.println("================================================");
        super.beforeStartUP();
    }

    @Override
    protected void initializeJdbcConnection(String jdbcUrl)
            throws SQLException, InstantiationException, IllegalAccessException {
        System.out.println("========== JDBC Connection Initialization Start ==========");
        System.out.println("JDBC URL: " + jdbcUrl);
        System.out.println("Username: " + jdbcCase.getUserName());
        System.out.println(
                "Password Length: "
                        + (jdbcCase.getPassword() != null ? jdbcCase.getPassword().length() : 0));

        try {
            super.initializeJdbcConnection(jdbcUrl);
        } catch (Exception e) {
            System.err.println("JDBC Connection Failed: " + e.getMessage());

            // Output container logs for diagnosis on failure
            if (dbServer != null && dbServer.isRunning()) {
                try {
                    String logs = dbServer.getLogs();
                    String[] logLines = logs.split("\n");
                    int startIndex = Math.max(0, logLines.length - 20); // Only last 20 lines
                    System.err.println("Container logs (last 20 lines):");
                    for (int i = startIndex; i < logLines.length; i++) {
                        System.err.println(logLines[i]);
                    }
                } catch (Exception logException) {
                    System.err.println(
                            "Failed to retrieve container logs: " + logException.getMessage());
                }
            }
            throw e;
        }
    }
}
