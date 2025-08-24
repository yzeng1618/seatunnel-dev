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

    // 静态初始化块，用于诊断系统环境
    static {
        System.out.println("========== JdbcVerticaIT 系统环境诊断 ==========");
        System.out.println("Java版本: " + System.getProperty("java.version"));
        System.out.println("操作系统: " + System.getProperty("os.name") + " " + System.getProperty("os.version"));
        System.out.println("可用内存: " + (Runtime.getRuntime().maxMemory() / 1024 / 1024) + "MB");
        System.out.println("Docker环境变量: " + System.getenv("DOCKER_HOST"));
        System.out.println("用户目录: " + System.getProperty("user.home"));
        System.out.println("当前工作目录: " + System.getProperty("user.dir"));
        
        // 检查是否在CI环境中
        String ciEnv = System.getenv("CI");
        if (ciEnv != null) {
            System.out.println("检测到CI环境: " + ciEnv);
            System.out.println("CI构建ID: " + System.getenv("BUILD_ID"));
            System.out.println("CI构建URL: " + System.getenv("BUILD_URL"));
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
        // 基于项目经验配置Vertica容器环境变量
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
            System.out.println("========== Vertica容器初始化开始 ==========");
            System.out.println("镜像: " + VERTICA_IMAGE);
            System.out.println("容器主机: " + VERTICA_CONTAINER_HOST);
            System.out.println("端口: " + VERTICA_PORT);
            System.out.println("Java版本: " + System.getProperty("java.version"));
            System.out.println("OS: " + System.getProperty("os.name"));
            System.out.println("可用内存: " + (Runtime.getRuntime().maxMemory() / 1024 / 1024) + "MB");
            
            // 基于项目经验的关键配置
            Map<String, String> containerEnv = new HashMap<>();
            containerEnv.put("TZ", "UTC");
            containerEnv.put("MALLOC_ARENA_MAX", "2");
            containerEnv.put("VERTICA_MEMDEBUG", "1");
            
            GenericContainer<?> container = new GenericContainer<>(VERTICA_IMAGE)
                    .withEnv(containerEnv)
                    .withNetwork(NETWORK)
                    .withNetworkAliases(VERTICA_CONTAINER_HOST)
                    .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(VERTICA_IMAGE)))
                    // 关键：基于经验配置内存和特权模式
                    .withSharedMemorySize(2L * 1024 * 1024 * 1024) // 2GB共享内存
                    .withPrivilegedMode(true) // 启用特权模式
                    .withStartupTimeout(java.time.Duration.ofMinutes(10))
                    // 添加等待策略
                    .waitingFor(org.testcontainers.containers.wait.strategy.Wait.forLogMessage(".*Vertica is now running.*", 1)
                        .withStartupTimeout(java.time.Duration.ofMinutes(8)));
            
            container.setPortBindings(
                    Arrays.asList(String.format("%s:%s", VERTICA_PORT, VERTICA_PORT)));
            
            System.out.println("容器配置完成 - 环境变量: " + containerEnv);
            System.out.println("容器配置完成 - 共享内存: 2GB");
            System.out.println("容器配置完成 - 特权模式: true");
            System.out.println("容器配置完成 - 启动超时: 10分钟");
            System.out.println("===============================================");
            
            return container;
            
        } catch (Exception e) {
            System.err.println("容器初始化异常: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            if (e.getCause() != null) {
                System.err.println("根本原因: " + e.getCause().getClass().getSimpleName() + ": " + e.getCause().getMessage());
            }
            e.printStackTrace();
            throw new RuntimeException("Vertica容器初始化失败", e);
        }
    }

    @Override
    public String quoteIdentifier(String field) {
        return "\"" + field + "\"";
    }
    
    @Override
    protected void beforeStartUP() {
        System.out.println("========== Vertica容器启动前检查 ==========");
        System.out.println("Docker环境: " + System.getenv("DOCKER_HOST"));
        
        // 检查CI环境
        String ciEnv = System.getenv("CI");
        if (ciEnv != null) {
            System.out.println("CI环境: " + ciEnv);
            System.out.println("构建ID: " + System.getenv("BUILD_ID"));
        }
        
        System.out.println("================================================");
        super.beforeStartUP();
    }
    
    @Override
    protected void initializeJdbcConnection(String jdbcUrl)
            throws SQLException, InstantiationException, IllegalAccessException {
        System.out.println("========== JDBC连接初始化开始 ==========");
        System.out.println("JDBC URL: " + jdbcUrl);
        System.out.println("用户名: " + jdbcCase.getUserName());
        System.out.println("密码长度: " + (jdbcCase.getPassword() != null ? jdbcCase.getPassword().length() : 0));
        
        if (dbServer != null) {
            System.out.println("容器状态: " + (dbServer.isRunning() ? "运行中" : "已停止"));
            System.out.println("容器主机: " + dbServer.getHost());
            System.out.println("容器端口: " + dbServer.getMappedPort(VERTICA_PORT));
            
            String actualJdbcUrl = jdbcUrl.replace(HOST, dbServer.getHost());
            System.out.println("实际JDBC URL: " + actualJdbcUrl);
        }
        
        try {
            super.initializeJdbcConnection(jdbcUrl);
            System.out.println("JDBC连接初始化成功");
        } catch (Exception e) {
            System.err.println("JDBC连接初始化失败: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            if (e.getCause() != null) {
                System.err.println("JDBC连接失败根本原因: " + e.getCause().getClass().getSimpleName() + ": " + e.getCause().getMessage());
            }
            
            // 输出容器日志帮助诊断
            if (dbServer != null && dbServer.isRunning()) {
                try {
                    System.err.println("容器日志(最后100行):");
                    String logs = dbServer.getLogs();
                    String[] logLines = logs.split("\n");
                    int startIndex = Math.max(0, logLines.length - 100);
                    for (int i = startIndex; i < logLines.length; i++) {
                        System.err.println(logLines[i]);
                    }
                } catch (Exception logException) {
                    System.err.println("获取容器日志失败: " + logException.getMessage());
                }
            }
            
            System.out.println("================================================");
            throw e;
        }
        System.out.println("================================================");
    }
}
