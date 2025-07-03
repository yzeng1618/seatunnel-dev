/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.shade.com.zaxxer.hikari.HikariDataSource;

import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormatBuilder;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionPoolProviderProxy;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Slf4j
public class JdbcSinkWriter extends AbstractJdbcSinkWriter<ConnectionPoolManager> {
    private final Integer primaryKeyIndex;

    public JdbcSinkWriter(
            TablePath sinkTablePath,
            JdbcDialect dialect,
            JdbcSinkConfig jdbcSinkConfig,
            TableSchema tableSchema,
            TableSchema databaseTableSchema,
            Integer primaryKeyIndex) {
        this.sinkTablePath = sinkTablePath;
        this.dialect = dialect;
        this.tableSchema = tableSchema;
        this.databaseTableSchema = databaseTableSchema;
        this.jdbcSinkConfig = jdbcSinkConfig;
        this.primaryKeyIndex = primaryKeyIndex;
        this.connectionProvider =
                dialect.getJdbcConnectionProvider(jdbcSinkConfig.getJdbcConnectionConfig());
        this.outputFormat =
                new JdbcOutputFormatBuilder(
                                dialect,
                                connectionProvider,
                                jdbcSinkConfig,
                                tableSchema,
                                databaseTableSchema)
                        .build();
    }

    @Override
    public MultiTableResourceManager<ConnectionPoolManager> initMultiTableResourceManager(
            int tableSize, int queueSize) {
        HikariDataSource ds = new HikariDataSource();
        try {
            Class.forName(jdbcSinkConfig.getJdbcConnectionConfig().getDriverName());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        ds.setIdleTimeout(30 * 1000);
        ds.setMaximumPoolSize(queueSize);
        ds.setJdbcUrl(jdbcSinkConfig.getJdbcConnectionConfig().getUrl());
        ds.setDriverClassName(jdbcSinkConfig.getJdbcConnectionConfig().getDriverName());
        if (jdbcSinkConfig.getJdbcConnectionConfig().getUsername().isPresent()) {
            ds.setUsername(jdbcSinkConfig.getJdbcConnectionConfig().getUsername().get());
        }
        if (jdbcSinkConfig.getJdbcConnectionConfig().getPassword().isPresent()) {
            ds.setPassword(jdbcSinkConfig.getJdbcConnectionConfig().getPassword().get());
        }
        ds.setAutoCommit(jdbcSinkConfig.getJdbcConnectionConfig().isAutoCommit());
        jdbcSinkConfig.getJdbcConnectionConfig().getProperties().forEach(ds::addDataSourceProperty);
        return new JdbcMultiTableResourceManager(new ConnectionPoolManager(ds));
    }

    @Override
    public void setMultiTableResourceManager(
            MultiTableResourceManager<ConnectionPoolManager> multiTableResourceManager,
            int queueIndex) {
        connectionProvider.closeConnection();
        this.connectionProvider =
                new SimpleJdbcConnectionPoolProviderProxy(
                        multiTableResourceManager.getSharedResource().get(),
                        jdbcSinkConfig.getJdbcConnectionConfig(),
                        queueIndex);
        this.outputFormat =
                new JdbcOutputFormatBuilder(
                                dialect,
                                connectionProvider,
                                jdbcSinkConfig,
                                tableSchema,
                                databaseTableSchema)
                        .build();
    }

    @Override
    public Optional<Integer> primaryKey() {
        return primaryKeyIndex != null ? Optional.of(primaryKeyIndex) : Optional.empty();
    }

    private void tryOpen() throws IOException {
        if (!isOpen) {
            isOpen = true;
            outputFormat.open();
        }
    }

    @Override
    public List<JdbcSinkState> snapshotState(long checkpointId) {
        return Collections.emptyList();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        tryOpen();
        outputFormat.writeRecord(element);
    }

    @Override
    public Optional<XidInfo> prepareCommit() throws IOException {
        tryOpen();
        outputFormat.checkFlushException();
        outputFormat.flush();
        try {
            if (!connectionProvider.getConnection().getAutoCommit()) {
                connectionProvider.getConnection().commit();
            }
        } catch (SQLException e) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.TRANSACTION_OPERATION_FAILED,
                    "commit failed," + e.getMessage(),
                    e);
        }
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {
        // 首先尝试刷新缓冲区
        try {
            tryOpen();
            outputFormat.flush();
        } catch (Exception e) {
            if (isConnectionPoolClosedException(e)) {
                log.warn("Connection pool already closed during flush: {}", e.getMessage());
            } else {
                log.error("Error flushing output format", e);
            }
        }

        // 尝试获取连接并提交事务
        try {
            Connection connection = null;
            connection = connectionProvider.getConnection();

            // 只有当连接不为null且有效时才尝试提交
            if (connection != null) {
                try {
                    if (!connection.isClosed() && !connection.getAutoCommit()) {
                        connection.commit();
                    }
                } catch (SQLException e) {
                    if (isConnectionPoolClosedException(e)) {
                        log.warn(
                                "Connection pool already closed when committing: {}",
                                e.getMessage());
                    } else {
                        throw new JdbcConnectorException(
                                CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED,
                                "Unable to commit transaction",
                                e);
                    }
                }
            }
        } finally {
            // 最后关闭输出格式
            try {
                outputFormat.close();
            } catch (Exception e) {
                if (isConnectionPoolClosedException(e)) {
                    log.warn(
                            "Connection pool already closed when closing output format: {}",
                            e.getMessage());
                } else {
                    throw new IOException("Error closing output format", e);
                }
            }
        }
    }

    /** 检查异常是否与连接池关闭相关 */
    private boolean isConnectionPoolClosedException(Throwable e) {
        if (e == null) {
            return false;
        }

        // 检查异常消息
        String message = e.getMessage();
        if (message != null
                && (message.contains("HikariDataSource has been closed")
                        || message.contains("Connection is closed")
                        || message.contains("Connection pool has been closed")
                        || message.contains("Pool has been shutdown"))) {
            return true;
        }

        // 检查异常类型
        if (e instanceof SQLNonTransientConnectionException) {
            return true;
        }

        // 检查原因链
        Throwable cause = e.getCause();
        if (cause != null && cause != e) {
            return isConnectionPoolClosedException(cause);
        }

        return false;
    }
}
