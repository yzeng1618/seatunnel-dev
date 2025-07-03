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

import lombok.Getter;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Getter
public class ConnectionPoolManager {

    private final HikariDataSource connectionPool;

    private final Map<Integer, Connection> connectionMap;

    ConnectionPoolManager(HikariDataSource connectionPool) {
        this.connectionPool = connectionPool;
        connectionMap = new ConcurrentHashMap<>();
    }

    public Connection getConnection(int index) {
        // 先检查连接池是否已关闭
        try {
            if (connectionPool == null || connectionPool.isClosed()) {
                log.warn("Connection pool is null or already closed");
                return null;
            }
        } catch (Exception e) {
            log.warn("Error checking if connection pool is closed: {}", e.getMessage());
            return null;
        }

        try {
            return connectionMap.computeIfAbsent(
                    index,
                    i -> {
                        try {
                            return connectionPool.getConnection();
                        } catch (SQLException e) {
                            // 如果是连接池已关闭的异常，返回null
                            if (e.getMessage() != null
                                    && (e.getMessage().contains("HikariDataSource has been closed")
                                            || e.getMessage().contains("Connection is closed")
                                            || e.getMessage().contains("Pool has been shutdown"))) {
                                log.warn(
                                        "Connection pool closed when getting connection: {}",
                                        e.getMessage());
                                return null;
                            }
                            throw new RuntimeException(e);
                        }
                    });
        } catch (Exception e) {
            // 捕获所有异常，包括可能的NullPointerException
            log.warn("Error getting connection from pool: {}", e.getMessage());
            return null;
        }
    }

    public boolean containsConnection(int index) {
        return connectionMap.containsKey(index);
    }

    public Connection remove(int index) {
        return connectionMap.remove(index);
    }

    public String getPoolName() {
        return connectionPool.getPoolName();
    }

    public void close() {
        if (!connectionPool.isClosed()) {
            connectionPool.close();
        }
    }
}
