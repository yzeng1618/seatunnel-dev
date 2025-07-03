/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.seatunnel.translation.flink.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.metrics.Counter;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.SQLException;

@Slf4j
public class FlinkSinkWriter
        implements org.apache.flink.api.connector.sink2.SinkWriter<SeaTunnelRow> {

    private final SinkWriter<SeaTunnelRow, ?, ?> sinkWriter;
    private final Counter numRecordsOut;

    public FlinkSinkWriter(SinkWriter<SeaTunnelRow, ?, ?> sinkWriter, Sink.InitContext context) {
        this.sinkWriter = sinkWriter;
        this.numRecordsOut = context.metricGroup().counter("numRecordsOut");
        log.info("FlinkSinkWriter initialized with sinkWriter: {}", sinkWriter);
    }

    @Override
    public void write(
            SeaTunnelRow element, org.apache.flink.api.connector.sink2.SinkWriter.Context context)
            throws IOException, InterruptedException {
        sinkWriter.write(element);
        numRecordsOut.inc();
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        // 在Flink Sink2 API中，flush方法是可选的，我们可以在这里调用SeaTunnel的prepare方法
        if (endOfInput) {
            sinkWriter.close();
        }
    }

    @Override
    public void close() throws Exception {
        try {
            // 尝试关闭 sinkWriter
            sinkWriter.close();
        } catch (Exception e) {
            // 检查是否是连接池已关闭的异常
            if (isConnectionPoolClosedException(e)) {
                log.warn(
                        "Caught connection pool closed exception during sink writer close: {}",
                        e.getMessage());
                // 不重新抛出异常，允许关闭过程继续
            } else {
                // 其他异常正常抛出
                throw e;
            }
        }
    }

    /** 检查异常是否是连接池已关闭的异常 */
    private boolean isConnectionPoolClosedException(Throwable e) {
        if (e == null) {
            return false;
        }

        // 检查异常消息
        if (e.getMessage() != null
                && (e.getMessage().contains("HikariDataSource has been closed")
                        || e.getMessage().contains("Connection is closed")
                        || e.getMessage().contains("Connection pool has been closed")
                        || e.getMessage().contains("Pool has been shutdown"))) {
            return true;
        }

        // 检查原因链
        Throwable cause = e.getCause();
        if (cause != null && cause != e) {
            return isConnectionPoolClosedException(cause);
        }

        // 检查是否是SQLException
        if (e instanceof SQLException) {
            return true;
        }

        return false;
    }
}
