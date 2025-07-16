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

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.MetricNames;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class FlinkSinkWriter
        implements org.apache.flink.api.connector.sink2.SinkWriter<SeaTunnelRow> {

    // 静态指标存储，用于在作业结束时收集
    public static final Map<String, Long> GLOBAL_COUNTERS = new ConcurrentHashMap<>();

    private final SinkWriter<SeaTunnelRow, ?, ?> sinkWriter;

    // Flink的标准计数器
    private final org.apache.flink.metrics.Counter numRecordsSendCounter;
    private final org.apache.flink.metrics.Counter numBytesSendCounter;

    // SeaTunnel的计数器
    private final Counter seatunnelWriteCount;
    private final Counter seatunnelWriteBytes;

    private final Sink.InitContext context;
    private final SinkWriterMetricGroup metricGroup;

    // 添加任务ID和子任务索引，用于区分不同的实例
    private final String jobId;
    private final int subtaskIndex;

    public FlinkSinkWriter(
            SinkWriter<SeaTunnelRow, ?, ?> sinkWriter,
            Sink.InitContext context,
            MetricsContext metricsContext) {
        this.sinkWriter = sinkWriter;
        this.context = context;
        this.metricGroup = context.metricGroup();

        // 获取任务ID和子任务索引
        this.jobId = getJobId(context);
        this.subtaskIndex = context.getSubtaskId();

        // 获取Flink标准计数器
        this.numRecordsSendCounter = metricGroup.getNumRecordsSendCounter();
        this.numBytesSendCounter = metricGroup.getNumBytesSendCounter();

        // SeaTunnel计数器
        this.seatunnelWriteCount = metricsContext.counter(MetricNames.SINK_WRITE_COUNT);
        this.seatunnelWriteBytes = metricsContext.counter(MetricNames.SINK_WRITE_BYTES);

        log.info(
                "[METRICS] FlinkSinkWriter initialized with jobId: {}, subtaskIndex: {}, sinkWriter class: {}",
                jobId,
                subtaskIndex,
                sinkWriter.getClass().getName());
        log.info(
                "[METRICS] Using SinkWriterMetricGroup: {}, MetricsContext: {}",
                metricGroup.getClass().getName(),
                metricsContext.getClass().getName());
    }

    /** 获取作业ID */
    private String getJobId(Sink.InitContext context) {
        try {
            String jobId = context.getJobInfo().getJobId().toString();
            log.info("[METRICS] Successfully retrieved JobID: {}", jobId);
            return jobId;
        } catch (Exception e) {
            log.warn("[METRICS] Failed to get job ID: {}", e.getMessage());
            return "unknown";
        }
    }

    @Override
    public void write(
            SeaTunnelRow element, org.apache.flink.api.connector.sink2.SinkWriter.Context context)
            throws IOException, InterruptedException {
        try {
            // 执行写入操作
            try {
                sinkWriter.write(element);
            } catch (Exception e) {
                if (isConnectionPoolClosedException(e)) {
                    log.warn(
                            "[METRICS] Connection pool closed during write operation: {}. This record will be skipped.",
                            e.getMessage());
                    // 在这里可以选择跳过这条记录，或者实现重试逻辑
                    return; // 跳过这条记录
                } else {
                    throw e; // 重新抛出其他类型的异常
                }
            }

            // 计算字节大小
            int bytesSize = estimateRowSize(element);

            // 更新Flink标准计数器
            numRecordsSendCounter.inc();
            numBytesSendCounter.inc(bytesSize);

            // 更新SeaTunnel计数器（现在会自动注册到accumulator）
            seatunnelWriteCount.inc();
            seatunnelWriteBytes.inc(bytesSize);

            // 调试日志
            if (log.isDebugEnabled()) {
                log.debug(
                        "[METRICS] Updated counters: writeCount={}, writeBytes={}",
                        seatunnelWriteCount.getCount(),
                        seatunnelWriteBytes.getCount());
            }

            // 每写入1000条记录打印一次日志
            long count = seatunnelWriteCount.getCount();
            if (count % 1000 == 0) {
                log.info(
                        "[METRICS] Write progress: jobId={}, subtaskIndex={}, count={}, bytes={}",
                        jobId,
                        subtaskIndex,
                        count,
                        seatunnelWriteBytes.getCount());

                // 同时记录Flink计数器的值
                log.info(
                        "[METRICS] Flink counters: numRecordsSend={}, numBytesSend={}",
                        numRecordsSendCounter.getCount(),
                        numBytesSendCounter.getCount());
            }
        } catch (Exception e) {
            log.error("[METRICS] Error writing row: {}", element, e);
            throw e;
        }
    }

    // 估算行大小的辅助方法
    private int estimateRowSize(SeaTunnelRow row) {
        // 简单实现，可以根据实际情况优化
        int size = 0;
        for (int i = 0; i < row.getArity(); i++) {
            Object field = row.getField(i);
            if (field != null) {
                if (field instanceof String) {
                    size += ((String) field).length() * 2; // 假设每个字符占2字节
                } else if (field instanceof Number) {
                    size += 8; // 假设数字类型平均占8字节
                } else {
                    size += 16; // 其他类型的默认估计
                }
            }
        }
        return Math.max(size, 1); // 确保至少返回1字节
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        try {
            log.info(
                    "[METRICS] Flushing sink writer, jobId={}, subtaskIndex={}, endOfInput={}, metrics: writeCount={}, writeBytes={}",
                    jobId,
                    subtaskIndex,
                    endOfInput,
                    seatunnelWriteCount.getCount(),
                    seatunnelWriteBytes.getCount());

            // accumulator会自动处理指标收集，不需要手动更新全局计数器

            if (endOfInput) {
                log.info("[METRICS] End of input reached, closing sink writer");
                try {
                    sinkWriter.close();
                } catch (Exception e) {
                    // 检查是否是连接池已关闭的异常
                    if (isConnectionPoolClosedException(e)) {
                        log.warn(
                                "[METRICS] Connection pool already closed when flushing sink writer: {}. This is expected during shutdown and can be safely ignored.",
                                e.getMessage());
                    } else {
                        // 如果是其他异常，则重新抛出
                        log.error("[METRICS] Error flushing sink writer", e);
                        throw new IOException("Failed to flush sink writer", e);
                    }
                }
            }
        } catch (Exception e) {
            if (isConnectionPoolClosedException(e)) {
                log.warn(
                        "[METRICS] Connection pool already closed during flush: {}. This is expected during shutdown and can be safely ignored.",
                        e.getMessage());
            } else {
                log.error("[METRICS] Failed to flush sink writer", e);
                throw new IOException("Failed to flush sink writer", e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        try {
            // 获取最终指标值
            long writeCount = seatunnelWriteCount.getCount();
            long writeBytes = seatunnelWriteBytes.getCount();
            long flinkRecordCount = numRecordsSendCounter.getCount();
            long flinkByteCount = numBytesSendCounter.getCount();

            log.info(
                    "[METRICS] Closing sink writer, jobId={}, subtaskIndex={}, final metrics: writeCount={}, writeBytes={}",
                    jobId,
                    subtaskIndex,
                    writeCount,
                    writeBytes);

            log.info(
                    "[METRICS] Flink final counters: numRecordsSend={}, numBytesSend={}",
                    flinkRecordCount,
                    flinkByteCount);

            // accumulator会自动处理指标收集，不需要手动更新MetricsRegistry

            // 关闭底层的SinkWriter
            if (sinkWriter != null) {
                try {
                    sinkWriter.close();
                } catch (Exception e) {
                    // 检查是否是连接池已关闭的异常
                    if (isConnectionPoolClosedException(e)) {
                        log.warn(
                                "[METRICS] Connection pool already closed when closing sink writer: {}. This is expected during shutdown and can be safely ignored.",
                                e.getMessage());
                    } else {
                        // 如果是其他异常，则重新抛出
                        log.error("[METRICS] Error closing sink writer", e);
                        throw e;
                    }
                }
            }
        } catch (Exception e) {
            log.error("[METRICS-DEBUG] Error in FlinkSinkWriter.close()", e);
            throw e;
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
