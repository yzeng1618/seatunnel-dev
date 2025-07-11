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

    private final SinkWriter<SeaTunnelRow, ?, ?> sinkWriter;
    // Flink 的 Counter 类型
    private final org.apache.flink.metrics.Counter numRecordsOut;
    private final org.apache.flink.metrics.Counter flinkWriteCount;
    private final org.apache.flink.metrics.Counter flinkWriteBytes;
    // SeaTunnel 的 Counter 类型
    private final Counter seatunnelWriteCount;
    private final Counter seatunnelWriteBytes;
    private final Sink.InitContext context;

    // 添加静态计数器映射，用于在作业结束时收集指标
    private static final Map<String, Long> GLOBAL_COUNTERS = new ConcurrentHashMap<>();

    // 添加任务ID和子任务索引，用于区分不同的实例
    private final String jobId;
    private final int subtaskIndex;

    public FlinkSinkWriter(
            SinkWriter<SeaTunnelRow, ?, ?> sinkWriter,
            Sink.InitContext context,
            MetricsContext metricsContext) {
        this.sinkWriter = sinkWriter;
        this.context = context;

        // 获取任务ID和子任务索引
        this.jobId = getJobId(context);
        this.subtaskIndex = context.getSubtaskId();

        // Flink 计数器
        this.numRecordsOut = context.metricGroup().counter("numRecordsOut");
        this.flinkWriteCount = context.metricGroup().counter("seatunnel_SINK_WRITE_COUNT");
        this.flinkWriteBytes = context.metricGroup().counter("seatunnel_SINK_WRITE_BYTES");

        // SeaTunnel 计数器
        this.seatunnelWriteCount = metricsContext.counter(MetricNames.SINK_WRITE_COUNT);
        this.seatunnelWriteBytes = metricsContext.counter(MetricNames.SINK_WRITE_BYTES);
        log.info(
                "FlinkSinkWriter initialized with sinkWriter: {}, jobId: {}, subtaskIndex: {}",
                sinkWriter,
                jobId,
                subtaskIndex);
    }

    /** 获取作业ID */
    private String getJobId(Sink.InitContext context) {
        try {
            return context.getJobInfo().getJobId().toString();
        } catch (Exception e) {
            log.warn("Failed to get job ID: {}", e.getMessage());
            return "unknown";
        }
    }

    @Override
    public void write(
            SeaTunnelRow element, org.apache.flink.api.connector.sink2.SinkWriter.Context context)
            throws IOException, InterruptedException {
        try {
            // 记录写入前的日志
            if (log.isDebugEnabled()) {
                log.debug("Writing row: {}", element);
            }

            // 执行写入操作
            sinkWriter.write(element);

            // 更新 Flink 计数器
            numRecordsOut.inc();
            flinkWriteCount.inc();

            // 更新 SeaTunnel 计数器
            seatunnelWriteCount.inc();

            // 计算并增加字节数
            int bytesSize = estimateRowSize(element);
            flinkWriteBytes.inc(bytesSize);
            seatunnelWriteBytes.inc(bytesSize);

            // 每写入100条记录打印一次日志
            long count = seatunnelWriteCount.getCount();
            if (count % 100 == 0) {
                log.info(
                        "Write progress: count={}, bytes={}",
                        count,
                        seatunnelWriteBytes.getCount());
            }
        } catch (Exception e) {
            log.error("Error writing row: {}", element, e);
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
        // 在Flink Sink2 API中，flush方法是可选的
        try {
            log.info(
                    "Flushing sink writer, endOfInput={}, current metrics: writeCount={}, writeBytes={}",
                    endOfInput,
                    seatunnelWriteCount.getCount(),
                    seatunnelWriteBytes.getCount());

            // 更新全局计数器
            updateGlobalCounters();

            if (endOfInput) {
                log.info("End of input reached, closing sink writer");
                sinkWriter.close();
            }
        } catch (Exception e) {
            log.error("Failed to flush sink writer", e);
            throw new IOException("Failed to flush sink writer", e);
        }
    }

    /** 更新全局计数器，用于在作业结束时收集指标 */
    private void updateGlobalCounters() {
        try {
            // 使用任务ID和子任务索引作为键的一部分，确保不同实例的计数器不会相互覆盖
            String writeCountKey =
                    String.format("%s_%d_%s", jobId, subtaskIndex, MetricNames.SINK_WRITE_COUNT);
            String writeBytesKey =
                    String.format("%s_%d_%s", jobId, subtaskIndex, MetricNames.SINK_WRITE_BYTES);

            // 更新全局计数器
            GLOBAL_COUNTERS.put(writeCountKey, seatunnelWriteCount.getCount());
            GLOBAL_COUNTERS.put(writeBytesKey, seatunnelWriteBytes.getCount());

            // 同时更新不带子任务索引的总计数器
            String totalWriteCountKey = String.format("%s_%s", jobId, MetricNames.SINK_WRITE_COUNT);
            String totalWriteBytesKey = String.format("%s_%s", jobId, MetricNames.SINK_WRITE_BYTES);

            GLOBAL_COUNTERS.compute(
                    totalWriteCountKey,
                    (k, v) -> (v == null ? 0 : v) + seatunnelWriteCount.getCount());
            GLOBAL_COUNTERS.compute(
                    totalWriteBytesKey,
                    (k, v) -> (v == null ? 0 : v) + seatunnelWriteBytes.getCount());

            log.info(
                    "Updated global counters: {} = {}, {} = {}",
                    writeCountKey,
                    GLOBAL_COUNTERS.get(writeCountKey),
                    writeBytesKey,
                    GLOBAL_COUNTERS.get(writeBytesKey));

            log.info(
                    "Total global counters: {} = {}, {} = {}",
                    totalWriteCountKey,
                    GLOBAL_COUNTERS.get(totalWriteCountKey),
                    totalWriteBytesKey,
                    GLOBAL_COUNTERS.get(totalWriteBytesKey));
        } catch (Exception e) {
            log.warn("Failed to update global counters: {}", e.getMessage());
        }
    }

    @Override
    public void close() throws Exception {
        try {
            // 记录最终指标
            long writeCount = seatunnelWriteCount.getCount();
            long writeBytes = seatunnelWriteBytes.getCount();

            log.info(
                    "Closing sink writer, final metrics: writeCount={}, writeBytes={}",
                    writeCount,
                    writeBytes);

            // 最后一次更新全局计数器
            updateGlobalCounters();

            // 尝试将指标添加到系统属性中，以便 FlinkJobMetricsSummary 可以收集
            try {
                String writeCountKey =
                        String.format(
                                "seatunnel.metric.%s.%s", jobId, MetricNames.SINK_WRITE_COUNT);
                String writeBytesKey =
                        String.format(
                                "seatunnel.metric.%s.%s", jobId, MetricNames.SINK_WRITE_BYTES);

                // 添加标准格式的系统属性
                System.setProperty(writeCountKey, String.valueOf(writeCount));
                System.setProperty(writeBytesKey, String.valueOf(writeBytes));

                // 同时添加大写格式的系统属性，以确保兼容性
                System.setProperty(
                        "seatunnel.metric." + jobId + ".SinkWriteCount",
                        String.valueOf(writeCount));
                System.setProperty(
                        "seatunnel.metric." + jobId + ".SinkWriteBytes",
                        String.valueOf(writeBytes));

                // 添加一个全局属性，不依赖于作业ID
                System.setProperty("seatunnel.global.SinkWriteCount", String.valueOf(writeCount));
                System.setProperty("seatunnel.global.SinkWriteBytes", String.valueOf(writeBytes));

                log.info(
                        "Added metrics to system properties: {}={}, {}={}",
                        writeCountKey,
                        writeCount,
                        writeBytesKey,
                        writeBytes);
            } catch (Exception e) {
                log.warn("Failed to add metrics to system properties: {}", e.getMessage());
            }

            // 尝试直接使用SinkWriterMetricGroup更新指标
            try {
                SinkWriterMetricGroup metricGroup = context.metricGroup();
                if (metricGroup != null) {
                    log.info("Got SinkWriterMetricGroup: {}", metricGroup.getClass().getName());

                    // 获取Flink内置计数器 - 修复类型转换错误
                    org.apache.flink.metrics.Counter numRecordsSendCounter =
                            metricGroup.getNumRecordsSendCounter();
                    org.apache.flink.metrics.Counter numBytesSendCounter =
                            metricGroup.getNumBytesSendCounter();

                    // 检查计数器当前值
                    if (numRecordsSendCounter != null) {
                        long currentCount = numRecordsSendCounter.getCount();
                        log.info("Current numRecordsSendCounter: {}", currentCount);

                        // 如果当前值为0，则增加我们的计数
                        if (currentCount == 0) {
                            numRecordsSendCounter.inc(writeCount);
                            log.info("Updated numRecordsSendCounter: +{}", writeCount);
                        } else {
                            log.info(
                                    "numRecordsSendCounter already has value {}, not updating",
                                    currentCount);
                        }
                    } else {
                        log.warn("numRecordsSendCounter is null");
                    }

                    if (numBytesSendCounter != null) {
                        long currentBytes = numBytesSendCounter.getCount();
                        log.info("Current numBytesSendCounter: {}", currentBytes);

                        // 如果当前值为0，则增加我们的计数
                        if (currentBytes == 0) {
                            numBytesSendCounter.inc(writeBytes);
                            log.info("Updated numBytesSendCounter: +{}", writeBytes);
                        } else {
                            log.info(
                                    "numBytesSendCounter already has value {}, not updating",
                                    currentBytes);
                        }
                    } else {
                        log.warn("numBytesSendCounter is null");
                    }

                    log.info("Successfully updated Flink metrics using SinkWriterMetricGroup");
                } else {
                    log.warn("metricGroup() returned null");
                }
            } catch (Exception e) {
                log.warn(
                        "Failed to update metrics using SinkWriterMetricGroup: {}", e.getMessage());
            }

            // 尝试关闭 sinkWriter
            try {
                sinkWriter.close();
            } catch (Exception e) {
                // 检查是否是连接池已关闭的异常
                if (isConnectionPoolClosedException(e)) {
                    log.warn(
                            "Caught connection pool closed exception during sink writer close: {}",
                            e.getMessage());
                    // 不重新抛出异常，允许关闭过程继续
                } else {
                    throw e;
                }
            }
        } catch (Exception e) {
            log.error("Error during sink writer close", e);
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
