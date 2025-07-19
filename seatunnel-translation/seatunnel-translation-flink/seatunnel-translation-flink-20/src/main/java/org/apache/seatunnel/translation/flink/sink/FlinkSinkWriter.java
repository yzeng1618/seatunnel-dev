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

@Slf4j
public class FlinkSinkWriter
        implements org.apache.flink.api.connector.sink2.SinkWriter<SeaTunnelRow> {

    private final SinkWriter<SeaTunnelRow, ?, ?> sinkWriter;

    private final org.apache.flink.metrics.Counter numRecordsSendCounter;
    private final org.apache.flink.metrics.Counter numBytesSendCounter;

    private final Counter seatunnelWriteCount;
    private final Counter seatunnelWriteBytes;

    private final Sink.InitContext context;
    private final SinkWriterMetricGroup metricGroup;

    private final String jobId;
    private final int subtaskIndex;

    public FlinkSinkWriter(
            SinkWriter<SeaTunnelRow, ?, ?> sinkWriter,
            Sink.InitContext context,
            MetricsContext metricsContext) {
        this.sinkWriter = sinkWriter;
        this.context = context;
        this.metricGroup = context.metricGroup();

        this.jobId = getJobId(context);
        this.subtaskIndex = context.getSubtaskId();

        this.numRecordsSendCounter = metricGroup.getNumRecordsSendCounter();
        this.numBytesSendCounter = metricGroup.getNumBytesSendCounter();

        this.seatunnelWriteCount = metricsContext.counter(MetricNames.SINK_WRITE_COUNT);
        this.seatunnelWriteBytes = metricsContext.counter(MetricNames.SINK_WRITE_BYTES);

        log.info(
                "FlinkSinkWriter initialized with jobId: {}, subtaskIndex: {}",
                jobId,
                subtaskIndex);
    }

    /** Get job ID */
    private String getJobId(Sink.InitContext context) {
        try {
            String jobId = context.getJobInfo().getJobId().toString();
            log.debug("Successfully retrieved JobID: {}", jobId);
            return jobId;
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
            try {
                sinkWriter.write(element);
            } catch (Exception e) {
                if (isConnectionPoolClosedException(e)) {
                    log.warn(
                            "[METRICS] Connection pool closed during write operation: {}. This record will be skipped.",
                            e.getMessage());
                    return;
                } else {
                    throw e;
                }
            }

            int bytesSize = estimateRowSize(element);

            numRecordsSendCounter.inc();
            numBytesSendCounter.inc(bytesSize);

            seatunnelWriteCount.inc();
            seatunnelWriteBytes.inc(bytesSize);

            long count = seatunnelWriteCount.getCount();
            if (count % 10000 == 0) {
                log.info(
                        "Write progress: jobId={}, subtaskIndex={}, count={}, bytes={}",
                        jobId,
                        subtaskIndex,
                        count,
                        seatunnelWriteBytes.getCount());
            }
        } catch (Exception e) {
            log.error("Error writing row: {}", element, e);
            throw e;
        }
    }

    private int estimateRowSize(SeaTunnelRow row) {
        int size = 0;
        for (int i = 0; i < row.getArity(); i++) {
            Object field = row.getField(i);
            if (field != null) {
                if (field instanceof String) {
                    size += ((String) field).length() * 2;
                } else if (field instanceof Number) {
                    size += 8;
                } else {
                    size += 16;
                }
            }
        }
        return Math.max(size, 1);
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

            if (endOfInput) {
                log.info("[METRICS] End of input reached, closing sink writer");
                try {
                    sinkWriter.close();
                } catch (Exception e) {
                    if (isConnectionPoolClosedException(e)) {
                        log.warn(
                                "[METRICS] Connection pool already closed when flushing sink writer: {}. This is expected during shutdown and can be safely ignored.",
                                e.getMessage());
                    } else {
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

            if (sinkWriter != null) {
                try {
                    sinkWriter.close();
                } catch (Exception e) {
                    if (isConnectionPoolClosedException(e)) {
                        log.warn(
                                "[METRICS] Connection pool already closed when closing sink writer: {}. This is expected during shutdown and can be safely ignored.",
                                e.getMessage());
                    } else {
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

    private boolean isConnectionPoolClosedException(Throwable e) {
        if (e == null) {
            return false;
        }

        if (e.getMessage() != null
                && (e.getMessage().contains("HikariDataSource has been closed")
                        || e.getMessage().contains("Connection is closed")
                        || e.getMessage().contains("Connection pool has been closed")
                        || e.getMessage().contains("Pool has been shutdown"))) {
            return true;
        }

        Throwable cause = e.getCause();
        if (cause != null && cause != e) {
            return isConnectionPoolClosedException(cause);
        }

        if (e instanceof SQLException) {
            return true;
        }

        return false;
    }
}
