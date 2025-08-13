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
import org.apache.seatunnel.api.common.metrics.Meter;
import org.apache.seatunnel.api.common.metrics.MetricNames;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportResourceShare;
import org.apache.seatunnel.api.sink.event.WriterCloseEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.flink.api.connector.sink2.Sink;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class FlinkSinkWriter
        implements org.apache.flink.api.connector.sink2.SinkWriter<SeaTunnelRow> {

    private final SinkWriter<SeaTunnelRow, ?, ?> sinkWriter;
    private final SinkWriter.Context context;
    private final Counter sinkWriteCount;
    private final Counter sinkWriteBytes;
    private final Meter sinkWriterQPS;
    private long checkpointId;
    private MultiTableResourceManager resourceManager;
    private volatile boolean closed = false;

    public FlinkSinkWriter(
            SinkWriter<SeaTunnelRow, ?, ?> sinkWriter,
            Sink.InitContext initContext,
            SinkWriter.Context context) {
        this.sinkWriter = sinkWriter;
        this.context = context;
        this.checkpointId = 1; // Start from checkpoint 1
        MetricsContext metricsContext = context.getMetricsContext();
        this.sinkWriteCount = metricsContext.counter(MetricNames.SINK_WRITE_COUNT);
        this.sinkWriteBytes = metricsContext.counter(MetricNames.SINK_WRITE_BYTES);
        this.sinkWriterQPS = metricsContext.meter(MetricNames.SINK_WRITE_QPS);

        // Initialize resource manager if supported
        if (sinkWriter instanceof SupportResourceShare) {
            resourceManager =
                    ((SupportResourceShare) sinkWriter).initMultiTableResourceManager(1, 1);
            ((SupportResourceShare) sinkWriter).setMultiTableResourceManager(resourceManager, 0);
        }
    }

    @Override
    public void write(
            SeaTunnelRow element, org.apache.flink.api.connector.sink2.SinkWriter.Context context)
            throws IOException, InterruptedException {
        if (element == null) {
            return;
        }
        sinkWriter.write(element);
        sinkWriteCount.inc();
        sinkWriteBytes.inc(element.getBytesSize());
        sinkWriterQPS.markEvent();
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        try {
            // For Flink Sink API 2.0, we need to simulate the checkpoint behavior
            // First call prepareCommit() to flush current batch
            sinkWriter.prepareCommit();
            log.debug("Sink writer prepareCommit called successfully, endOfInput: {}", endOfInput);

            // Then call snapshotState() to finalize the checkpoint and start new batch
            // This is crucial for connectors like Doris that need proper checkpoint handling
            sinkWriter.snapshotState(checkpointId);
            log.debug("Sink writer snapshotState called with checkpointId: {}", checkpointId);

            // Increment checkpoint ID for next flush
            this.checkpointId++;
        } catch (Exception e) {
            log.error("Error during sink writer flush", e);
            throw new IOException("Failed to flush sink writer", e);
        }
    }

    @Override
    public void close() throws Exception {
        sinkWriter.close();
        context.getEventListener().onEvent(new WriterCloseEvent());
        try {
            if (resourceManager != null) {
                resourceManager.close();
            }
        } catch (Throwable e) {
            log.error("close resourceManager error", e);
        }
    }
}
