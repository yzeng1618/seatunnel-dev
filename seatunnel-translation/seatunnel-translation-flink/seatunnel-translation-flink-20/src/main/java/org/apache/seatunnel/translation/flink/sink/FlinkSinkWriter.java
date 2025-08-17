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

import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

@Slf4j
public class FlinkSinkWriter<CommT, WriterStateT>
        implements CommittingSinkWriter<SeaTunnelRow, CommitWrapper<CommT>>,
                StatefulSinkWriter<SeaTunnelRow, FlinkWriterState<WriterStateT>> {

    private final SinkWriter<SeaTunnelRow, CommT, WriterStateT> sinkWriter;
    private final SinkWriter.Context context;
    private final Counter sinkWriteCount;
    private final Counter sinkWriteBytes;
    private final Meter sinkWriterQPS;
    private long checkpointId;
    private MultiTableResourceManager resourceManager;
    private boolean closed = false;
    private boolean isMultiTableSink = false;

    public FlinkSinkWriter(
            SinkWriter<SeaTunnelRow, CommT, WriterStateT> sinkWriter,
            WriterInitContext initContext,
            SinkWriter.Context context) {
        this(sinkWriter, initContext, context, 1); // Default checkpoint ID
    }

    public FlinkSinkWriter(
            SinkWriter<SeaTunnelRow, CommT, WriterStateT> sinkWriter,
            WriterInitContext initContext,
            SinkWriter.Context context,
            long checkpointId) {
        this.sinkWriter = sinkWriter;
        this.context = context;
        this.checkpointId = checkpointId;
        MetricsContext metricsContext = context.getMetricsContext();
        this.sinkWriteCount = metricsContext.counter(MetricNames.SINK_WRITE_COUNT);
        this.sinkWriteBytes = metricsContext.counter(MetricNames.SINK_WRITE_BYTES);
        this.sinkWriterQPS = metricsContext.meter(MetricNames.SINK_WRITE_QPS);

        // Initialize resource manager if supported
        if (sinkWriter instanceof SupportResourceShare) {
            resourceManager =
                    ((SupportResourceShare) sinkWriter).initMultiTableResourceManager(1, 1);
            ((SupportResourceShare) sinkWriter).setMultiTableResourceManager(resourceManager, 0);
            isMultiTableSink = true;
            log.debug("Multi-table resource manager initialized for sink writer");
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
        if (closed) {
            log.warn("Sink writer is already closed, skipping flush");
            return;
        }

        try {
            // In Flink 1.20 Sink2 API, flush is called before prepareCommit
            // We don't need to call snapshotState here as it will be called in snapshotState method
            log.debug(
                    "Sink writer flush completed, endOfInput: {}, current checkpointId: {}",
                    endOfInput,
                    checkpointId);
        } catch (Exception e) {
            log.error("Error during sink writer flush with checkpointId: {}", checkpointId, e);
            throw new IOException("Failed to flush sink writer", e);
        }
    }

    @Override
    public Collection<CommitWrapper<CommT>> prepareCommit()
            throws IOException, InterruptedException {
        if (closed) {
            log.warn("Sink writer is already closed, returning empty commit collection");
            return new ArrayList<>();
        }

        try {
            // Call the SeaTunnel sink writer's prepareCommit method
            // Use the current checkpointId (which should be the one we're preparing for)
            long currentCheckpointId = this.checkpointId;
            Optional<CommT> commitInfo = sinkWriter.prepareCommit(currentCheckpointId);
            log.debug(
                    "Sink writer prepareCommit called with checkpointId: {}, returned commit info: {}",
                    currentCheckpointId,
                    commitInfo.isPresent());

            // Wrap the commit info in CommitWrapper
            List<CommitWrapper<CommT>> wrappedCommits = new ArrayList<>();
            if (commitInfo.isPresent()) {
                CommitWrapper<CommT> wrapper = new CommitWrapper<>(commitInfo.get());
                wrappedCommits.add(wrapper);
                log.debug(
                        "Created CommitWrapper for checkpointId: {} with commit: {}",
                        currentCheckpointId,
                        commitInfo.get());

                // Enhanced logging for schema evolution scenarios
                if (isMultiTableSink) {
                    log.debug(
                            "Multi-table sink prepared commit for checkpointId: {} - this may be part of schema evolution",
                            currentCheckpointId);
                }
            } else {
                log.debug(
                        "No commit info to wrap for checkpointId: {} - this is normal for empty checkpoints or schema evolution scenarios",
                        currentCheckpointId);
                // For multi-table scenarios and schema evolution, some writers may not have data to
                // commit
                // This is normal and should not be treated as an error
                // However, we should still track this for debugging purposes
                if (isMultiTableSink) {
                    log.debug(
                            "Multi-table sink has no commit info for checkpointId: {} - may indicate schema evolution or empty checkpoint",
                            currentCheckpointId);
                }
            }

            return wrappedCommits;
        } catch (Exception e) {
            log.error(
                    "Error during sink writer prepareCommit with checkpointId: {}",
                    checkpointId,
                    e);
            throw new IOException("Failed to prepare commit for sink writer", e);
        }
    }

    // StatefulSinkWriter interface method
    @Override
    public List<FlinkWriterState<WriterStateT>> snapshotState(long checkpointId)
            throws IOException {
        log.debug("Snapshotting state for checkpointId: {}", checkpointId);

        try {
            // Get state from SeaTunnel sink writer
            List<WriterStateT> states = sinkWriter.snapshotState(checkpointId);

            // Wrap states in FlinkWriterState
            List<FlinkWriterState<WriterStateT>> wrappedStates = new ArrayList<>();
            if (states != null) {
                for (WriterStateT state : states) {
                    wrappedStates.add(new FlinkWriterState<>(checkpointId, state));
                }

                // Enhanced logging for schema evolution scenarios
                if (isMultiTableSink && !states.isEmpty()) {
                    log.debug(
                            "Multi-table sink snapshotted {} states for checkpointId: {} - schema evolution may be in progress",
                            states.size(),
                            checkpointId);
                }
            } else {
                log.debug(
                        "No states to snapshot for checkpointId: {} - this may be normal for schema evolution scenarios",
                        checkpointId);
            }

            log.debug(
                    "Snapshotted {} states for checkpointId: {}",
                    wrappedStates.size(),
                    checkpointId);

            // Update internal checkpoint ID for next checkpoint (similar to flink-common)
            // This is critical for maintaining transaction boundaries in schema evolution scenarios
            long previousCheckpointId = this.checkpointId;
            this.checkpointId = checkpointId + 1;

            log.debug(
                    "Updated internal checkpointId from {} to {} after snapshot",
                    previousCheckpointId,
                    this.checkpointId);

            return wrappedStates;
        } catch (Exception e) {
            log.error("Error during state snapshot for checkpointId: {}", checkpointId, e);
            throw new IOException("Failed to snapshot writer state", e);
        }
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }

        try {
            // Perform final flush before closing to ensure all data is committed
            log.debug("Performing final flush before closing sink writer");
            flush(true);
        } catch (Exception e) {
            log.warn("Error during final flush before close", e);
            // Continue with close even if flush fails
        }

        try {
            sinkWriter.close();
            context.getEventListener().onEvent(new WriterCloseEvent());
        } catch (Exception e) {
            log.error("Error closing sink writer: " + e.getMessage(), e);
        } finally {
            closed = true;
        }

        // Close resource manager
        try {
            if (resourceManager != null) {
                resourceManager.close();
            }
        } catch (Throwable e) {
            log.error("close resourceManager error", e);
        }
    }
}
