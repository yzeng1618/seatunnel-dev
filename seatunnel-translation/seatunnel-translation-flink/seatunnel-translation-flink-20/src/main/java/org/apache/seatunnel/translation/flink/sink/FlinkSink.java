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

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.flink.serialization.CommitWrapperSerializer;
import org.apache.seatunnel.translation.flink.serialization.EmptyFlinkWriterStateSerializer;
import org.apache.seatunnel.translation.flink.serialization.FlinkWriterStateSerializer;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.SupportsWriterState;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class FlinkSink<CommT, WriterStateT, GlobalCommT>
        implements Sink<SeaTunnelRow>,
                SupportsCommitter<CommitWrapper<CommT>>,
                SupportsWriterState<SeaTunnelRow, FlinkWriterState<WriterStateT>> {

    private final SeaTunnelSink<SeaTunnelRow, WriterStateT, CommT, GlobalCommT> seaTunnelSink;
    private final List<CatalogTable> catalogTables;
    private final int parallelism;

    @SuppressWarnings("unchecked")
    public FlinkSink(
            SeaTunnelSink<?, ?, ?, ?> seaTunnelSink,
            List<CatalogTable> catalogTables,
            int parallelism) {
        this.seaTunnelSink =
                (SeaTunnelSink<SeaTunnelRow, WriterStateT, CommT, GlobalCommT>) seaTunnelSink;
        this.catalogTables = catalogTables;
        this.parallelism = parallelism;
        log.info("FlinkSink initialized with parallelism: {}", parallelism);
    }

    @Override
    public SinkWriter<SeaTunnelRow> createWriter(Sink.InitContext initContext) throws IOException {
        // This is the deprecated method that we must implement
        // We'll delegate to the WriterInitContext version by wrapping the context
        if (initContext instanceof WriterInitContext) {
            return createWriter((WriterInitContext) initContext);
        } else {
            throw new UnsupportedOperationException(
                    "createWriter(InitContext) requires WriterInitContext in this implementation");
        }
    }

    @Override
    public SinkWriter<SeaTunnelRow> createWriter(WriterInitContext context) throws IOException {
        log.info("Creating FlinkSinkWriter with context: {}", context);
        FlinkSinkWriterContext writerContext = new FlinkSinkWriterContext(context, parallelism);

        org.apache.seatunnel.api.sink.SinkWriter<SeaTunnelRow, CommT, WriterStateT>
                seatunnelWriter = seaTunnelSink.createWriter(writerContext);

        return new FlinkSinkWriter<>(seatunnelWriter, context, writerContext);
    }

    @Override
    public Committer<CommitWrapper<CommT>> createCommitter(CommitterInitContext context)
            throws IOException {
        log.debug("Creating FlinkCommitter with context: {}", context);

        try {
            // Try to create SinkCommitter first
            if (seaTunnelSink.createCommitter().isPresent()) {
                org.apache.seatunnel.api.sink.SinkCommitter<CommT> sinkCommitter =
                        seaTunnelSink.createCommitter().get();
                if (sinkCommitter != null) {
                    log.info(
                            "Created FlinkCommitter with SinkCommitter: {}",
                            sinkCommitter.getClass().getSimpleName());
                    return new FlinkCommitter<>(sinkCommitter);
                } else {
                    log.warn("SinkCommitter is null, cannot create FlinkCommitter");
                }
            }

            // If no SinkCommitter, try SinkAggregatedCommitter
            // Note: Flink 2.0 sink2 API doesn't support GlobalCommitter,
            // so we handle SinkAggregatedCommitter through regular Committer adapter
            if (seaTunnelSink.createAggregatedCommitter().isPresent()) {
                org.apache.seatunnel.api.sink.SinkAggregatedCommitter<CommT, ?>
                        aggregatedCommitter = seaTunnelSink.createAggregatedCommitter().get();
                if (aggregatedCommitter != null) {
                    log.info(
                            "Created FlinkAggregatedCommitterAdapter with SinkAggregatedCommitter: {}",
                            aggregatedCommitter.getClass().getSimpleName());
                    log.warn(
                            "Using AggregatedCommitter through adapter - some features may be limited in Flink 2.0");
                    return new FlinkAggregatedCommitterAdapter<>(aggregatedCommitter);
                }
            }

            log.debug(
                    "No committer available for sink: {}",
                    seaTunnelSink.getClass().getSimpleName());
            return null;
        } catch (Exception e) {
            log.error("Error creating FlinkCommitter: {}", e.getMessage(), e);
            throw new IOException("Failed to create FlinkCommitter", e);
        }
    }

    @Override
    public SimpleVersionedSerializer<CommitWrapper<CommT>> getCommittableSerializer() {
        log.debug("Getting committable serializer");
        try {
            // Based on flink-common implementation: return serializer if any committer exists
            if (seaTunnelSink.createCommitter().isPresent()
                    || seaTunnelSink.createAggregatedCommitter().isPresent()) {

                Optional<Serializer<CommT>> serializerOpt = seaTunnelSink.getCommitInfoSerializer();
                if (serializerOpt.isPresent()) {
                    return new CommitWrapperSerializer<>(serializerOpt.get());
                } else {
                    log.debug("No commit info serializer available, using default");
                    return new CommitWrapperSerializer<>();
                }
            } else {
                log.debug("No committer available, returning null serializer");
                return null;
            }
        } catch (IOException e) {
            log.error("Error getting committable serializer: {}", e.getMessage(), e);
            // Fallback to default serializer to avoid job failure
            return new CommitWrapperSerializer<>();
        }
    }

    // SupportsWriterState interface methods
    @Override
    public StatefulSinkWriter<SeaTunnelRow, FlinkWriterState<WriterStateT>> restoreWriter(
            WriterInitContext context, Collection<FlinkWriterState<WriterStateT>> recoveredState)
            throws IOException {
        log.info("Restoring FlinkSinkWriter with {} recovered states", recoveredState.size());
        FlinkSinkWriterContext writerContext = new FlinkSinkWriterContext(context, parallelism);

        if (recoveredState == null || recoveredState.isEmpty()) {
            // No state to restore, create new writer
            org.apache.seatunnel.api.sink.SinkWriter<SeaTunnelRow, CommT, WriterStateT>
                    seatunnelWriter = seaTunnelSink.createWriter(writerContext);
            return new FlinkSinkWriter<>(seatunnelWriter, context, writerContext);
        } else {
            // Restore from state
            List<WriterStateT> states =
                    recoveredState.stream()
                            .map(FlinkWriterState::getState)
                            .collect(Collectors.toList());

            org.apache.seatunnel.api.sink.SinkWriter<SeaTunnelRow, CommT, WriterStateT>
                    seatunnelWriter = seaTunnelSink.restoreWriter(writerContext, states);

            // Use the checkpoint ID from the first recovered state
            long checkpointId = recoveredState.iterator().next().getCheckpointId() + 1;
            return new FlinkSinkWriter<>(seatunnelWriter, context, writerContext, checkpointId);
        }
    }

    @Override
    public SimpleVersionedSerializer<FlinkWriterState<WriterStateT>> getWriterStateSerializer() {
        log.debug("Getting writer state serializer");
        if (seaTunnelSink.getWriterStateSerializer().isPresent()) {
            return new FlinkWriterStateSerializer<>(seaTunnelSink.getWriterStateSerializer().get());
        } else {
            return new EmptyFlinkWriterStateSerializer<>();
        }
    }
}
