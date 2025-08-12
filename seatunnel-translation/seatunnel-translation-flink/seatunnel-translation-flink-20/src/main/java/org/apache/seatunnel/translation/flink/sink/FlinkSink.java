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

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.flink.serialization.CommitWrapperSerializer;
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
import java.util.stream.Collectors;

/**
 * The sink implementation for Flink 1.20+ using sink2 API, adapted from common module with full
 * state management and committer support.
 *
 * @param <InputT> The generic type of input data
 * @param <CommT> The generic type of commit message
 * @param <WriterStateT> The generic type of writer state
 * @param <GlobalCommT> The generic type of global commit message
 */
@Slf4j
public class FlinkSink<InputT, CommT, WriterStateT, GlobalCommT>
        implements Sink<InputT>,
                SupportsWriterState<InputT, FlinkWriterState<WriterStateT>>,
                SupportsCommitter<CommitWrapper<CommT>> {

    private final SeaTunnelSink<SeaTunnelRow, WriterStateT, CommT, GlobalCommT> sink;
    private final List<CatalogTable> catalogTables;
    private final int parallelism;

    public FlinkSink(
            SeaTunnelSink<SeaTunnelRow, WriterStateT, CommT, GlobalCommT> sink,
            List<CatalogTable> catalogTables,
            int parallelism) {
        this.sink = sink;
        this.catalogTables = catalogTables;
        this.parallelism = parallelism;
        log.info("FlinkSink initialized with parallelism: {} for Flink 1.20+", parallelism);
    }

    @Override
    public SinkWriter<InputT> createWriter(InitContext initContext) throws IOException {
        log.info("Creating FlinkSinkWriter with InitContext for Flink 1.20+");
        org.apache.seatunnel.api.sink.SinkWriter.Context stContext =
                new FlinkSinkWriterContext(initContext, parallelism);

        org.apache.seatunnel.api.sink.SinkWriter<SeaTunnelRow, CommT, WriterStateT>
                seatunnelWriter = sink.createWriter(stContext);

        return new FlinkSinkWriter<>(seatunnelWriter, 1, stContext);
    }

    @Override
    public SinkWriter<InputT> createWriter(WriterInitContext context) throws IOException {
        log.info("Creating FlinkSinkWriter with WriterInitContext for Flink 1.20+");
        org.apache.seatunnel.api.sink.SinkWriter.Context stContext =
                new FlinkSinkWriterContext(context, parallelism);

        org.apache.seatunnel.api.sink.SinkWriter<SeaTunnelRow, CommT, WriterStateT>
                seatunnelWriter = sink.createWriter(stContext);

        return new FlinkSinkWriter<>(seatunnelWriter, 1, stContext);
    }

    @Override
    public StatefulSinkWriter<InputT, FlinkWriterState<WriterStateT>> restoreWriter(
            WriterInitContext context, Collection<FlinkWriterState<WriterStateT>> recoveredState)
            throws IOException {
        log.info(
                "Restoring FlinkSinkWriter with {} recovered states for Flink 1.20+",
                recoveredState.size());
        org.apache.seatunnel.api.sink.SinkWriter.Context stContext =
                new FlinkSinkWriterContext(context, parallelism);

        if (recoveredState == null || recoveredState.isEmpty()) {
            org.apache.seatunnel.api.sink.SinkWriter<SeaTunnelRow, CommT, WriterStateT>
                    seatunnelWriter = sink.createWriter(stContext);
            return new FlinkSinkWriter<>(seatunnelWriter, 1, stContext);
        } else {
            List<WriterStateT> restoredState =
                    recoveredState.stream()
                            .map(FlinkWriterState::getState)
                            .collect(Collectors.toList());
            org.apache.seatunnel.api.sink.SinkWriter<SeaTunnelRow, CommT, WriterStateT>
                    seatunnelWriter = sink.restoreWriter(stContext, restoredState);

            long maxCheckpointId =
                    recoveredState.stream()
                            .mapToLong(FlinkWriterState::getCheckpointId)
                            .max()
                            .orElse(0L);

            return new FlinkSinkWriter<>(seatunnelWriter, maxCheckpointId + 1, stContext);
        }
    }

    @Override
    public Committer<CommitWrapper<CommT>> createCommitter(CommitterInitContext context)
            throws IOException {
        log.debug("Creating Committer for Flink 1.20+");
        return sink.createCommitter()
                .<Committer<CommitWrapper<CommT>>>map(FlinkCommitter20::new)
                .orElse(null);
    }

    @Override
    public SimpleVersionedSerializer<FlinkWriterState<WriterStateT>> getWriterStateSerializer() {
        log.debug("Getting writer state serializer for Flink 1.20+");
        if (sink.getWriterStateSerializer().isPresent()) {
            return new FlinkWriterStateSerializer<>(sink.getWriterStateSerializer().get());
        } else {
            return new EmptyWriterStateSerializer<WriterStateT>();
        }
    }



    @Override
    public SimpleVersionedSerializer<CommitWrapper<CommT>> getCommittableSerializer() {
        log.debug("Getting committable serializer for Flink 1.20+");
        try {
            if (sink.createCommitter().isPresent()
                    || sink.createAggregatedCommitter().isPresent()) {
                return sink.getCommitInfoSerializer()
                        .map(CommitWrapperSerializer::new)
                        .orElse(null);
            } else {
                return null;
            }
        } catch (IOException e) {
            log.error("Failed to create Committer or AggregatedCommitter", e);
            throw new RuntimeException("Failed to create Committer or AggregatedCommitter", e);
        }
    }
}
