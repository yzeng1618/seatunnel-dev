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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;

@Slf4j
public class FlinkSink implements Sink<SeaTunnelRow> {

    private final SeaTunnelSink<SeaTunnelRow, ?, ?, ?> seaTunnelSink;
    private final List<CatalogTable> catalogTables;
    private final int parallelism;

    @SuppressWarnings("unchecked")
    public FlinkSink(
            SeaTunnelSink<?, ?, ?, ?> seaTunnelSink,
            List<CatalogTable> catalogTables,
            int parallelism) {
        this.seaTunnelSink = (SeaTunnelSink<SeaTunnelRow, ?, ?, ?>) seaTunnelSink;
        this.catalogTables = catalogTables;
        this.parallelism = parallelism;
        log.info("FlinkSink initialized with parallelism: {}", parallelism);
    }

    @Override
    public SinkWriter<SeaTunnelRow> createWriter(InitContext context) throws IOException {
        log.info("Creating FlinkSinkWriter with context: {}", context);
        FlinkSinkWriterContext writerContext = new FlinkSinkWriterContext(context, parallelism);

        // 创建SeaTunnel SinkWriter
        org.apache.seatunnel.api.sink.SinkWriter<SeaTunnelRow, ?, ?> seatunnelWriter =
                seaTunnelSink.createWriter(writerContext);

        // 传递context和MetricsContext给FlinkSinkWriter
        return new FlinkSinkWriter(seatunnelWriter, context, writerContext.getMetricsContext());
    }
}
