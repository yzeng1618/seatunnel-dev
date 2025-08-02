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

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class FlinkSinkWriter
        implements org.apache.flink.api.connector.sink2.SinkWriter<SeaTunnelRow> {

    private final SinkWriter<SeaTunnelRow, ?, ?> sinkWriter;
    private final Counter sinkWriteCount;
    private final Counter sinkWriteBytes;
    private volatile boolean closed = false;

    public FlinkSinkWriter(
            SinkWriter<SeaTunnelRow, ?, ?> sinkWriter,
            Sink.InitContext context,
            MetricsContext metricsContext) {
        this.sinkWriter = sinkWriter;
        this.sinkWriteCount = metricsContext.counter(MetricNames.SINK_WRITE_COUNT);
        this.sinkWriteBytes = metricsContext.counter(MetricNames.SINK_WRITE_BYTES);
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
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (endOfInput) {
            // 使用同步块确保只关闭一次
            synchronized (this) {
                if (!closed) {
                    sinkWriter.close();
                    closed = true;
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        // 使用同步块确保只关闭一次
        synchronized (this) {
            if (!closed) {
                sinkWriter.close();
                closed = true;
            }
        }
    }
}
