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

package org.apache.seatunnel.translation.flink.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;

/**
 * The source implementation of {@link Source}, used for proxy all {@link SeaTunnelSource} in flink
 * 1.20.
 *
 * @param <SplitT> The generic type of source split
 * @param <EnumStateT> The generic type of enumerator state
 */
@Slf4j
public class FlinkSource<SplitT extends SourceSplit, EnumStateT extends Serializable>
        implements Source<SeaTunnelRow, SplitWrapper<SplitT>, EnumStateT>,
                ResultTypeQueryable<SeaTunnelRow> {

    private final SeaTunnelSource<SeaTunnelRow, SplitT, EnumStateT> source;
    private final Config envConfig;

    public FlinkSource(SeaTunnelSource<SeaTunnelRow, SplitT, EnumStateT> source, Config envConfig) {
        this.source = source;
        this.envConfig = envConfig;
        log.info("FlinkSource initialized with source: {}", source.getClass().getName());
    }

    @Override
    public Boundedness getBoundedness() {
        org.apache.seatunnel.api.source.Boundedness boundedness = source.getBoundedness();
        return boundedness == org.apache.seatunnel.api.source.Boundedness.BOUNDED
                ? Boundedness.BOUNDED
                : Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<SeaTunnelRow, SplitWrapper<SplitT>> createReader(
            SourceReaderContext readerContext) throws Exception {
        log.info("Creating SourceReader for subtask: {}", readerContext.getIndexOfSubtask());
        org.apache.seatunnel.api.source.SourceReader.Context context =
                new FlinkSourceReaderContext(readerContext, source);
        org.apache.seatunnel.api.source.SourceReader<SeaTunnelRow, SplitT> reader =
                source.createReader(context);

        // 使用common模块中的FlinkSourceReader类
        return new org.apache.seatunnel.translation.flink.source.FlinkSourceReader<>(
                reader, context, envConfig);
    }

    @Override
    public SplitEnumerator<SplitWrapper<SplitT>, EnumStateT> createEnumerator(
            SplitEnumeratorContext<SplitWrapper<SplitT>> enumContext) throws Exception {
        log.info("Creating SplitEnumerator");
        FlinkSourceSplitEnumeratorContext<SplitT> sourceEnumContext =
                new FlinkSourceSplitEnumeratorContext<>(enumContext);
        SourceSplitEnumerator<SplitT, EnumStateT> enumerator =
                source.createEnumerator(sourceEnumContext);
        return new FlinkSourceEnumerator<>(
                enumerator, sourceEnumContext, enumContext.currentParallelism());
    }

    @Override
    public SplitEnumerator<SplitWrapper<SplitT>, EnumStateT> restoreEnumerator(
            SplitEnumeratorContext<SplitWrapper<SplitT>> enumContext, EnumStateT checkpoint)
            throws Exception {
        log.info("Restoring SplitEnumerator from checkpoint");
        FlinkSourceSplitEnumeratorContext<SplitT> sourceEnumContext =
                new FlinkSourceSplitEnumeratorContext<>(enumContext);
        SourceSplitEnumerator<SplitT, EnumStateT> enumerator =
                source.restoreEnumerator(sourceEnumContext, checkpoint);
        return new FlinkSourceEnumerator<>(
                enumerator, sourceEnumContext, enumContext.currentParallelism());
    }

    @Override
    public SimpleVersionedSerializer<SplitWrapper<SplitT>> getSplitSerializer() {
        return new SplitWrapperSerializer<>(source.getSplitSerializer());
    }

    @Override
    public SimpleVersionedSerializer<EnumStateT> getEnumeratorCheckpointSerializer() {
        Serializer<EnumStateT> enumeratorStateSerializer = source.getEnumeratorStateSerializer();
        return new EnumeratorStateSerializer<>(enumeratorStateSerializer);
    }

    @Override
    public TypeInformation<SeaTunnelRow> getProducedType() {
        return TypeInformation.of(SeaTunnelRow.class);
    }
}
