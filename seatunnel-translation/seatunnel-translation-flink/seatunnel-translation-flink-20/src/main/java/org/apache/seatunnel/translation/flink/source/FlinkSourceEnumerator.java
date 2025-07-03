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

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.event.EnumeratorCloseEvent;
import org.apache.seatunnel.api.source.event.EnumeratorOpenEvent;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The implementation of {@link SplitEnumerator}, used for proxy all {@link SourceSplitEnumerator}
 * in flink 1.20.
 *
 * @param <SplitT> The generic type of source split
 * @param <EnumStateT> The generic type of enumerator state
 */
@Slf4j
public class FlinkSourceEnumerator<SplitT extends SourceSplit, EnumStateT>
        implements SplitEnumerator<SplitWrapper<SplitT>, EnumStateT> {

    private final SourceSplitEnumerator<SplitT, EnumStateT> sourceSplitEnumerator;
    private final SplitEnumeratorContext<SplitWrapper<SplitT>> enumeratorContext;
    private final FlinkSourceSplitEnumeratorContext<SplitT> context;
    private final int parallelism;
    private final Object lock = new Object();
    private volatile boolean isRun = false;
    private volatile int currentRegisterReaders = 0;

    public FlinkSourceEnumerator(
            SourceSplitEnumerator<SplitT, EnumStateT> enumerator,
            FlinkSourceSplitEnumeratorContext<SplitT> context,
            int parallelism) {
        this.sourceSplitEnumerator = enumerator;
        this.enumeratorContext = context.getEnumContext();
        this.context = context;
        this.parallelism = parallelism;
        log.info("FlinkSourceEnumerator initialized with parallelism: {}", parallelism);
    }

    @Override
    public void start() {
        sourceSplitEnumerator.open();
        context.getEventListener().onEvent(new EnumeratorOpenEvent());
        log.info("SourceSplitEnumerator started");
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        sourceSplitEnumerator.handleSplitRequest(subtaskId);
        log.debug("Handling split request from subtask: {}", subtaskId);
    }

    @Override
    public void addSplitsBack(List<SplitWrapper<SplitT>> splits, int subtaskId) {
        log.info("Adding splits back from subtask: {}, splits count: {}", subtaskId, splits.size());
        sourceSplitEnumerator.addSplitsBack(
                splits.stream().map(SplitWrapper::getSourceSplit).collect(Collectors.toList()),
                subtaskId);
    }

    @Override
    public void addReader(int subtaskId) {
        log.info(
                "Adding reader: {}, current registered readers: {}",
                subtaskId,
                currentRegisterReaders);
        sourceSplitEnumerator.registerReader(subtaskId);
        synchronized (lock) {
            currentRegisterReaders++;
            if (!isRun && currentRegisterReaders == parallelism) {
                try {
                    log.info("All readers registered, running enumerator");
                    sourceSplitEnumerator.run();
                } catch (Exception e) {
                    log.error("Failed to run enumerator", e);
                    throw new RuntimeException(e);
                }
                isRun = true;
            }
        }
    }

    @Override
    public EnumStateT snapshotState(long checkpointId) throws Exception {
        log.debug("Snapshotting state for checkpoint: {}", checkpointId);
        return sourceSplitEnumerator.snapshotState(checkpointId);
    }

    @Override
    public void close() throws IOException {
        log.info("Closing enumerator");
        sourceSplitEnumerator.close();
        context.getEventListener().onEvent(new EnumeratorCloseEvent());
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof NoMoreElementEvent) {
            log.info(
                    "Received NoMoreElementEvent from reader [{}], total registered readers [{}]",
                    subtaskId,
                    enumeratorContext.currentParallelism());
            enumeratorContext.sendEventToSourceReader(subtaskId, sourceEvent);
        }
        if (sourceEvent instanceof SourceEventWrapper) {
            log.debug("Handling source event from subtask: {}", subtaskId);
            sourceSplitEnumerator.handleSourceEvent(
                    subtaskId, (((SourceEventWrapper) sourceEvent).getSourceEvent()));
        }
    }
}
