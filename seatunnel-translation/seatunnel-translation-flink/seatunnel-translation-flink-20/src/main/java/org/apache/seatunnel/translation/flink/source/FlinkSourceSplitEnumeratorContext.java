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

import org.apache.seatunnel.api.common.metrics.AbstractMetricsContext;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.DefaultEventProcessor;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.source.coordinator.SourceCoordinatorContext;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * The implementation of {@link SourceSplitEnumerator.Context}, used for proxy all {@link
 * SourceSplitEnumerator} in flink 1.20. Uses common module approach for JobId retrieval.
 *
 * @param <SplitT> The generic type of source split
 */
@Slf4j
public class FlinkSourceSplitEnumeratorContext<SplitT extends SourceSplit>
        implements SourceSplitEnumerator.Context<SplitT> {

    private final SplitEnumeratorContext<
                    org.apache.seatunnel.translation.flink.source.SplitWrapper<SplitT>>
            enumContext;
    protected final EventListener eventListener;

    public FlinkSourceSplitEnumeratorContext(
            SplitEnumeratorContext<
                            org.apache.seatunnel.translation.flink.source.SplitWrapper<SplitT>>
                    enumContext) {
        this.enumContext = enumContext;

        String jobId = getFlinkJobId(enumContext);
        this.eventListener = new DefaultEventProcessor(jobId);
        log.info("FlinkSourceSplitEnumeratorContext initialized with JobID: {}", jobId);
    }

    @Override
    public int currentParallelism() {
        return enumContext.currentParallelism();
    }

    @Override
    public Set<Integer> registeredReaders() {
        return enumContext.registeredReaders().keySet();
    }

    @Override
    public void assignSplit(int subtaskId, List<SplitT> splits) {
        splits.forEach(
                split -> {
                    enumContext.assignSplit(
                            new org.apache.seatunnel.translation.flink.source.SplitWrapper<>(split),
                            subtaskId);
                });
    }

    @Override
    public void signalNoMoreSplits(int subtask) {
        enumContext.signalNoMoreSplits(subtask);
    }

    @Override
    public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
        enumContext.sendEventToSourceReader(
                subtaskId,
                new org.apache.seatunnel.translation.flink.source.SourceEventWrapper(event));
    }

    @Override
    public MetricsContext getMetricsContext() {
        return new AbstractMetricsContext() {};
    }

    @Override
    public EventListener getEventListener() {
        return eventListener;
    }

    public SplitEnumeratorContext<
                    org.apache.seatunnel.translation.flink.source.SplitWrapper<SplitT>>
            getEnumContext() {
        return enumContext;
    }

    private static String getFlinkJobId(SplitEnumeratorContext enumContext) {
        try {
            String jobId = getJobIdUsingCommonApproach(enumContext);
            if (jobId != null) {
                return jobId;
            }
            return "generated-" + UUID.randomUUID().toString();
        } catch (Exception e) {
            log.warn("Failed to get JobId: {}", e.getMessage());
            return "generated-" + UUID.randomUUID().toString();
        }
    }

    private static String getJobIdUsingCommonApproach(SplitEnumeratorContext enumContext) {
        try {
            if (!(enumContext instanceof SourceCoordinatorContext)) {
                return null;
            }

            SourceCoordinatorContext coordinatorContext = (SourceCoordinatorContext) enumContext;
            Field field =
                    coordinatorContext.getClass().getDeclaredField("operatorCoordinatorContext");
            field.setAccessible(true);
            OperatorCoordinator.Context operatorCoordinatorContext =
                    (OperatorCoordinator.Context) field.get(coordinatorContext);

            // Follow the exact common module logic
            Field[] fields = operatorCoordinatorContext.getClass().getDeclaredFields();
            Optional<Field> fieldOptional =
                    Arrays.stream(fields)
                            .filter(f -> f.getName().equals("globalFailureHandler"))
                            .findFirst();

            if (!fieldOptional.isPresent()) {
                // RecreateOnResetOperatorCoordinator.QuiesceableContext case
                fieldOptional =
                        Arrays.stream(fields)
                                .filter(f -> f.getName().equals("context"))
                                .findFirst();

                if (fieldOptional.isPresent()) {
                    field = fieldOptional.get();
                    field.setAccessible(true);
                    operatorCoordinatorContext =
                            (OperatorCoordinator.Context) field.get(operatorCoordinatorContext);
                } else {
                    return null;
                }
            }

            // OperatorCoordinatorHolder.LazyInitializedCoordinatorContext
            field =
                    Arrays.stream(operatorCoordinatorContext.getClass().getDeclaredFields())
                            .filter(f -> f.getName().equals("globalFailureHandler"))
                            .findFirst()
                            .orElse(null);

            if (field == null) {
                return null;
            }

            field.setAccessible(true);
            Object globalFailureHandler = field.get(operatorCoordinatorContext);

            // SchedulerBase$xxx
            Field[] handlerFields = globalFailureHandler.getClass().getDeclaredFields();
            field =
                    Arrays.stream(handlerFields)
                            .filter(f -> f.getName().equals("arg$1"))
                            .findFirst()
                            .orElse(null);

            if (field == null) {
                return null;
            }

            field.setAccessible(true);
            Object schedulerBase = field.get(globalFailureHandler);

            Method getExecutionGraphMethod =
                    schedulerBase.getClass().getMethod("getExecutionGraph");
            Object executionGraph = getExecutionGraphMethod.invoke(schedulerBase);

            Method getJobIDMethod = executionGraph.getClass().getMethod("getJobID");
            Object jobID = getJobIDMethod.invoke(executionGraph);

            return jobID.toString();
        } catch (Exception e) {
            return null;
        }
    }
}
