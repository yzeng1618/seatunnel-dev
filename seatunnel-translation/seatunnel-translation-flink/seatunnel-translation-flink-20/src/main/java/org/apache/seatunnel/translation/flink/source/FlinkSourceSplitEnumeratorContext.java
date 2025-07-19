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
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.source.coordinator.SourceCoordinatorContext;

import org.slf4j.MDC;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * The implementation of {@link org.apache.seatunnel.api.source.SourceSplitEnumerator.Context} for
 * flink 1.20 engine.
 *
 * @param <SplitT>
 */
@Slf4j
public class FlinkSourceSplitEnumeratorContext<SplitT extends SourceSplit>
        implements SourceSplitEnumerator.Context<SplitT> {

    private final SplitEnumeratorContext<SplitWrapper<SplitT>> enumContext;
    protected final EventListener eventListener;

    public FlinkSourceSplitEnumeratorContext(
            SplitEnumeratorContext<SplitWrapper<SplitT>> enumContext) {
        this.enumContext = enumContext;

        String jobId = null;
        try {
            jobId = getFlinkJobId(enumContext);
        } catch (Exception e) {
            log.warn("Failed to get Flink JobID, event processing may be limited", e);
        }

        if (jobId == null || jobId.equals("unknown-job-id")) {
            jobId = "generated-" + UUID.randomUUID().toString();
            log.info("Using generated JobID: {}", jobId);
        }

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
        log.info("Assigning {} splits to subtask: {}", splits.size(), subtaskId);
        splits.forEach(
                split -> {
                    enumContext.assignSplit(new SplitWrapper<>(split), subtaskId);
                });
    }

    @Override
    public void signalNoMoreSplits(int subtask) {
        log.info("Signaling no more splits to subtask: {}", subtask);
        enumContext.signalNoMoreSplits(subtask);
    }

    @Override
    public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
        log.debug("Sending event to source reader: {}", subtaskId);
        enumContext.sendEventToSourceReader(subtaskId, new SourceEventWrapper(event));
    }

    @Override
    public MetricsContext getMetricsContext() {
        return new AbstractMetricsContext() {};
    }

    @Override
    public EventListener getEventListener() {
        return eventListener;
    }

    public SplitEnumeratorContext<SplitWrapper<SplitT>> getEnumContext() {
        return enumContext;
    }

    private static String getFlinkJobId(SplitEnumeratorContext enumContext) {
        try {
            String jobId = getJobIdForFlink20(enumContext);
            if (jobId != null && !jobId.equals("unknown-job-id")) {
                log.info("Successfully retrieved JobID: {}", jobId);
                return jobId;
            }
            log.warn("Could not retrieve JobID using primary method");
            return null;
        } catch (Exception e) {
            log.warn("Get flink job id failed: {}", e.getMessage());
            return null;
        }
    }

    private static String getJobIdForFlink20(SplitEnumeratorContext enumContext) {
        try {
            if (enumContext instanceof SourceCoordinatorContext) {
                SourceCoordinatorContext coordinatorContext =
                        (SourceCoordinatorContext) enumContext;

                Field field =
                        coordinatorContext
                                .getClass()
                                .getDeclaredField("operatorCoordinatorContext");
                field.setAccessible(true);
                OperatorCoordinator.Context operatorCoordinatorContext =
                        (OperatorCoordinator.Context) field.get(coordinatorContext);

                try {
                    OperatorID operatorID = operatorCoordinatorContext.getOperatorId();
                    if (operatorID != null) {
                        String operatorIdStr = operatorID.toString();
                        if (operatorIdStr.contains("_")) {
                            return operatorIdStr.split("_")[0];
                        }
                    }
                } catch (Exception e) {
                    log.debug("Failed to get JobID from OperatorID: {}", e.getMessage());
                }

                try {
                    CheckpointCoordinator checkpointCoordinator =
                            operatorCoordinatorContext.getCheckpointCoordinator();
                    if (checkpointCoordinator != null) {
                        Field jobField = checkpointCoordinator.getClass().getDeclaredField("job");
                        jobField.setAccessible(true);
                        Object job = jobField.get(checkpointCoordinator);
                        if (job != null) {
                            Method getJobIdMethod = job.getClass().getMethod("getJobID");
                            Object jobId = getJobIdMethod.invoke(job);
                            if (jobId != null) {
                                return jobId.toString();
                            }
                        }
                    }
                } catch (Exception e) {
                    log.debug("Failed to get JobID from CheckpointCoordinator: {}", e.getMessage());
                }
            }

            String threadName = Thread.currentThread().getName();
            if (threadName.contains("jobmanager-job_")) {
                int startIndex = threadName.indexOf("jobmanager-job_") + "jobmanager-job_".length();
                int endIndex = threadName.indexOf("_", startIndex);
                if (endIndex > startIndex) {
                    return threadName.substring(startIndex, endIndex);
                }
            }

            try {
                String mdcJobId = MDC.get("flink.jobId");
                if (mdcJobId != null && !mdcJobId.isEmpty()) {
                    return mdcJobId;
                }
            } catch (Exception e) {
                log.debug("Failed to get JobID from MDC: {}", e.getMessage());
            }

            log.info("Could not determine JobID from context, using fallback value");
            return "unknown-job-id";
        } catch (Exception e) {
            log.error("Failed to get JobID", e);
            return "unknown-job-id";
        }
    }
}
