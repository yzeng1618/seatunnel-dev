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

package org.apache.seatunnel.translation.flink.metric;

import org.apache.seatunnel.api.common.metrics.MetricNames;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.StringFormatUtils;

import org.apache.flink.api.common.JobExecutionResult;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.LocalDateTime;

@Slf4j
public final class FlinkJobMetricsSummary {

    private final JobExecutionResult jobExecutionResult;

    private final LocalDateTime jobStartTime;

    private final LocalDateTime jobEndTime;

    FlinkJobMetricsSummary(
            JobExecutionResult jobExecutionResult,
            LocalDateTime jobStartTime,
            LocalDateTime jobEndTime) {
        this.jobExecutionResult = jobExecutionResult;
        this.jobStartTime = jobStartTime;
        this.jobEndTime = jobEndTime;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private JobExecutionResult jobExecutionResult;

        private long jobStartTime;

        private long jobEndTime;

        private Builder() {}

        public Builder jobExecutionResult(JobExecutionResult jobExecutionResult) {
            this.jobExecutionResult = jobExecutionResult;
            return this;
        }

        public Builder jobStartTime(long jobStartTime) {
            this.jobStartTime = jobStartTime;
            return this;
        }

        public Builder jobEndTime(long jobEndTime) {
            this.jobEndTime = jobEndTime;
            return this;
        }

        public FlinkJobMetricsSummary build() {
            return new FlinkJobMetricsSummary(
                    jobExecutionResult,
                    DateTimeUtils.parse(jobStartTime),
                    DateTimeUtils.parse(jobEndTime));
        }
    }

    @Override
    public String toString() {
        // 获取指标值，如果为null则使用0
        Object sourceReceivedCount =
                jobExecutionResult
                        .getAllAccumulatorResults()
                        .getOrDefault(MetricNames.SOURCE_RECEIVED_COUNT, 0L);

        Object sinkWriteCount =
                jobExecutionResult
                        .getAllAccumulatorResults()
                        .getOrDefault(MetricNames.SINK_WRITE_COUNT, 0L);

        Object sourceReceivedBytes =
                jobExecutionResult
                        .getAllAccumulatorResults()
                        .getOrDefault(MetricNames.SOURCE_RECEIVED_BYTES, 0L);

        Object sinkWriteBytes =
                jobExecutionResult
                        .getAllAccumulatorResults()
                        .getOrDefault(MetricNames.SINK_WRITE_BYTES, 0L);

        // 检查指标是否为null，如果是则记录警告并使用0
        if (sourceReceivedCount == null) {
            log.warn("SOURCE_RECEIVED_COUNT is null, using 0");
            sourceReceivedCount = 0L;
        }

        if (sinkWriteCount == null) {
            log.warn("SINK_WRITE_COUNT is null, using 0");
            sinkWriteCount = 0L;
        }

        if (sourceReceivedBytes == null) {
            log.warn("SOURCE_RECEIVED_BYTES is null, using 0");
            sourceReceivedBytes = 0L;
        }

        if (sinkWriteBytes == null) {
            log.warn("SINK_WRITE_BYTES is null, using 0");
            sinkWriteBytes = 0L;
        }

        return StringFormatUtils.formatTable(
                "Job Statistic Information",
                "Start Time",
                DateTimeUtils.toString(jobStartTime, DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS),
                "End Time",
                DateTimeUtils.toString(jobEndTime, DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS),
                "Total Time(s)",
                Duration.between(jobStartTime, jobEndTime).getSeconds(),
                "Total Read Count",
                jobExecutionResult
                        .getAllAccumulatorResults()
                        .get(MetricNames.SOURCE_RECEIVED_COUNT),
                "Total Write Count",
                jobExecutionResult.getAllAccumulatorResults().get(MetricNames.SINK_WRITE_COUNT),
                "Total Read Bytes",
                jobExecutionResult
                        .getAllAccumulatorResults()
                        .get(MetricNames.SOURCE_RECEIVED_BYTES),
                "Total Write Bytes",
                jobExecutionResult.getAllAccumulatorResults().get(MetricNames.SINK_WRITE_BYTES));
    }
}
