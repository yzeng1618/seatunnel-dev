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
import java.util.HashMap;
import java.util.Map;

/** Flink 1.20 专用的作业指标摘要类。 处理 Flink 1.20 中的 Accumulator 结果，并处理 null 值。 */
@Slf4j
public class FlinkJobMetricsSummary {

    private final JobExecutionResult jobExecutionResult;
    private final LocalDateTime jobStartTime;
    private final LocalDateTime jobEndTime;

    public FlinkJobMetricsSummary(
            JobExecutionResult jobExecutionResult,
            LocalDateTime jobStartTime,
            LocalDateTime jobEndTime) {
        this.jobExecutionResult = jobExecutionResult;
        this.jobStartTime = jobStartTime;
        this.jobEndTime = jobEndTime;
    }

    /**
     * 创建一个 FlinkJobMetricsSummary 的 Builder
     *
     * @return FlinkJobMetricsSummary.Builder 实例
     */
    public static Builder builder() {
        return new Builder();
    }

    /** FlinkJobMetricsSummary 的 Builder 类 */
    public static class Builder {
        private JobExecutionResult jobExecutionResult;
        private long jobStartTime;
        private long jobEndTime;

        private Builder() {}

        /**
         * 设置作业执行结果
         *
         * @param jobExecutionResult 作业执行结果
         * @return Builder 实例
         */
        public Builder jobExecutionResult(JobExecutionResult jobExecutionResult) {
            this.jobExecutionResult = jobExecutionResult;
            return this;
        }

        /**
         * 设置作业开始时间
         *
         * @param jobStartTime 作业开始时间（毫秒时间戳）
         * @return Builder 实例
         */
        public Builder jobStartTime(long jobStartTime) {
            this.jobStartTime = jobStartTime;
            return this;
        }

        /**
         * 设置作业结束时间
         *
         * @param jobEndTime 作业结束时间（毫秒时间戳）
         * @return Builder 实例
         */
        public Builder jobEndTime(long jobEndTime) {
            this.jobEndTime = jobEndTime;
            return this;
        }

        /**
         * 构建 FlinkJobMetricsSummary 实例
         *
         * @return FlinkJobMetricsSummary 实例
         */
        public FlinkJobMetricsSummary build() {
            return new FlinkJobMetricsSummary(
                    jobExecutionResult,
                    DateTimeUtils.parse(jobStartTime),
                    DateTimeUtils.parse(jobEndTime));
        }
    }

    @Override
    public String toString() {
        Map<String, Object> accumulatorResults = jobExecutionResult.getAllAccumulatorResults();

        log.info("Available accumulators: {}", accumulatorResults.keySet());
        accumulatorResults.forEach(
                (key, value) ->
                        log.info(
                                "Accumulator [{}] = {} (type: {})",
                                key,
                                value,
                                (value != null ? value.getClass().getName() : "null")));

        // 尝试从 FlinkGroupCounter.COUNTER_VALUES 获取指标值
        Map<String, Long> counterValues = new HashMap<>();
        try {
            // 获取 FlinkGroupCounter.COUNTER_VALUES 字段
            Class<?> flinkGroupCounterClass =
                    Class.forName(
                            "org.apache.seatunnel.translation.flink.metric.FlinkGroupCounter");
            java.lang.reflect.Field counterValuesField =
                    flinkGroupCounterClass.getDeclaredField("COUNTER_VALUES");
            counterValuesField.setAccessible(true);
            Map<String, Long> values = (Map<String, Long>) counterValuesField.get(null);

            if (values != null) {
                counterValues.putAll(values);
                log.info("Retrieved {} counter values from FlinkGroupCounter", values.size());
            }
        } catch (Exception e) {
            log.warn(
                    "Failed to retrieve counter values from FlinkGroupCounter: {}", e.getMessage());
        }

        // 获取指标值，优先使用 counterValues，然后是 accumulatorResults
        long sourceReceivedCount =
                getCounterValueOrDefault(
                        counterValues, accumulatorResults, MetricNames.SOURCE_RECEIVED_COUNT, 0L);

        long sinkWriteCount =
                getCounterValueOrDefault(
                        counterValues, accumulatorResults, MetricNames.SINK_WRITE_COUNT, 0L);

        long sourceReceivedBytes =
                getCounterValueOrDefault(
                        counterValues, accumulatorResults, MetricNames.SOURCE_RECEIVED_BYTES, 0L);

        long sinkWriteBytes =
                getCounterValueOrDefault(
                        counterValues, accumulatorResults, MetricNames.SINK_WRITE_BYTES, 0L);

        return StringFormatUtils.formatTable(
                "Job Statistic Information",
                "Start Time",
                DateTimeUtils.toString(jobStartTime, DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS),
                "End Time",
                DateTimeUtils.toString(jobEndTime, DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS),
                "Total Time(s)",
                Duration.between(jobStartTime, jobEndTime).getSeconds(),
                "Total Read Count",
                sourceReceivedCount,
                "Total Write Count",
                sinkWriteCount,
                "Total Read Bytes",
                sourceReceivedBytes,
                "Total Write Bytes",
                sinkWriteBytes);
    }

    /** 获取计数器值，优先使用 counterValues，然后是 accumulatorResults */
    private long getCounterValueOrDefault(
            Map<String, Long> counterValues,
            Map<String, Object> accumulatorResults,
            String name,
            long defaultValue) {

        // 首先尝试从 counterValues 获取
        Long counterValue = counterValues.get(name);
        if (counterValue != null) {
            log.info("Using counter value for {}: {}", name, counterValue);
            return counterValue;
        }

        // 然后尝试从 accumulatorResults 获取
        Object accValue = accumulatorResults.get(name);
        if (accValue == null) {
            log.warn(
                    "Counter {} not found in either source, using default value: {}",
                    name,
                    defaultValue);
            return defaultValue;
        }

        if (accValue instanceof Number) {
            return ((Number) accValue).longValue();
        }

        try {
            return Long.parseLong(accValue.toString());
        } catch (NumberFormatException e) {
            log.warn(
                    "Failed to parse counter {} value: {}, using default: {}",
                    name,
                    accValue,
                    defaultValue);
            return defaultValue;
        }
    }
}
