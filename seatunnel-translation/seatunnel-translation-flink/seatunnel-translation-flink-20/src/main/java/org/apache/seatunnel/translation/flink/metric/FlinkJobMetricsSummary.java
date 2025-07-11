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
import java.util.Properties;
import java.util.stream.Collectors;

/** Flink 1.20 专用的作业指标摘要类。 处理 Flink 1.20 中的 Accumulator 结果，并处理 null 值。 */
@Slf4j
public class FlinkJobMetricsSummary {

    static {
        System.out.println("FLINK-20-MODULE: FlinkJobMetricsSummary class is being loaded");
        log.info("FLINK-20-MODULE: FlinkJobMetricsSummary class is being loaded");

        // 打印类加载器信息
        ClassLoader cl = FlinkJobMetricsSummary.class.getClassLoader();
        System.out.println("FLINK-20-MODULE: Class loaded by: " + cl);
        log.info("FLINK-20-MODULE: Class loaded by: {}", cl);

        // 打印类的位置
        try {
            System.out.println(
                    "FLINK-20-MODULE: Class location: "
                            + FlinkJobMetricsSummary.class
                                    .getProtectionDomain()
                                    .getCodeSource()
                                    .getLocation());
            log.info(
                    "FLINK-20-MODULE: Class location: {}",
                    FlinkJobMetricsSummary.class
                            .getProtectionDomain()
                            .getCodeSource()
                            .getLocation());
        } catch (Exception e) {
            System.out.println("FLINK-20-MODULE: Failed to get class location: " + e.getMessage());
            log.error("FLINK-20-MODULE: Failed to get class location", e);
        }
    }

    private final JobExecutionResult jobExecutionResult;
    private final LocalDateTime jobStartTime;
    private final LocalDateTime jobEndTime;

    public FlinkJobMetricsSummary(
            JobExecutionResult jobExecutionResult,
            LocalDateTime jobStartTime,
            LocalDateTime jobEndTime) {
        System.out.println("FLINK-20-MODULE: FlinkJobMetricsSummary constructor called");
        log.info("FLINK-20-MODULE: FlinkJobMetricsSummary constructor called");
        this.jobExecutionResult = jobExecutionResult;
        this.jobStartTime = jobStartTime;
        this.jobEndTime = jobEndTime;
        System.out.println(
                "FLINK-20-MODULE: FlinkJobMetricsSummary constructed with jobId: "
                        + (jobExecutionResult != null ? jobExecutionResult.getJobID() : "null"));
        log.info(
                "FLINK-20-MODULE: FlinkJobMetricsSummary constructed with jobId: {}",
                (jobExecutionResult != null ? jobExecutionResult.getJobID() : "null"));
    }

    /**
     * 创建一个 FlinkJobMetricsSummary 的 Builder
     *
     * @return FlinkJobMetricsSummary.Builder 实例
     */
    public static Builder builder() {
        System.out.println("FLINK-20-MODULE: FlinkJobMetricsSummary.builder() called");
        log.info("FLINK-20-MODULE: FlinkJobMetricsSummary.builder() called");
        return new Builder();
    }

    /** FlinkJobMetricsSummary 的 Builder 类 */
    public static class Builder {
        private JobExecutionResult jobExecutionResult;
        private long jobStartTime;
        private long jobEndTime;

        private Builder() {
            System.out.println(
                    "FLINK-20-MODULE: FlinkJobMetricsSummary.Builder constructor called");
            log.info("FLINK-20-MODULE: FlinkJobMetricsSummary.Builder constructor called");
        }

        /**
         * 设置作业执行结果
         *
         * @param jobExecutionResult 作业执行结果
         * @return Builder 实例
         */
        public Builder jobExecutionResult(JobExecutionResult jobExecutionResult) {
            System.out.println(
                    "FLINK-20-MODULE: Builder.jobExecutionResult() called with jobId: "
                            + (jobExecutionResult != null
                                    ? jobExecutionResult.getJobID()
                                    : "null"));
            log.info(
                    "FLINK-20-MODULE: Builder.jobExecutionResult() called with jobId: {}",
                    (jobExecutionResult != null ? jobExecutionResult.getJobID() : "null"));
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
            System.out.println(
                    "FLINK-20-MODULE: Builder.jobStartTime() called with: " + jobStartTime);
            log.info("FLINK-20-MODULE: Builder.jobStartTime() called with: {}", jobStartTime);
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
            System.out.println("FLINK-20-MODULE: Builder.jobEndTime() called with: " + jobEndTime);
            log.info("FLINK-20-MODULE: Builder.jobEndTime() called with: {}", jobEndTime);
            this.jobEndTime = jobEndTime;
            return this;
        }

        /**
         * 构建 FlinkJobMetricsSummary 实例
         *
         * @return FlinkJobMetricsSummary 实例
         */
        public FlinkJobMetricsSummary build() {
            System.out.println("FLINK-20-MODULE: Builder.build() called");
            log.info("FLINK-20-MODULE: Builder.build() called");
            return new FlinkJobMetricsSummary(
                    jobExecutionResult,
                    DateTimeUtils.parse(jobStartTime),
                    DateTimeUtils.parse(jobEndTime));
        }
    }

    /**
     * 获取作业指标
     *
     * @return 指标映射
     */
    public Map<String, Object> getMetrics() {
        System.out.println("FLINK-20-MODULE: getMetrics() called");
        log.info("FLINK-20-MODULE: getMetrics() called");
        Map<String, Object> metrics = new HashMap<>();

        // 从累加器结果中获取指标
        Map<String, Object> accumulatorResults = jobExecutionResult.getAllAccumulatorResults();
        System.out.println("FLINK-20-MODULE: Accumulator results: " + accumulatorResults);
        log.info("FLINK-20-MODULE: Accumulator results: {}", accumulatorResults);
        metrics.putAll(accumulatorResults);

        // 获取作业ID
        String jobId = jobExecutionResult.getJobID().toString();
        System.out.println("FLINK-20-MODULE: Getting metrics for job ID: " + jobId);
        log.info("FLINK-20-MODULE: Getting metrics for job ID: {}", jobId);

        // 列出所有系统属性，用于调试
        Properties props = System.getProperties();
        System.out.println(
                "FLINK-20-MODULE: Available system properties (first 20): "
                        + props.stringPropertyNames().stream()
                                .limit(20)
                                .collect(Collectors.toList()));
        log.info(
                "FLINK-20-MODULE: Available system properties (first 20): {}",
                props.stringPropertyNames().stream().limit(20).collect(Collectors.toList()));

        // 从系统属性中获取指标
        String prefix = "seatunnel.metric." + jobId + ".";
        System.out.println("FLINK-20-MODULE: Looking for system properties with prefix: " + prefix);
        log.info("FLINK-20-MODULE: Looking for system properties with prefix: {}", prefix);

        // 打印所有包含seatunnel的系统属性，用于调试
        System.out.println("FLINK-20-MODULE: All seatunnel-related system properties:");
        props.stringPropertyNames().stream()
                .filter(name -> name.toLowerCase().contains("seatunnel"))
                .forEach(
                        name -> {
                            System.out.println("  " + name + " = " + System.getProperty(name));
                            log.info(
                                    "FLINK-20-MODULE: Found seatunnel property: {} = {}",
                                    name,
                                    System.getProperty(name));
                        });

        // 尝试从累加器中直接获取Sink指标
        if (!metrics.containsKey(MetricNames.SINK_WRITE_COUNT)
                && metrics.containsKey(MetricNames.SOURCE_RECEIVED_COUNT)) {
            System.out.println(
                    "FLINK-20-MODULE: No sink metrics found, using source metrics as fallback");
            log.info("FLINK-20-MODULE: No sink metrics found, using source metrics as fallback");

            // 使用Source指标作为Sink指标的备选
            Object sourceCount = metrics.get(MetricNames.SOURCE_RECEIVED_COUNT);
            Object sourceBytes = metrics.get(MetricNames.SOURCE_RECEIVED_BYTES);

            if (sourceCount != null) {
                metrics.put(MetricNames.SINK_WRITE_COUNT, sourceCount);
                System.out.println(
                        "FLINK-20-MODULE: Using source count as sink count: " + sourceCount);
                log.info("FLINK-20-MODULE: Using source count as sink count: {}", sourceCount);
            }

            if (sourceBytes != null) {
                metrics.put(MetricNames.SINK_WRITE_BYTES, sourceBytes);
                System.out.println(
                        "FLINK-20-MODULE: Using source bytes as sink bytes: " + sourceBytes);
                log.info("FLINK-20-MODULE: Using source bytes as sink bytes: {}", sourceBytes);
            }
        }

        for (String propName : props.stringPropertyNames()) {
            if (propName.startsWith(prefix)) {
                String metricName = propName.substring(propName.lastIndexOf('.') + 1);
                String value = System.getProperty(propName);
                System.out.println(
                        "FLINK-20-MODULE: Found metric in system properties: "
                                + metricName
                                + " = "
                                + value);
                log.info(
                        "FLINK-20-MODULE: Found metric in system properties: {} = {}",
                        metricName,
                        value);

                try {
                    long longValue = Long.parseLong(value);
                    metrics.put(metricName, longValue);
                } catch (NumberFormatException e) {
                    metrics.put(metricName, value);
                }
            }
        }

        System.out.println("FLINK-20-MODULE: Collected metrics: " + metrics);
        log.info("FLINK-20-MODULE: Collected metrics: {}", metrics);
        return metrics;
    }

    @Override
    public String toString() {
        System.out.println("FLINK-20-MODULE: toString() called");
        log.info("FLINK-20-MODULE: toString() called");
        Map<String, Object> metrics = getMetrics();

        System.out.println("FLINK-20-MODULE: Available metrics: " + metrics.keySet());
        log.info("FLINK-20-MODULE: Available metrics: {}", metrics.keySet());

        // 获取指标值
        long sourceReceivedCount = getCounterValue(metrics, MetricNames.SOURCE_RECEIVED_COUNT, 0L);
        long sinkWriteCount = getCounterValue(metrics, MetricNames.SINK_WRITE_COUNT, 0L);
        long sourceReceivedBytes = getCounterValue(metrics, MetricNames.SOURCE_RECEIVED_BYTES, 0L);
        long sinkWriteBytes = getCounterValue(metrics, MetricNames.SINK_WRITE_BYTES, 0L);

        System.out.println(
                "FLINK-20-MODULE: Metrics values - sourceReceivedCount: "
                        + sourceReceivedCount
                        + ", sinkWriteCount: "
                        + sinkWriteCount
                        + ", sourceReceivedBytes: "
                        + sourceReceivedBytes
                        + ", sinkWriteBytes: "
                        + sinkWriteBytes);
        log.info(
                "FLINK-20-MODULE: Metrics values - sourceReceivedCount: {}, sinkWriteCount: {}, sourceReceivedBytes: {}, sinkWriteBytes: {}",
                sourceReceivedCount,
                sinkWriteCount,
                sourceReceivedBytes,
                sinkWriteBytes);

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

    private long getCounterValue(Map<String, Object> metrics, String name, long defaultValue) {
        System.out.println("FLINK-20-MODULE: getCounterValue() called for: " + name);
        log.info("FLINK-20-MODULE: getCounterValue() called for: {}", name);
        Object value = metrics.get(name);
        if (value == null) {
            System.out.println(
                    "FLINK-20-MODULE: No value found for: "
                            + name
                            + ", using default: "
                            + defaultValue);
            log.info(
                    "FLINK-20-MODULE: No value found for: {}, using default: {}",
                    name,
                    defaultValue);
            return defaultValue;
        }

        if (value instanceof Number) {
            long result = ((Number) value).longValue();
            System.out.println(
                    "FLINK-20-MODULE: Found numeric value for: " + name + " = " + result);
            log.info("FLINK-20-MODULE: Found numeric value for: {} = {}", name, result);
            return result;
        }

        try {
            long result = Long.parseLong(value.toString());
            System.out.println(
                    "FLINK-20-MODULE: Parsed string value for: " + name + " = " + result);
            log.info("FLINK-20-MODULE: Parsed string value for: {} = {}", name, result);
            return result;
        } catch (NumberFormatException e) {
            System.out.println(
                    "FLINK-20-MODULE: Failed to parse counter value: "
                            + name
                            + " = "
                            + value
                            + ", using default: "
                            + defaultValue);
            log.warn(
                    "FLINK-20-MODULE: Failed to parse counter value: {} = {}, using default: {}",
                    name,
                    value,
                    defaultValue);
            return defaultValue;
        }
    }
}
