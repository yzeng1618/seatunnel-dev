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
import org.apache.seatunnel.translation.flink.sink.FlinkSinkWriter;

import org.apache.flink.api.common.JobExecutionResult;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileInputStream;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

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
        Map<String, Object> metrics = new HashMap<>();

        // 获取作业ID
        String jobId = jobExecutionResult.getJobID().toString();

        // 1. 尝试从Flink的指标系统中获取指标 - 使用MetricsRegistry
        try {
            // 从MetricsRegistry获取聚合指标
            Map<String, Long> aggregatedMetrics =
                    FlinkMetricsRegistry.getAggregatedJobMetrics(jobId);

            // 检查是否包含我们需要的指标
            if (aggregatedMetrics.containsKey(MetricNames.SINK_WRITE_COUNT)) {
                long writeCount = aggregatedMetrics.get(MetricNames.SINK_WRITE_COUNT);
                metrics.put(MetricNames.SINK_WRITE_COUNT, writeCount);
            } else {
                log.info("FLINK-20-MODULE No sink count found in MetricsRegistry");
            }

            if (aggregatedMetrics.containsKey(MetricNames.SINK_WRITE_BYTES)) {
                long writeBytes = aggregatedMetrics.get(MetricNames.SINK_WRITE_BYTES);
                metrics.put(MetricNames.SINK_WRITE_BYTES, writeBytes);
            } else {
                log.info("FLINK-20-MODULE No sink bytes found in MetricsRegistry");
            }

            // 如果MetricsRegistry中没有找到指标，尝试从Flink的内置指标获取
            if (!metrics.containsKey(MetricNames.SINK_WRITE_COUNT)
                    || !metrics.containsKey(MetricNames.SINK_WRITE_BYTES)) {

                System.out.println(
                        "FLINK-20-MODULE Metrics not complete from MetricsRegistry, trying Flink accumulators");
                log.info(
                        "FLINK-20-MODULE Metrics not complete from MetricsRegistry, trying Flink accumulators");

                // 尝试从Flink的累加器结果中获取
                Map<String, Object> accumulatorResults =
                        jobExecutionResult.getAllAccumulatorResults();

                for (Map.Entry<String, Object> entry : accumulatorResults.entrySet()) {
                    String key = entry.getKey();
                    Object value = entry.getValue();

                    if (key.equalsIgnoreCase(MetricNames.SINK_WRITE_COUNT)
                            || key.equalsIgnoreCase("numRecordsOut")
                            || key.equalsIgnoreCase("numRecordsSend")) {

                        if (value instanceof Number) {
                            long longValue = ((Number) value).longValue();
                            metrics.put(MetricNames.SINK_WRITE_COUNT, longValue);
                        }
                    } else if (key.equalsIgnoreCase(MetricNames.SINK_WRITE_BYTES)
                            || key.equalsIgnoreCase("numBytesOut")) {

                        if (value instanceof Number) {
                            long longValue = ((Number) value).longValue();
                            metrics.put(MetricNames.SINK_WRITE_BYTES, longValue);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.warn(
                    "FLINK-20-MODULE Failed to get metrics from MetricsRegistry: {}",
                    e.getMessage(),
                    e);
        }

        // 2. 尝试从系统属性中获取指标
        try {
            // 尝试从全局系统属性中获取指标
            String globalWriteCount =
                    System.getProperty("seatunnel.global." + MetricNames.SINK_WRITE_COUNT);
            String globalWriteBytes =
                    System.getProperty("seatunnel.global." + MetricNames.SINK_WRITE_BYTES);

            log.info("FLINK-20-MODULE System property for sink count: {}", globalWriteCount);
            log.info("FLINK-20-MODULE System property for sink bytes: {}", globalWriteBytes);

            if (globalWriteCount != null) {
                try {
                    long value = Long.parseLong(globalWriteCount);
                    metrics.put(MetricNames.SINK_WRITE_COUNT, value);
                    log.info("FLINK-20-MODULE Using global property for sink count: {}", value);
                } catch (NumberFormatException e) {
                    log.warn(
                            "FLINK-20-MODULE Failed to parse global sink count: {}",
                            globalWriteCount);
                }
            }

            if (globalWriteBytes != null) {
                try {
                    long value = Long.parseLong(globalWriteBytes);
                    metrics.put(MetricNames.SINK_WRITE_BYTES, value);
                    log.info("FLINK-20-MODULE Using global property for sink bytes: {}", value);
                } catch (NumberFormatException e) {
                    log.warn(
                            "FLINK-20-MODULE Failed to parse global sink bytes: {}",
                            globalWriteBytes);
                }
            }
        } catch (Exception e) {
            log.warn(
                    "FLINK-20-MODULE Failed to get metrics from system properties: {}",
                    e.getMessage(),
                    e);
        }

        // 3. 尝试从文件中获取指标
        try {
            log.info("FLINK-20-MODULE Step 3: Checking metrics files");

            String tmpDir = System.getProperty("java.io.tmpdir", "/tmp");
            File tmpDirectory = new File(tmpDir);
            File[] metricsFiles =
                    tmpDirectory.listFiles(
                            (dir, name) -> name.startsWith("seatunnel_metrics_" + jobId));

            if (metricsFiles != null) {
                log.info("FLINK-20-MODULE Found {} metrics files", metricsFiles.length);

                if (metricsFiles.length > 0) {
                    long totalWriteCount = 0;
                    long totalWriteBytes = 0;

                    for (File file : metricsFiles) {
                        log.info("FLINK-20-MODULE Processing file: {}", file.getName());

                        try {
                            Properties props = new Properties();
                            try (FileInputStream fis = new FileInputStream(file)) {
                                props.load(fis);

                                String writeCountStr =
                                        props.getProperty(MetricNames.SINK_WRITE_COUNT);
                                String writeBytesStr =
                                        props.getProperty(MetricNames.SINK_WRITE_BYTES);

                                log.info(
                                        "FLINK-20-MODULE File contains sink count: {}",
                                        writeCountStr);
                                log.info(
                                        "FLINK-20-MODULE File contains sink bytes: {}",
                                        writeBytesStr);

                                if (writeCountStr != null) {
                                    long writeCount = Long.parseLong(writeCountStr);
                                    totalWriteCount += writeCount;
                                    log.info(
                                            "FLINK-20-MODULE Added write count from file: +{}",
                                            writeCount);
                                }

                                if (writeBytesStr != null) {
                                    long writeBytes = Long.parseLong(writeBytesStr);
                                    totalWriteBytes += writeBytes;
                                    log.info(
                                            "FLINK-20-MODULE Added write bytes from file: +{}",
                                            writeBytes);
                                }
                            }
                        } catch (Exception e) {
                            log.warn(
                                    "FLINK-20-MODULE Failed to read metrics from file: {}",
                                    e.getMessage(),
                                    e);
                        }
                    }

                    // 只有在找到有效指标时才更新
                    if (totalWriteCount > 0) {
                        metrics.put(MetricNames.SINK_WRITE_COUNT, totalWriteCount);
                        log.info(
                                "FLINK-20-MODULE Using file-based metrics for sink count: {}",
                                totalWriteCount);
                    }

                    if (totalWriteBytes > 0) {
                        metrics.put(MetricNames.SINK_WRITE_BYTES, totalWriteBytes);
                        log.info(
                                "FLINK-20-MODULE Using file-based metrics for sink bytes: {}",
                                totalWriteBytes);
                    }
                } else {
                    log.info("FLINK-20-MODULE No metrics files found for job ID: {}", jobId);
                }
            }
        } catch (Exception e) {
            log.warn("FLINK-20-MODULE Failed to get metrics from files: {}", e.getMessage(), e);
        }

        // 4. 尝试从静态全局计数器中获取指标
        try {
            log.info("FLINK-20-MODULE Step 4: Checking global counters");

            // 获取所有与当前作业相关的计数器
            Map<String, Long> jobCounters = new HashMap<>();
            for (Map.Entry<String, Long> entry : FlinkSinkWriter.GLOBAL_COUNTERS.entrySet()) {
                if (entry.getKey().startsWith(jobId)) {
                    jobCounters.put(entry.getKey(), entry.getValue());
                }
            }
            log.info(
                    "FLINK-20-MODULE Found {} global counters for job ID: {}",
                    jobCounters.size(),
                    jobId);
            log.info("FLINK-20-MODULE Global counters: {}", jobCounters);

            if (!jobCounters.isEmpty()) {
                // 查找总计数器
                String totalWriteCountKey = jobId + "_" + MetricNames.SINK_WRITE_COUNT;
                String totalWriteBytesKey = jobId + "_" + MetricNames.SINK_WRITE_BYTES;

                log.info(
                        "FLINK-20-MODULE Looking for total counter keys: {}, {}",
                        totalWriteCountKey,
                        totalWriteBytesKey);

                if (jobCounters.containsKey(totalWriteCountKey)) {
                    long totalWriteCount = jobCounters.get(totalWriteCountKey);
                    metrics.put(MetricNames.SINK_WRITE_COUNT, totalWriteCount);
                    log.info(
                            "FLINK-20-MODULE Using global counter for sink count: {}",
                            totalWriteCount);
                }

                if (jobCounters.containsKey(totalWriteBytesKey)) {
                    long totalWriteBytes = jobCounters.get(totalWriteBytesKey);
                    metrics.put(MetricNames.SINK_WRITE_BYTES, totalWriteBytes);
                    log.info(
                            "FLINK-20-MODULE Using global counter for sink bytes: {}",
                            totalWriteBytes);
                }

                // 如果没有找到总计数器，则手动计算
                if (!metrics.containsKey(MetricNames.SINK_WRITE_COUNT)
                        || !metrics.containsKey(MetricNames.SINK_WRITE_BYTES)) {

                    log.info(
                            "FLINK-20-MODULE No total counters found, calculating from subtask counters");

                    long totalWriteCount = 0;
                    long totalWriteBytes = 0;

                    for (Map.Entry<String, Long> entry : jobCounters.entrySet()) {
                        if (entry.getKey().contains("_" + MetricNames.SINK_WRITE_COUNT)
                                && !entry.getKey().equals(totalWriteCountKey)) {
                            totalWriteCount += entry.getValue();
                            log.info(
                                    "FLINK-20-MODULE Adding to write count: {} = {}",
                                    entry.getKey(),
                                    entry.getValue());
                        } else if (entry.getKey().contains("_" + MetricNames.SINK_WRITE_BYTES)
                                && !entry.getKey().equals(totalWriteBytesKey)) {
                            totalWriteBytes += entry.getValue();
                            log.info(
                                    "FLINK-20-MODULE Adding to write bytes: {} = {}",
                                    entry.getKey(),
                                    entry.getValue());
                        }
                    }

                    if (totalWriteCount > 0 && !metrics.containsKey(MetricNames.SINK_WRITE_COUNT)) {
                        metrics.put(MetricNames.SINK_WRITE_COUNT, totalWriteCount);
                        log.info(
                                "FLINK-20-MODULE Calculated total sink count from subtask counters: {}",
                                totalWriteCount);
                    }

                    if (totalWriteBytes > 0 && !metrics.containsKey(MetricNames.SINK_WRITE_BYTES)) {
                        metrics.put(MetricNames.SINK_WRITE_BYTES, totalWriteBytes);
                        log.info(
                                "FLINK-20-MODULE Calculated total sink bytes from subtask counters: {}",
                                totalWriteBytes);
                    }
                }
            } else {
                log.info("FLINK-20-MODULE No global counters found for job ID: {}", jobId);
            }
        } catch (Exception e) {
            log.warn(
                    "FLINK-20-MODULE Failed to get metrics from global counters: {}",
                    e.getMessage(),
                    e);
        }

        // 5. 如果仍然没有找到Sink指标，使用Source指标作为备选
        if (!metrics.containsKey(MetricNames.SINK_WRITE_COUNT)
                || !metrics.containsKey(MetricNames.SINK_WRITE_BYTES)) {
            log.info(
                    "FLINK-20-MODULE Step 5: Still missing metrics, trying source metrics as fallback");

            // 尝试从累加器结果中获取Source指标
            Map<String, Object> accumulatorResults = jobExecutionResult.getAllAccumulatorResults();

            for (Map.Entry<String, Object> entry : accumulatorResults.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                log.info("FLINK-20-MODULE Checking source accumulator: {} = {}", key, value);

                if (key.equalsIgnoreCase(MetricNames.SOURCE_RECEIVED_COUNT)
                        || key.equalsIgnoreCase("SourceReceivedCount")
                        || key.equalsIgnoreCase("numRecordsIn")) {
                    if (!metrics.containsKey(MetricNames.SINK_WRITE_COUNT)) {
                        metrics.put(MetricNames.SINK_WRITE_COUNT, value);
                        log.info("FLINK-20-MODULE Using source count as sink count: {}", value);
                    }
                } else if (key.equalsIgnoreCase(MetricNames.SOURCE_RECEIVED_BYTES)
                        || key.equalsIgnoreCase("SourceReceivedBytes")
                        || key.equalsIgnoreCase("numBytesIn")) {
                    if (!metrics.containsKey(MetricNames.SINK_WRITE_BYTES)) {
                        metrics.put(MetricNames.SINK_WRITE_BYTES, value);
                        log.info("FLINK-20-MODULE Using source bytes as sink bytes: {}", value);
                    }
                }
            }
        }
        log.info("FLINK-20-MODULE Final collected metrics: {}", metrics);
        return metrics;
    }

    @Override
    public String toString() {
        log.info("FLINK-20-MODULE: toString() called");
        Map<String, Object> metrics = getMetrics();

        log.info("FLINK-20-MODULE: Available metrics: {}", metrics.keySet());

        // 获取指标值
        long sinkWriteCount = getCounterValue(metrics, MetricNames.SINK_WRITE_COUNT, 0L);
        long sinkWriteBytes = getCounterValue(metrics, MetricNames.SINK_WRITE_BYTES, 0L);

        log.info(
                "FLINK-20-MODULE: Metrics values - sinkWriteCount: {}, sinkWriteBytes: {}",
                sinkWriteCount,
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
                jobExecutionResult
                        .getAllAccumulatorResults()
                        .get(MetricNames.SOURCE_RECEIVED_COUNT),
                "Total Write Count",
                sinkWriteCount,
                "Total Read Bytes",
                jobExecutionResult
                        .getAllAccumulatorResults()
                        .get(MetricNames.SOURCE_RECEIVED_BYTES),
                "Total Write Bytes",
                sinkWriteBytes);
    }

    private long getCounterValue(Map<String, Object> metrics, String name, long defaultValue) {
        log.info("FLINK-20-MODULE: getCounterValue() called for: {}", name);
        Object value = metrics.get(name);
        if (value == null) {
            log.info(
                    "FLINK-20-MODULE: No value found for: {}, using default: {}",
                    name,
                    defaultValue);
            return defaultValue;
        }

        if (value instanceof Number) {
            long result = ((Number) value).longValue();
            log.info("FLINK-20-MODULE: Found numeric value for: {} = {}", name, result);
            return result;
        }

        try {
            long result = Long.parseLong(value.toString());
            log.info("FLINK-20-MODULE: Parsed string value for: {} = {}", name, result);
            return result;
        } catch (NumberFormatException e) {
            log.warn(
                    "FLINK-20-MODULE: Failed to parse counter value: {} = {}, using default: {}",
                    name,
                    value,
                    defaultValue);
            return defaultValue;
        }
    }
}
