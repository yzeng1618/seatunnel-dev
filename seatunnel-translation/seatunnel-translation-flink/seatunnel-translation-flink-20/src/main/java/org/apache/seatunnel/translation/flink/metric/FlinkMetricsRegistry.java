package org.apache.seatunnel.translation.flink.metric;

import org.apache.seatunnel.api.common.metrics.MetricNames;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/** 一个简单的指标注册表，用于在Flink作业中跨任务和组件共享指标。 */
@Slf4j
public class FlinkMetricsRegistry {

    private static final Map<String, Long> METRICS = new ConcurrentHashMap<>();

    /**
     * 注册一个指标值
     *
     * @param jobId 作业ID
     * @param subtaskIndex 子任务索引
     * @param metricName 指标名称
     * @param value 指标值
     */
    public static void registerMetric(
            String jobId, int subtaskIndex, String metricName, long value) {
        String key = String.format("%s_%d_%s", jobId, subtaskIndex, metricName);
        METRICS.put(key, value);
        log.debug("[METRICS] Registered metric: {} = {}", key, value);

        // 同时更新总计数
        String totalKey = String.format("%s_%s", jobId, metricName);
        METRICS.compute(totalKey, (k, v) -> (v == null ? 0 : v) + value);
        log.debug("[METRICS] Updated total metric: {} = {}", totalKey, METRICS.get(totalKey));

        // 保存到文件以确保持久化
        saveMetricsToFile(jobId, subtaskIndex);
    }

    /**
     * 更新一个指标值
     *
     * @param jobId 作业ID
     * @param subtaskIndex 子任务索引
     * @param metricName 指标名称
     * @param value 新的指标值
     */
    public static void updateMetric(String jobId, int subtaskIndex, String metricName, long value) {
        String key = String.format("%s_%d_%s", jobId, subtaskIndex, metricName);

        // 获取旧值
        Long oldValue = METRICS.get(key);

        // 更新指标
        METRICS.put(key, value);
        log.debug("[METRICS] Updated metric: {} = {}", key, value);

        // 更新总计数
        String totalKey = String.format("%s_%s", jobId, metricName);
        if (oldValue != null) {
            // 减去旧值，加上新值
            METRICS.compute(totalKey, (k, v) -> (v == null ? 0 : v) - oldValue + value);
        } else {
            // 没有旧值，直接加上新值
            METRICS.compute(totalKey, (k, v) -> (v == null ? 0 : v) + value);
        }
        log.debug("[METRICS] Updated total metric: {} = {}", totalKey, METRICS.get(totalKey));

        // 保存到文件以确保持久化
        saveMetricsToFile(jobId, subtaskIndex);
    }

    /**
     * 获取一个指标值
     *
     * @param jobId 作业ID
     * @param subtaskIndex 子任务索引
     * @param metricName 指标名称
     * @return 指标值，如果不存在则返回0
     */
    public static long getMetric(String jobId, int subtaskIndex, String metricName) {
        String key = String.format("%s_%d_%s", jobId, subtaskIndex, metricName);
        return METRICS.getOrDefault(key, 0L);
    }

    /**
     * 获取一个作业的总指标值
     *
     * @param jobId 作业ID
     * @param metricName 指标名称
     * @return 总指标值，如果不存在则返回0
     */
    public static long getTotalMetric(String jobId, String metricName) {
        String totalKey = String.format("%s_%s", jobId, metricName);
        return METRICS.getOrDefault(totalKey, 0L);
    }

    /**
     * 获取一个作业的所有指标
     *
     * @param jobId 作业ID
     * @return 指标映射
     */
    public static Map<String, Long> getJobMetrics(String jobId) {
        Map<String, Long> jobMetrics = new ConcurrentHashMap<>();

        for (Map.Entry<String, Long> entry : METRICS.entrySet()) {
            if (entry.getKey().startsWith(jobId)) {
                jobMetrics.put(entry.getKey(), entry.getValue());
            }
        }

        return jobMetrics;
    }

    /**
     * 获取一个作业的聚合指标
     *
     * @param jobId 作业ID
     * @return 聚合指标映射
     */
    public static Map<String, Long> getAggregatedJobMetrics(String jobId) {
        Map<String, Long> aggregatedMetrics = new ConcurrentHashMap<>();

        // 直接获取总计数
        aggregatedMetrics.put(
                MetricNames.SINK_WRITE_COUNT, getTotalMetric(jobId, MetricNames.SINK_WRITE_COUNT));
        aggregatedMetrics.put(
                MetricNames.SINK_WRITE_BYTES, getTotalMetric(jobId, MetricNames.SINK_WRITE_BYTES));

        return aggregatedMetrics;
    }

    /**
     * 保存指标到文件
     *
     * @param jobId 作业ID
     * @param subtaskIndex 子任务索引
     */
    private static void saveMetricsToFile(String jobId, int subtaskIndex) {
        try {
            String tmpDir = System.getProperty("java.io.tmpdir", "/tmp");
            File metricsFile =
                    new File(
                            tmpDir,
                            "seatunnel_metrics_" + jobId + "_" + subtaskIndex + ".properties");

            Properties props = new Properties();

            // 添加子任务指标
            props.setProperty(
                    MetricNames.SINK_WRITE_COUNT,
                    String.valueOf(getMetric(jobId, subtaskIndex, MetricNames.SINK_WRITE_COUNT)));
            props.setProperty(
                    MetricNames.SINK_WRITE_BYTES,
                    String.valueOf(getMetric(jobId, subtaskIndex, MetricNames.SINK_WRITE_BYTES)));

            // 添加总指标
            props.setProperty(
                    "total." + MetricNames.SINK_WRITE_COUNT,
                    String.valueOf(getTotalMetric(jobId, MetricNames.SINK_WRITE_COUNT)));
            props.setProperty(
                    "total." + MetricNames.SINK_WRITE_BYTES,
                    String.valueOf(getTotalMetric(jobId, MetricNames.SINK_WRITE_BYTES)));

            try (FileOutputStream fos = new FileOutputStream(metricsFile)) {
                props.store(fos, "SeaTunnel Metrics for Job " + jobId + " Subtask " + subtaskIndex);
                log.debug("[METRICS] Saved metrics to file: {}", metricsFile.getAbsolutePath());
            }
        } catch (Exception e) {
            log.warn("[METRICS] Failed to save metrics to file: {}", e.getMessage());
        }
    }

    /** 从文件加载指标 */
    public static void loadMetricsFromFiles() {
        try {
            String tmpDir = System.getProperty("java.io.tmpdir", "/tmp");
            File[] files =
                    new File(tmpDir)
                            .listFiles(
                                    (dir, name) ->
                                            name.startsWith("seatunnel_metrics_")
                                                    && name.endsWith(".properties"));

            if (files != null) {
                log.info("[METRICS] Found {} metrics files to load", files.length);

                for (File file : files) {
                    try {
                        Properties props = new Properties();
                        try (FileInputStream fis = new FileInputStream(file)) {
                            props.load(fis);

                            // 解析文件名以获取jobId和subtaskIndex
                            String fileName = file.getName();
                            String[] parts =
                                    fileName.replace("seatunnel_metrics_", "")
                                            .replace(".properties", "")
                                            .split("_");

                            if (parts.length >= 2) {
                                String jobId = parts[0];
                                int subtaskIndex = Integer.parseInt(parts[1]);

                                // 加载指标
                                for (String metricName :
                                        new String[] {
                                            MetricNames.SINK_WRITE_COUNT,
                                            MetricNames.SINK_WRITE_BYTES
                                        }) {
                                    String valueStr = props.getProperty(metricName);
                                    if (valueStr != null) {
                                        try {
                                            long value = Long.parseLong(valueStr);
                                            String key =
                                                    String.format(
                                                            "%s_%d_%s",
                                                            jobId, subtaskIndex, metricName);
                                            METRICS.put(key, value);
                                            log.debug(
                                                    "[METRICS] Loaded metric from file: {} = {}",
                                                    key,
                                                    value);
                                        } catch (NumberFormatException e) {
                                            log.warn(
                                                    "[METRICS] Failed to parse metric value: {} = {}",
                                                    metricName,
                                                    valueStr);
                                        }
                                    }
                                }

                                // 加载总指标
                                for (String metricName :
                                        new String[] {
                                            MetricNames.SINK_WRITE_COUNT,
                                            MetricNames.SINK_WRITE_BYTES
                                        }) {
                                    String totalKey = "total." + metricName;
                                    String valueStr = props.getProperty(totalKey);
                                    if (valueStr != null) {
                                        try {
                                            long value = Long.parseLong(valueStr);
                                            String key = String.format("%s_%s", jobId, metricName);
                                            METRICS.put(key, value);
                                            log.debug(
                                                    "[METRICS] Loaded total metric from file: {} = {}",
                                                    key,
                                                    value);
                                        } catch (NumberFormatException e) {
                                            log.warn(
                                                    "[METRICS] Failed to parse total metric value: {} = {}",
                                                    totalKey,
                                                    valueStr);
                                        }
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        log.warn(
                                "[METRICS] Failed to load metrics from file {}: {}",
                                file.getName(),
                                e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            log.warn("[METRICS] Failed to load metrics from files: {}", e.getMessage());
        }
    }

    // 静态初始化块，加载已有的指标文件
    static {
        loadMetricsFromFiles();
    }
}
