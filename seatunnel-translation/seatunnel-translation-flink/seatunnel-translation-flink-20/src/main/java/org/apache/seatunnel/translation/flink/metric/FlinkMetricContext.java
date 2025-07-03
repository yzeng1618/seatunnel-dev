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

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.Meter;
import org.apache.seatunnel.api.common.metrics.Metric;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.common.metrics.Unit;

import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class FlinkMetricContext implements MetricsContext {

    private final Map<String, Metric> metrics = new ConcurrentHashMap<>();
    private final MetricGroup metricGroup;

    public FlinkMetricContext(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
        log.info("FlinkMetricContext initialized with metricGroup: {}", metricGroup);
    }

    public FlinkMetricContext(StreamingRuntimeContext runtimeContext) {
        this(runtimeContext != null ? runtimeContext.getMetricGroup() : null);
        log.info("FlinkMetricContext initialized with StreamingRuntimeContext: {}", runtimeContext);
    }

    @Override
    public Counter counter(String name) {
        if (metrics.containsKey(name)) {
            log.debug("Returning existing counter for: {}", name);
            return (Counter) metrics.get(name);
        }

        if (metricGroup == null) {
            log.warn("MetricGroup is null, using NoOpCounter for: {}", name);
            return this.counter(name, new NoOpCounter(name));
        }

        try {
            // 使用唯一前缀避免名称冲突
            String uniqueName = "seatunnel_" + name;

            // 检查是否是SinkWriterMetricGroup
            if (name.equals("SinkWriteCount")
                    && metricGroup.getClass().getName().contains("SinkWriterMetricGroup")) {
                log.info("Using SinkWriterMetricGroup for SinkWriteCount");
                try {
                    org.apache.flink.metrics.Counter counter = metricGroup.counter(uniqueName);
                    log.info(
                            "Created counter for SinkWriteCount: {} with Flink name: {}",
                            name,
                            uniqueName);
                    return this.counter(name, new FlinkGroupCounter(name, counter));
                } catch (Exception e) {
                    log.warn("Failed to create SinkWriterMetricGroup counter: {}", name, e);
                }
            }

            // 标准计数器创建
            org.apache.flink.metrics.Counter counter = metricGroup.counter(uniqueName);
            log.info("Created standard counter: {} with Flink name: {}", name, uniqueName);
            return this.counter(name, new FlinkGroupCounter(name, counter));
        } catch (Exception e) {
            log.warn("Failed to create counter: {}", name, e);
            return this.counter(name, new NoOpCounter(name));
        }
    }

    @Override
    public <C extends Counter> C counter(String name, C counter) {
        this.addMetric(name, counter);
        return counter;
    }

    @Override
    public Meter meter(String name) {
        if (metrics.containsKey(name)) {
            log.debug("Returning existing meter for: {}", name);
            return (Meter) metrics.get(name);
        }

        if (metricGroup == null) {
            log.warn("MetricGroup is null, using NoOpMeter for: {}", name);
            return this.meter(name, new NoOpMeter(name));
        }

        try {
            // 使用唯一前缀避免名称冲突
            String uniqueName = "seatunnel_" + name;

            // 标准meter创建
            org.apache.flink.metrics.Meter meter = metricGroup.meter(uniqueName, new MeterView(5));
            log.info("Created standard meter: {} with Flink name: {}", name, uniqueName);
            return this.meter(name, new FlinkMeter(name, meter));
        } catch (Exception e) {
            log.warn("Failed to create meter: {}", name, e);
            return this.meter(name, new NoOpMeter(name));
        }
    }

    @Override
    public <M extends Meter> M meter(String name, M meter) {
        this.addMetric(name, meter);
        return meter;
    }

    protected void addMetric(String name, Metric metric) {
        if (metric == null) {
            log.warn("Ignoring attempted add of a metric due to being null for name {}.", name);
        } else {
            synchronized (this) {
                Metric prior = this.metrics.put(name, metric);
                if (prior != null) {
                    this.metrics.put(name, prior);
                    log.warn(
                            "Name collision: MetricsContext already contains a Metric with the name '"
                                    + name
                                    + "'. Metric will not be reported.");
                }
            }
        }
    }

    // Flink计数器实现
    private static class FlinkGroupCounter implements Counter {
        private final String name;
        private final org.apache.flink.metrics.Counter counter;

        public FlinkGroupCounter(String name, org.apache.flink.metrics.Counter counter) {
            this.name = name;
            this.counter = counter;
        }

        @Override
        public void inc() {
            counter.inc();
        }

        @Override
        public void inc(long n) {
            counter.inc(n);
        }

        @Override
        public void dec() {
            counter.dec();
        }

        @Override
        public void dec(long n) {
            counter.dec(n);
        }

        @Override
        public void set(long n) {
            // Flink Counter没有set方法，我们可以通过重置和增加来模拟
            counter.inc(n - getCount());
        }

        @Override
        public long getCount() {
            return counter.getCount();
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Unit unit() {
            return Unit.COUNT;
        }
    }

    // Flink计量器实现
    private static class FlinkMeter implements Meter {
        private final String name;
        private final org.apache.flink.metrics.Meter meter;

        public FlinkMeter(String name, org.apache.flink.metrics.Meter meter) {
            this.name = name;
            this.meter = meter;
        }

        @Override
        public void markEvent() {
            meter.markEvent();
        }

        @Override
        public void markEvent(long n) {
            meter.markEvent(n);
        }

        @Override
        public double getRate() {
            return meter.getRate();
        }

        @Override
        public long getCount() {
            return meter.getCount();
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Unit unit() {
            return Unit.COUNT;
        }
    }

    // 空操作计数器实现
    private static class NoOpCounter implements Counter {
        private final String name;
        private long count = 0;

        public NoOpCounter(String name) {
            this.name = name;
        }

        @Override
        public void inc() {
            count++;
        }

        @Override
        public void inc(long n) {
            count += n;
        }

        @Override
        public void dec() {
            count--;
        }

        @Override
        public void dec(long n) {
            count -= n;
        }

        @Override
        public void set(long n) {
            count = n;
        }

        @Override
        public long getCount() {
            return count;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Unit unit() {
            return Unit.COUNT;
        }
    }

    // 空操作计量器实现
    private static class NoOpMeter implements Meter {
        private final String name;
        private long count = 0;

        public NoOpMeter(String name) {
            this.name = name;
        }

        @Override
        public void markEvent() {
            count++;
        }

        @Override
        public void markEvent(long n) {
            count += n;
        }

        @Override
        public double getRate() {
            return 0;
        }

        @Override
        public long getCount() {
            return count;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Unit unit() {
            return Unit.COUNT;
        }
    }
}
