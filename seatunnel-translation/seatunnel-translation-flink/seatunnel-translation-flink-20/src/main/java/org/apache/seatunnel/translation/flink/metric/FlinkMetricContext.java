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
import org.apache.seatunnel.api.common.metrics.MetricNames;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.common.metrics.Unit;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** The implementation of MetricsContext for Flink 1.20. */
@Slf4j
public class FlinkMetricContext implements MetricsContext {

    private final MetricGroup metricGroup;
    private final StreamingRuntimeContext runtimeContext;
    private final RuntimeContext generalRuntimeContext;
    private final Map<String, Counter> counters = new ConcurrentHashMap<>();
    private final Map<String, Meter> meters = new ConcurrentHashMap<>();

    public FlinkMetricContext(StreamingRuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
        this.generalRuntimeContext = runtimeContext;
        this.metricGroup = runtimeContext != null ? runtimeContext.getMetricGroup() : null;
        log.info(
                "FlinkMetricContext initialized with StreamingRuntimeContext: {}",
                runtimeContext != null ? "valid" : "null");
    }

    /** 新的构造函数，支持通过RuntimeContext注册accumulator */
    public FlinkMetricContext(RuntimeContext runtimeContext, MetricGroup metricGroup) {
        this.runtimeContext =
                runtimeContext instanceof StreamingRuntimeContext
                        ? (StreamingRuntimeContext) runtimeContext
                        : null;
        this.generalRuntimeContext = runtimeContext;
        this.metricGroup = metricGroup;
        log.info(
                "FlinkMetricContext initialized with RuntimeContext: {}, MetricGroup: {}",
                runtimeContext != null ? "valid" : "null",
                metricGroup != null ? "valid" : "null");
    }

    /** 只使用MetricGroup的构造函数，用于回退情况 */
    public FlinkMetricContext(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
        this.generalRuntimeContext = null;
        this.runtimeContext = null;
        log.info(
                "FlinkMetricContext initialized with MetricGroup only: {}",
                metricGroup != null ? "valid" : "null");
    }

    public FlinkMetricContext(MetricGroup metricGroup, RuntimeContext generalRuntimeContext) {
        this.metricGroup = metricGroup;
        this.generalRuntimeContext = generalRuntimeContext;
        this.runtimeContext = null;
        log.info(
                "FlinkMetricContext initialized with metricGroup: {}",
                metricGroup != null ? "valid" : "null");
    }

    @Override
    public Counter counter(String name) {
        Counter existingCounter = counters.get(name);
        if (existingCounter != null) {
            return existingCounter;
        }

        if (metricGroup == null) {
            log.warn("MetricGroup is null, returning no-op counter for: {}", name);
            Counter noOpCounter = new NoOpCounter();
            counters.put(name, noOpCounter);
            return noOpCounter;
        }

        try {
            org.apache.flink.metrics.Counter flinkCounter = metricGroup.counter(name);

            // 对于关键指标，同时创建累加器
            if (isKeyMetric(name) && generalRuntimeContext != null) {
                try {
                    // 显式声明参数类型以避免编译器混淆
                    String counterName = name;
                    org.apache.flink.metrics.Counter fCounter = flinkCounter;
                    RuntimeContext rContext = generalRuntimeContext;

                    Counter counter = new FlinkAccumulatorCounter(counterName, fCounter, rContext);
                    counters.put(name, counter);
                    log.info("Created counter with accumulator: {}", name);
                    return counter;
                } catch (Exception e) {
                    log.warn(
                            "Failed to create accumulator for: {}, falling back to simple counter",
                            name,
                            e);
                }
            }

            // 创建普通计数器
            Counter counter = new FlinkCounter(flinkCounter);
            counters.put(name, counter);
            log.debug("Created counter: {}", name);
            return counter;
        } catch (Exception e) {
            log.warn("Failed to create counter: {}, returning no-op counter", name, e);
            Counter noOpCounter = new NoOpCounter();
            counters.put(name, noOpCounter);
            return noOpCounter;
        }
    }

    @Override
    public <C extends Counter> C counter(String name, C counter) {
        return null;
    }

    @Override
    public Meter meter(String name) {
        Meter existingMeter = meters.get(name);
        if (existingMeter != null) {
            return existingMeter;
        }

        if (metricGroup == null) {
            log.warn("MetricGroup is null, returning no-op meter for: {}", name);
            Meter noOpMeter = new NoOpMeter();
            meters.put(name, noOpMeter);
            return noOpMeter;
        }

        try {
            org.apache.flink.metrics.Meter flinkMeter =
                    metricGroup.meter(name, new org.apache.flink.metrics.MeterView(60));

            Meter meter = new FlinkMeter(flinkMeter);
            meters.put(name, meter);
            log.debug("Created meter: {}", name);
            return meter;
        } catch (Exception e) {
            log.warn("Failed to create meter: {}, returning no-op meter", name, e);
            Meter noOpMeter = new NoOpMeter();
            meters.put(name, noOpMeter);
            return noOpMeter;
        }
    }

    @Override
    public <M extends Meter> M meter(String name, M meter) {
        return null;
    }

    /** 判断是否是关键指标 */
    private boolean isKeyMetric(String name) {
        return name.equals(MetricNames.SOURCE_RECEIVED_COUNT)
                || name.equals(MetricNames.SOURCE_RECEIVED_BYTES)
                || name.equals(MetricNames.SINK_WRITE_COUNT)
                || name.equals(MetricNames.SINK_WRITE_BYTES);
    }

    /** Flink 计数器实现 */
    private static class FlinkCounter implements Counter {
        private final org.apache.flink.metrics.Counter flinkCounter;

        FlinkCounter(org.apache.flink.metrics.Counter flinkCounter) {
            this.flinkCounter = flinkCounter;
        }

        @Override
        public void inc() {
            flinkCounter.inc();
        }

        @Override
        public void inc(long n) {
            flinkCounter.inc(n);
        }

        @Override
        public void dec() {}

        @Override
        public void dec(long n) {}

        @Override
        public void set(long n) {}

        @Override
        public long getCount() {
            return flinkCounter.getCount();
        }

        @Override
        public String name() {
            return "";
        }

        @Override
        public Unit unit() {
            return null;
        }
    }

    /** 无操作的计数器实现 */
    private static class NoOpCounter implements Counter {
        private final AtomicLong count = new AtomicLong(0);

        @Override
        public void inc() {
            count.incrementAndGet();
        }

        @Override
        public void inc(long n) {
            count.addAndGet(n);
        }

        @Override
        public void dec() {}

        @Override
        public void dec(long n) {}

        @Override
        public void set(long n) {}

        @Override
        public long getCount() {
            return count.get();
        }

        @Override
        public String name() {
            return "";
        }

        @Override
        public Unit unit() {
            return null;
        }
    }

    /** Flink 计量器实现 */
    private static class FlinkMeter implements Meter {
        private final org.apache.flink.metrics.Meter flinkMeter;

        FlinkMeter(org.apache.flink.metrics.Meter flinkMeter) {
            this.flinkMeter = flinkMeter;
        }

        @Override
        public void markEvent() {
            flinkMeter.markEvent();
        }

        @Override
        public void markEvent(long n) {
            flinkMeter.markEvent(n);
        }

        @Override
        public double getRate() {
            return flinkMeter.getRate();
        }

        @Override
        public long getCount() {
            return 0;
        }

        @Override
        public String name() {
            return "";
        }

        @Override
        public Unit unit() {
            return null;
        }
    }

    /** 无操作的计量器实现 */
    private static class NoOpMeter implements Meter {
        private final AtomicLong count = new AtomicLong(0);

        @Override
        public void markEvent() {
            count.incrementAndGet();
        }

        @Override
        public void markEvent(long n) {
            count.addAndGet(n);
        }

        @Override
        public double getRate() {
            return 0;
        }

        @Override
        public long getCount() {
            return 0;
        }

        @Override
        public String name() {
            return "";
        }

        @Override
        public Unit unit() {
            return null;
        }
    }
}
