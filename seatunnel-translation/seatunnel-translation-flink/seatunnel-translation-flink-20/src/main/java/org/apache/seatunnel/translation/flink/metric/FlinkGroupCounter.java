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
import org.apache.seatunnel.api.common.metrics.Unit;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A counter implementation that tracks values globally across all instances. This is used to
 * collect metrics from all parallel tasks.
 */
@Slf4j
public class FlinkGroupCounter implements Counter {

    /** 全局计数器值存储 */
    public static final Map<String, Long> COUNTER_VALUES = new ConcurrentHashMap<>();

    /** 本地计数器值 */
    private final AtomicLong localCounter = new AtomicLong(0);

    /** 计数器名称 */
    private final String name;

    /** Flink 计数器 */
    private final org.apache.flink.metrics.Counter flinkCounter;

    /**
     * 创建一个新的 FlinkGroupCounter
     *
     * @param name 计数器名称
     * @param flinkCounter Flink 计数器
     */
    public FlinkGroupCounter(String name, org.apache.flink.metrics.Counter flinkCounter) {
        this.name = name;
        this.flinkCounter = flinkCounter;
        log.debug("Created FlinkGroupCounter: {}", name);
    }

    @Override
    public void inc() {
        inc(1L);
    }

    @Override
    public void inc(long n) {
        if (n <= 0) {
            return;
        }

        // 更新本地计数器
        localCounter.addAndGet(n);

        // 更新 Flink 计数器
        flinkCounter.inc(n);

        // 更新全局计数器
        updateGlobalCounter(name, n);

        // 定期记录日志
        long count = localCounter.get();
        if (count % 100000 == 0) {
            log.info("Counter [{}] reached: {}", name, count);
        }
    }

    @Override
    public void dec() {}

    @Override
    public void dec(long n) {}

    @Override
    public void set(long n) {}

    @Override
    public long getCount() {
        return localCounter.get();
    }

    /**
     * 更新全局计数器值
     *
     * @param name 计数器名称
     * @param delta 增量值
     */
    private static void updateGlobalCounter(String name, long delta) {
        COUNTER_VALUES.compute(name, (k, v) -> (v == null) ? delta : v + delta);
    }

    /**
     * 获取全局计数器值
     *
     * @param name 计数器名称
     * @return 计数器值，如果不存在则返回 0
     */
    public static long getGlobalCounterValue(String name) {
        return COUNTER_VALUES.getOrDefault(name, 0L);
    }

    /** 重置所有全局计数器 */
    public static void resetAllGlobalCounters() {
        COUNTER_VALUES.clear();
        log.info("All global counters have been reset");
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
