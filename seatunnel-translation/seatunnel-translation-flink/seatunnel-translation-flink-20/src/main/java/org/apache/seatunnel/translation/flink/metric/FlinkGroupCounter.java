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

@Slf4j
public class FlinkGroupCounter implements Counter {
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
        try {
            // Flink 1.20 支持 inc(-1) 操作
            counter.inc(-1);
        } catch (Exception e) {
            log.warn("Error decrementing counter: {}", name, e);
        }
    }

    @Override
    public void dec(long n) {
        try {
            // Flink 1.20 支持 inc(-n) 操作
            counter.inc(-n);
        } catch (Exception e) {
            log.warn("Error decrementing counter by {}: {}", n, name, e);
        }
    }

    @Override
    public void set(long n) {
        // Flink 1.20 不支持直接设置计数器值
        log.warn(
                "Flink metrics does not support set operation directly, ignoring set({}) for: {}",
                n,
                name);
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
