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
import org.apache.seatunnel.api.common.metrics.MetricNames;
import org.apache.seatunnel.api.common.metrics.Unit;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RuntimeContext;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FlinkAccumulatorCounter implements Counter {
    private final String name;
    private final org.apache.flink.metrics.Counter flinkCounter;
    private final LongCounter accumulator;
    private final RuntimeContext runtimeContext;
    private volatile long localCount = 0L;

    public FlinkAccumulatorCounter(
            String name,
            org.apache.flink.metrics.Counter flinkCounter,
            RuntimeContext runtimeContext) {
        this.name = name;
        this.flinkCounter = flinkCounter;
        this.runtimeContext = runtimeContext;
        this.accumulator = new LongCounter();

        try {
            String accumulatorName = getStandardAccumulatorName(name);
            runtimeContext.addAccumulator(accumulatorName, accumulator);
            log.info(
                    "Successfully registered accumulator: {} (original name: {})",
                    accumulatorName,
                    name);
        } catch (Exception e) {
            log.warn("Failed to register accumulator: {}, error: {}", name, e.getMessage());
        }
    }

    @Override
    public void inc() {
        inc(1L);
    }

    @Override
    public void inc(long n) {
        try {
            if (flinkCounter != null) {
                flinkCounter.inc(n);
            }

            accumulator.add(n);

            localCount += n;

        } catch (Exception e) {
            log.warn("Error incrementing counter {}: {}", name, e.getMessage());
            localCount += n;
        }
    }

    @Override
    public void dec() {
        dec(1L);
    }

    @Override
    public void dec(long n) {
        try {
            if (flinkCounter != null) {
                flinkCounter.inc(-n);
            }

            accumulator.add(-n);

            localCount -= n;

        } catch (Exception e) {
            log.warn("Error decrementing counter {}: {}", name, e.getMessage());
            localCount -= n;
        }
    }

    @Override
    public void set(long n) {
        try {
            long diff = n - localCount;

            if (flinkCounter != null) {
                flinkCounter.inc(diff);
            }

            accumulator.add(diff);
            localCount = n;

            if (log.isDebugEnabled()) {
                log.debug("Counter [{}] set to {}", name, n);
            }
        } catch (Exception e) {
            log.warn("Error setting counter {}: {}", name, e.getMessage());
            localCount = n;
        }
    }

    @Override
    public long getCount() {
        try {
            long accumulatorValue = accumulator.getLocalValue();
            if (accumulatorValue != localCount) {
                localCount = accumulatorValue;
            }
            return accumulatorValue;
        } catch (Exception e) {
            log.warn("Failed to get accumulator value, using local count: {}", localCount);
            return localCount;
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Unit unit() {
        return Unit.COUNT;
    }

    public LongCounter getAccumulator() {
        return accumulator;
    }

    public void sync() {
        try {
            long accumulatorValue = accumulator.getLocalValue();
            if (accumulatorValue != localCount) {
                log.info(
                        "Syncing counter [{}]: accumulator={}, local={}",
                        name,
                        accumulatorValue,
                        localCount);
                localCount = accumulatorValue;
            }
        } catch (Exception e) {
            log.warn("Failed to sync counter [{}]: {}", name, e.getMessage());
        }
    }

    private String getStandardAccumulatorName(String originalName) {
        if (originalName.contains("SinkWriteCount")
                || originalName.equals(MetricNames.SINK_WRITE_COUNT)) {
            return MetricNames.SINK_WRITE_COUNT;
        } else if (originalName.contains("SinkWriteBytes")
                || originalName.equals(MetricNames.SINK_WRITE_BYTES)) {
            return MetricNames.SINK_WRITE_BYTES;
        } else if (originalName.contains("SourceReceivedCount")
                || originalName.equals(MetricNames.SOURCE_RECEIVED_COUNT)) {
            return MetricNames.SOURCE_RECEIVED_COUNT;
        } else if (originalName.contains("SourceReceivedBytes")
                || originalName.equals(MetricNames.SOURCE_RECEIVED_BYTES)) {
            return MetricNames.SOURCE_RECEIVED_BYTES;
        }
        return originalName;
    }
}
