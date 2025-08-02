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

import org.apache.seatunnel.api.common.metrics.Meter;
import org.apache.seatunnel.api.common.metrics.Unit;

import java.util.concurrent.atomic.AtomicLong;

/** No-operation implementation of Meter metric. Used as fallback when metric registration fails. */
public class NoOpMeter implements Meter {
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
        return Unit.COUNT;
    }
}
