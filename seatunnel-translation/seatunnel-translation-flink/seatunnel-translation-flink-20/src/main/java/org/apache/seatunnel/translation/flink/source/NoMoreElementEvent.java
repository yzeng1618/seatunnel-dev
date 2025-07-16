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

package org.apache.seatunnel.translation.flink.source;

import org.apache.flink.api.connector.source.SourceEvent;

/**
 * This event represents that there is no more data to read.
 *
 * <p>The execution process is as follows: 1. When a SourceReader has no more data to read, it sends
 * this event to the SourceEnumerator 2. The SourceEnumerator forwards this event back to the
 * SourceReader 3. The SourceReader changes its InputStatus from MORE_AVAILABLE to END_OF_INPUT
 */
public class NoMoreElementEvent implements SourceEvent {

    private final int subtaskId;

    public NoMoreElementEvent(int subtaskId) {
        this.subtaskId = subtaskId;
    }

    public int getSubtaskId() {
        return subtaskId;
    }

    @Override
    public String toString() {
        return "NoMoreElementEvent{" + "subtaskId=" + subtaskId + '}';
    }
}
