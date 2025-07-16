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

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.SourceSplit;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/**
 * Serializer for {@link SplitWrapper}.
 *
 * @param <SplitT> The type of the wrapped split
 */
public class SplitWrapperSerializer<SplitT extends SourceSplit>
        implements SimpleVersionedSerializer<SplitWrapper<SplitT>> {

    private final Serializer<SplitT> splitSerializer;

    public SplitWrapperSerializer(Serializer<SplitT> splitSerializer) {
        this.splitSerializer = splitSerializer;
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(SplitWrapper<SplitT> splitWrapper) throws IOException {
        return splitSerializer.serialize(splitWrapper.getSourceSplit());
    }

    @Override
    public SplitWrapper<SplitT> deserialize(int version, byte[] serialized) throws IOException {
        if (version != getVersion()) {
            throw new IOException("Unsupported version: " + version);
        }
        SplitT split = splitSerializer.deserialize(serialized);
        return new SplitWrapper<>(split);
    }
}
