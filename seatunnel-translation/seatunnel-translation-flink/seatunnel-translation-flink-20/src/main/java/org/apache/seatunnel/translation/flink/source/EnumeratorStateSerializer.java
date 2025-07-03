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

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/**
 * The serializer for enumerator state, used to adapt SeaTunnel's {@link Serializer} to Flink's
 * {@link SimpleVersionedSerializer}.
 *
 * @param <T> The type of the serialized data.
 */
public class EnumeratorStateSerializer<T> implements SimpleVersionedSerializer<T> {

    private final Serializer<T> serializer;

    public EnumeratorStateSerializer(Serializer<T> serializer) {
        this.serializer = serializer;
    }

    @Override
    public int getVersion() {
        // 由于Serializer接口没有getVersion方法，我们返回一个固定版本号
        return 0;
    }

    @Override
    public byte[] serialize(T obj) throws IOException {
        return serializer.serialize(obj);
    }

    @Override
    public T deserialize(int version, byte[] serialized) throws IOException {
        // 忽略version参数，直接调用Serializer的deserialize方法
        return serializer.deserialize(serialized);
    }
}
