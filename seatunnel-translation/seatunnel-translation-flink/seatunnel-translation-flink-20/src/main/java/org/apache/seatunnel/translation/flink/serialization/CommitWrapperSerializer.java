/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.translation.flink.serialization;

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.translation.flink.sink.CommitWrapper;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.InstantiationUtil;

import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * The serializer of {@link CommitWrapper}, which is used to serialize and deserialize the commit
 * message wrapper. Supports both SeaTunnel Serializer and Java serialization for compatibility.
 *
 * @param <CommT> The generic type of commit message
 */
@Slf4j
public class CommitWrapperSerializer<CommT>
        implements SimpleVersionedSerializer<CommitWrapper<CommT>> {

    private final Serializer<CommT> seatunnelSerializer;

    /** Constructor with SeaTunnel serializer (preferred) */
    public CommitWrapperSerializer(Serializer<CommT> serializer) {
        this.seatunnelSerializer = serializer;
    }

    /** Default constructor using Java serialization (fallback) */
    public CommitWrapperSerializer() {
        this.seatunnelSerializer = null;
    }

    @Override
    public int getVersion() {
        return seatunnelSerializer != null ? 2 : 1;
    }

    @Override
    public byte[] serialize(CommitWrapper<CommT> obj) throws IOException {
        if (obj == null || obj.getCommit() == null) {
            return new byte[0];
        }

        if (seatunnelSerializer != null) {
            // Use SeaTunnel serializer (like flink-common)
            try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    final DataOutputStream out = new DataOutputStream(baos)) {
                byte[] serialized = seatunnelSerializer.serialize(obj.getCommit());
                out.writeInt(serialized.length);
                out.write(serialized);
                out.flush();
                return baos.toByteArray();
            }
        } else {
            // Fallback to Java serialization
            return InstantiationUtil.serializeObject(obj.getCommit());
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public CommitWrapper<CommT> deserialize(int version, byte[] serialized) throws IOException {
        if (serialized == null || serialized.length == 0) {
            return new CommitWrapper<>(null);
        }

        try {
            if (version == 2 && seatunnelSerializer != null) {
                // Use SeaTunnel deserializer
                java.io.DataInputStream in =
                        new java.io.DataInputStream(new java.io.ByteArrayInputStream(serialized));
                int length = in.readInt();
                byte[] data = new byte[length];
                in.readFully(data);
                CommT commit = seatunnelSerializer.deserialize(data);
                return new CommitWrapper<>(commit);
            } else {
                // Fallback to Java deserialization
                CommT commit =
                        (CommT)
                                InstantiationUtil.deserializeObject(
                                        serialized, getClass().getClassLoader());
                return new CommitWrapper<>(commit);
            }
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize commit wrapper", e);
        }
    }
}
