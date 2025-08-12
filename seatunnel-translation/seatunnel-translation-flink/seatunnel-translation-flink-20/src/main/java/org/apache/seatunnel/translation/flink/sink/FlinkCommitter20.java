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

package org.apache.seatunnel.translation.flink.sink;

import org.apache.seatunnel.api.sink.SinkCommitter;

import org.apache.flink.api.connector.sink2.Committer;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The committer wrapper for Flink 1.20+ sink2 API, adapted from FlinkCommitter to use the new
 * Committer interface.
 *
 * @param <CommT> The generic type of commit message
 */
@Slf4j
public class FlinkCommitter20<CommT> implements Committer<CommitWrapper<CommT>> {

    private final SinkCommitter<CommT> sinkCommitter;

    public FlinkCommitter20(SinkCommitter<CommT> sinkCommitter) {
        this.sinkCommitter = sinkCommitter;
    }

    @Override
    public void commit(Collection<Committer.CommitRequest<CommitWrapper<CommT>>> committables)
            throws IOException, InterruptedException {
        List<CommT> reCommittable =
                committables.stream()
                        .map(request -> request.getCommittable().getCommit())
                        .collect(Collectors.toList());

        try {
            List<CommT> needRetryCommittable = sinkCommitter.commit(reCommittable);
            if (needRetryCommittable != null && !needRetryCommittable.isEmpty()) {
                log.warn("Some committables need retry: {}", needRetryCommittable.size());
                // For Flink 1.20+, signal retry for failed committables
                for (Committer.CommitRequest<CommitWrapper<CommT>> request : committables) {
                    if (needRetryCommittable.contains(request.getCommittable().getCommit())) {
                        request.retryLater();
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to commit", e);
            // Signal failure for all requests
            for (Committer.CommitRequest<CommitWrapper<CommT>> request : committables) {
                request.signalFailedWithUnknownReason(e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        try {
            // SinkCommitter doesn't have close method, so we don't need to close it
            log.debug("FlinkCommitter20 closed");
        } catch (Exception e) {
            log.error("Failed to close committer", e);
            throw e;
        }
    }
}
