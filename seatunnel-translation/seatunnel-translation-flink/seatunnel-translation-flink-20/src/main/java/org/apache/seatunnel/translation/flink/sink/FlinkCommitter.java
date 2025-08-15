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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The committer wrapper of {@link SinkCommitter}, which is created by {@link
 * org.apache.flink.api.connector.sink2.SupportsCommitter#createCommitter()}, used to unify the
 * different sink committer implementations
 *
 * @param <CommT> The generic type of commit message
 */
@Slf4j
public class FlinkCommitter<CommT> implements Committer<CommitWrapper<CommT>> {

    private final SinkCommitter<CommT> sinkCommitter;

    public FlinkCommitter(SinkCommitter<CommT> sinkCommitter) {
        this.sinkCommitter = sinkCommitter;
        log.debug(
                "FlinkCommitter created with SeaTunnel SinkCommitter: {}",
                sinkCommitter.getClass().getSimpleName());
    }

    @Override
    public void commit(Collection<Committer.CommitRequest<CommitWrapper<CommT>>> committables)
            throws IOException, InterruptedException {
        if (committables == null || committables.isEmpty()) {
            log.debug("No committables to commit");
            return;
        }

        log.debug("Committing {} committables", committables.size());

        // Validate sinkCommitter is not null
        if (sinkCommitter == null) {
            log.error("SinkCommitter is null, cannot perform commit");
            for (Committer.CommitRequest<CommitWrapper<CommT>> request : committables) {
                request.signalFailedWithKnownReason(
                        new IOException("SinkCommitter is null"));
            }
            throw new IOException("SinkCommitter is null");
        }

        // Extract commit info from CommitRequest wrappers with null checks
        List<CommT> commitInfos = new ArrayList<>();
        for (Committer.CommitRequest<CommitWrapper<CommT>> request : committables) {
            if (request != null && request.getCommittable() != null) {
                CommT commit = request.getCommittable().getCommit();
                if (commit != null) {
                    commitInfos.add(commit);
                } else {
                    log.warn("Found null commit in committable, skipping");
                }
            } else {
                log.warn("Found null request or committable, skipping");
            }
        }

        if (commitInfos.isEmpty()) {
            log.warn("No valid commit infos found, marking all as failed");
            for (Committer.CommitRequest<CommitWrapper<CommT>> request : committables) {
                if (request != null) {
                    request.signalFailedWithKnownReason(
                            new IOException("No valid commit info found"));
                }
            }
            return;
        }

        try {
            // Call SeaTunnel's commit method
            List<CommT> reCommittable = sinkCommitter.commit(commitInfos);

            if (reCommittable != null && !reCommittable.isEmpty()) {
                log.warn(
                        "SeaTunnel committer returned {} items for re-commit, but Flink 1.20 sink2 API doesn't support re-commit. These will be ignored.",
                        reCommittable.size());
                // In Flink 1.20 sink2 API, we can't return failed commits for retry
                // We mark them as failed with known reason
                for (Committer.CommitRequest<CommitWrapper<CommT>> request : committables) {
                    if (request != null && request.getCommittable() != null) {
                        CommT commit = request.getCommittable().getCommit();
                        if (reCommittable.contains(commit)) {
                            request.signalFailedWithKnownReason(
                                    new IOException(
                                            "Commit failed and re-commit is not supported in Flink 1.20"));
                        } else {
                            request.signalAlreadyCommitted();
                        }
                    }
                }
            } else {
                // All commits succeeded, mark them as committed
                for (Committer.CommitRequest<CommitWrapper<CommT>> request : committables) {
                    if (request != null) {
                        request.signalAlreadyCommitted();
                    }
                }
                log.debug("Successfully committed {} items", committables.size());
            }
        } catch (Exception e) {
            log.error("Error during commit operation", e);
            // Mark all requests as failed
            for (Committer.CommitRequest<CommitWrapper<CommT>> request : committables) {
                if (request != null) {
                    request.signalFailedWithKnownReason(e);
                }
            }
            throw new IOException("Failed to commit data", e);
        }
    }

    @Override
    public void close() throws Exception {
        log.debug("Closing FlinkCommitter");
        // SinkCommitter doesn't have a close method, so nothing to do here
    }
}
