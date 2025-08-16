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

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;

import org.apache.flink.api.connector.sink2.Committer;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Wrapper for SinkAggregatedCommitter to work with Flink 1.20 sink2 API. This provides a bridge
 * between Flink's Committer interface and SeaTunnel's SinkAggregatedCommitter.
 */
@Slf4j
public class FlinkAggregatedCommitterWrapper<CommT, GlobalCommT>
        implements Committer<CommitWrapper<CommT>> {

    private final SinkAggregatedCommitter<CommT, GlobalCommT> aggregatedCommitter;

    public FlinkAggregatedCommitterWrapper(
            SinkAggregatedCommitter<CommT, GlobalCommT> aggregatedCommitter) {
        this.aggregatedCommitter = aggregatedCommitter;

        // Initialize the aggregated committer
        try {
            this.aggregatedCommitter.init();
            log.debug("FlinkAggregatedCommitterWrapper created and initialized");
        } catch (Exception e) {
            log.error("Failed to initialize aggregated committer", e);
            throw new RuntimeException("Failed to initialize aggregated committer", e);
        }
    }

    @Override
    public void commit(Collection<Committer.CommitRequest<CommitWrapper<CommT>>> committables)
            throws IOException, InterruptedException {
        if (committables == null || committables.isEmpty()) {
            log.debug("No committables to commit");
            return;
        }

        log.debug("Committing {} committables using aggregated committer", committables.size());

        // Extract commit info from CommitRequest wrappers with null checks
        List<CommT> commitInfos = new ArrayList<>();
        List<Committer.CommitRequest<CommitWrapper<CommT>>> validRequests = new ArrayList<>();

        for (Committer.CommitRequest<CommitWrapper<CommT>> request : committables) {
            if (request != null && request.getCommittable() != null) {
                CommT commit = request.getCommittable().getCommit();
                if (commit != null) {
                    commitInfos.add(commit);
                    validRequests.add(request);
                } else {
                    log.warn("Found null commit in committable, marking as failed");
                    request.signalFailedWithKnownReason(
                            new IOException("Null commit in committable"));
                }
            } else {
                log.warn("Found null request or committable, skipping");
                if (request != null) {
                    request.signalFailedWithKnownReason(new IOException("Null committable"));
                }
            }
        }

        if (commitInfos.isEmpty()) {
            log.warn("No valid commit infos found");
            return;
        }

        try {
            // Step 1: Combine commits into global commit
            GlobalCommT globalCommit = aggregatedCommitter.combine(commitInfos);

            if (globalCommit == null) {
                log.warn("Aggregated committer returned null global commit");
                for (Committer.CommitRequest<CommitWrapper<CommT>> request : validRequests) {
                    request.signalFailedWithKnownReason(
                            new IOException("Aggregated committer returned null global commit"));
                }
                return;
            }

            // Step 2: Commit the global commit
            List<GlobalCommT> reCommittable =
                    aggregatedCommitter.commit(java.util.Collections.singletonList(globalCommit));

            if (reCommittable != null && !reCommittable.isEmpty()) {
                log.warn(
                        "Aggregated committer returned {} items for re-commit, but Flink 1.20 sink2 API doesn't support re-commit",
                        reCommittable.size());
                // Mark all as failed since we can't re-commit
                for (Committer.CommitRequest<CommitWrapper<CommT>> request : validRequests) {
                    request.signalFailedWithKnownReason(
                            new IOException(
                                    "Commit failed and re-commit is not supported in Flink 1.20"));
                }
            } else {
                // All commits succeeded
                for (Committer.CommitRequest<CommitWrapper<CommT>> request : validRequests) {
                    request.signalAlreadyCommitted();
                }
                log.debug(
                        "Successfully committed {} items using aggregated committer",
                        validRequests.size());
            }

        } catch (Exception e) {
            log.error("Error during aggregated commit operation", e);
            // Mark all requests as failed
            for (Committer.CommitRequest<CommitWrapper<CommT>> request : validRequests) {
                request.signalFailedWithKnownReason(e);
            }
            throw new IOException("Failed to commit using aggregated committer", e);
        }
    }

    @Override
    public void close() throws Exception {
        log.debug("Closing FlinkAggregatedCommitterWrapper");
        try {
            if (aggregatedCommitter != null) {
                aggregatedCommitter.close();
                log.debug("Aggregated committer closed successfully");
            }
        } catch (Exception e) {
            log.error("Error closing aggregated committer", e);
            throw e;
        }
    }
}
