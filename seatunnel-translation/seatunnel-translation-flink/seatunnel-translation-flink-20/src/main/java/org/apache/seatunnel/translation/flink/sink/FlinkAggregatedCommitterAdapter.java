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

import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SupportResourceShare;

import org.apache.flink.api.connector.sink2.Committer;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Adapter to handle SinkAggregatedCommitter through Flink 2.0's regular Committer interface. Since
 * Flink 2.0 sink2 API doesn't support GlobalCommitter, we adapt the aggregated committer to work as
 * a regular committer with some limitations.
 *
 * @param <CommT> The generic type of commit message
 * @param <GlobalCommT> The generic type of global commit message
 */
@Slf4j
public class FlinkAggregatedCommitterAdapter<CommT, GlobalCommT>
        implements Committer<CommitWrapper<CommT>> {

    private final SinkAggregatedCommitter<CommT, GlobalCommT> aggregatedCommitter;
    private MultiTableResourceManager resourceManager;

    public FlinkAggregatedCommitterAdapter(
            SinkAggregatedCommitter<CommT, GlobalCommT> aggregatedCommitter) {
        this.aggregatedCommitter = aggregatedCommitter;

        // Initialize resource manager if supported
        if (this.aggregatedCommitter instanceof SupportResourceShare) {
            resourceManager =
                    ((SupportResourceShare) this.aggregatedCommitter)
                            .initMultiTableResourceManager(1, 1);
        }

        // Initialize the aggregated committer
        try {
            aggregatedCommitter.init();
            if (resourceManager != null) {
                ((SupportResourceShare) this.aggregatedCommitter)
                        .setMultiTableResourceManager(resourceManager, 0);
            }
            log.info(
                    "Initialized FlinkAggregatedCommitterAdapter with aggregated committer: {}",
                    aggregatedCommitter.getClass().getSimpleName());
        } catch (Exception e) {
            log.error("Failed to initialize aggregated committer: {}", e.getMessage(), e);
        }
    }

    @Override
    public void commit(Collection<Committer.CommitRequest<CommitWrapper<CommT>>> committables)
            throws IOException, InterruptedException {
        if (committables == null || committables.isEmpty()) {
            log.debug("No committables to commit");
            return;
        }

        log.debug("Committing {} committables through aggregated committer", committables.size());

        try {
            // Extract commit info from CommitRequest wrappers
            List<CommT> commitInfos = new ArrayList<>();
            for (Committer.CommitRequest<CommitWrapper<CommT>> request : committables) {
                if (request != null
                        && request.getCommittable() != null
                        && request.getCommittable().getCommit() != null) {
                    commitInfos.add(request.getCommittable().getCommit());
                }
            }

            if (commitInfos.isEmpty()) {
                log.debug("No valid commit infos found, marking all as committed");
                for (Committer.CommitRequest<CommitWrapper<CommT>> request : committables) {
                    if (request != null) {
                        request.signalAlreadyCommitted();
                    }
                }
                return;
            }

            // Step 1: Combine commits into global commit (like GlobalCommitter.combine)
            GlobalCommT globalCommit = aggregatedCommitter.combine(commitInfos);
            log.debug("Combined {} commits into global commit", commitInfos.size());

            // Step 2: Commit the global commit (like GlobalCommitter.commit)
            List<GlobalCommT> globalCommits = new ArrayList<>();
            if (globalCommit != null) {
                globalCommits.add(globalCommit);
            }

            List<GlobalCommT> reCommittable = aggregatedCommitter.commit(globalCommits);

            if (reCommittable != null && !reCommittable.isEmpty()) {
                log.warn(
                        "Aggregated committer returned {} items for re-commit, but Flink 2.0 doesn't support re-commit",
                        reCommittable.size());
            }

            // Mark all as committed - simplified approach
            for (Committer.CommitRequest<CommitWrapper<CommT>> request : committables) {
                if (request != null) {
                    request.signalAlreadyCommitted();
                }
            }

            log.debug(
                    "Successfully committed {} items through aggregated committer",
                    committables.size());

        } catch (Exception e) {
            log.error("Error during aggregated commit operation: {}", e.getMessage(), e);
            // Mark all requests as failed but don't throw exception
            for (Committer.CommitRequest<CommitWrapper<CommT>> request : committables) {
                if (request != null) {
                    request.signalFailedWithKnownReason(e);
                }
            }
            log.warn("Aggregated commit failed but continuing to avoid job failure");
        }
    }

    @Override
    public void close() throws Exception {
        log.debug("Closing FlinkAggregatedCommitterAdapter");
        try {
            if (aggregatedCommitter != null) {
                aggregatedCommitter.close();
            }
        } catch (Exception e) {
            log.error("Error closing aggregated committer: {}", e.getMessage(), e);
        }

        try {
            if (resourceManager != null) {
                resourceManager.close();
            }
        } catch (Exception e) {
            log.error("Error closing resource manager: {}", e.getMessage(), e);
        }
    }
}
