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
 * Simplified aggregated committer for Flink 1.20 that directly wraps SeaTunnel's
 * SinkAggregatedCommitter. This is a much simpler approach compared to FlinkMultiTableSinkManager.
 */
@Slf4j
public class FlinkSimpleAggregatedCommitter<CommT, GlobalCommT>
        implements Committer<CommitWrapper<CommT>> {

    private final SinkAggregatedCommitter<CommT, GlobalCommT> aggregatedCommitter;
    private MultiTableResourceManager<Object> resourceManager;

    public FlinkSimpleAggregatedCommitter(
            SinkAggregatedCommitter<CommT, GlobalCommT> aggregatedCommitter) {
        this.aggregatedCommitter = aggregatedCommitter;

        // Initialize resource manager if supported
        if (aggregatedCommitter instanceof SupportResourceShare) {
            @SuppressWarnings("unchecked")
            SupportResourceShare<Object> supportCommitter =
                    (SupportResourceShare<Object>) aggregatedCommitter;
            resourceManager = supportCommitter.initMultiTableResourceManager(1, 1);
            supportCommitter.setMultiTableResourceManager(resourceManager, 0);
        }

        // Initialize the aggregated committer
        try {
            aggregatedCommitter.init();
            log.debug("FlinkSimpleAggregatedCommitter initialized");
        } catch (Exception e) {
            log.error("Failed to initialize aggregated committer", e);
            throw new RuntimeException("Failed to initialize aggregated committer", e);
        }
    }

    @Override
    public void commit(Collection<Committer.CommitRequest<CommitWrapper<CommT>>> committables)
            throws IOException, InterruptedException {
        if (committables == null || committables.isEmpty()) {
            log.debug("No committables to commit - this is normal for some scenarios");
            // Even when committables is empty, we should not return directly
            // because Flink may still expect us to handle the commit request properly
            // However, since there are no committables, there's nothing to process
            return;
        }

        log.debug(
                "Committing {} committables using simple aggregated committer",
                committables.size());

        // Enhanced logging for schema evolution scenarios
        if (log.isDebugEnabled()) {
            committables.forEach(
                    request -> {
                        if (request != null && request.getCommittable() != null) {
                            log.debug(
                                    "Processing committable: {}",
                                    request.getCommittable().getCommit());
                        }
                    });
        }

        // Extract commit info from CommitRequest wrappers
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
            log.warn("No valid commit infos found, but will signal success for empty commits");
            // Even if no commit infos, we should signal success for all valid requests
            // This handles cases where all committables are empty but requests need to be
            // acknowledged
            for (Committer.CommitRequest<CommitWrapper<CommT>> request : validRequests) {
                request.signalAlreadyCommitted();
            }
            return;
        }

        try {
            // Step 1: Combine commits into global commit with schema evolution support
            log.debug("Combining {} commit infos into global commit", commitInfos.size());
            GlobalCommT globalCommit = combineWithSchemaEvolutionSupport(commitInfos);

            if (globalCommit == null) {
                log.warn(
                        "Aggregated committer returned null global commit, treating as successful empty commit");
                // Some aggregated committers may return null for empty commits, which should be
                // treated as success
                // This is common in schema evolution scenarios where some checkpoints may be empty
                for (Committer.CommitRequest<CommitWrapper<CommT>> request : validRequests) {
                    request.signalAlreadyCommitted();
                }
                log.debug("Successfully handled {} empty commits", validRequests.size());
                return;
            }

            log.debug("Successfully combined commits into global commit: {}", globalCommit);

            // Step 2: Commit the global commit
            log.debug("Committing global commit to aggregated committer");
            List<GlobalCommT> reCommittable =
                    aggregatedCommitter.commit(java.util.Collections.singletonList(globalCommit));

            if (reCommittable != null && !reCommittable.isEmpty()) {
                log.error(
                        "Aggregated committer returned {} items for re-commit, but Flink 1.20 sink2 API doesn't support re-commit. "
                                + "This may indicate a transaction failure in schema evolution scenario.",
                        reCommittable.size());

                // In schema evolution scenarios, transaction failures can be critical
                // We need to fail fast to maintain data consistency
                IOException commitException =
                        new IOException(
                                String.format(
                                        "Transaction commit failed with %d items requiring re-commit. "
                                                + "Re-commit is not supported in Flink 1.20. This may cause data inconsistency in schema evolution scenarios.",
                                        reCommittable.size()));

                // Mark all as failed since we can't re-commit
                for (Committer.CommitRequest<CommitWrapper<CommT>> request : validRequests) {
                    request.signalFailedWithKnownReason(commitException);
                }

                // Log the failed global commit for debugging
                log.error("Failed global commit details: {}", globalCommit);

            } else {
                // All commits succeeded
                log.debug(
                        "Global commit succeeded, signaling success for all {} requests",
                        validRequests.size());
                for (Committer.CommitRequest<CommitWrapper<CommT>> request : validRequests) {
                    request.signalAlreadyCommitted();
                }
                log.info(
                        "Successfully committed {} items using simple aggregated committer",
                        validRequests.size());
            }

        } catch (Exception e) {
            log.error(
                    "Error during simple aggregated commit operation. This is critical in schema evolution scenarios.",
                    e);

            // Enhanced error information for debugging schema evolution issues
            log.error(
                    "Commit context - Total committables: {}, Valid requests: {}, Commit infos: {}",
                    committables.size(),
                    validRequests.size(),
                    commitInfos.size());

            // Mark all requests as failed with detailed error information
            IOException detailedException =
                    new IOException(
                            String.format(
                                    "Aggregated commit failed during schema evolution. "
                                            + "Processed %d committables, %d valid requests. Original error: %s",
                                    committables.size(), validRequests.size(), e.getMessage()),
                            e);

            for (Committer.CommitRequest<CommitWrapper<CommT>> request : validRequests) {
                request.signalFailedWithKnownReason(detailedException);
            }

            // Re-throw with enhanced context
            throw new IOException(
                    "Critical failure in aggregated committer during schema evolution", e);
        }
    }

    /**
     * Validates commit infos for potential schema evolution issues. This method helps identify
     * patterns that might indicate schema evolution problems.
     */
    private void validateCommitInfosForSchemaEvolution(List<CommT> commitInfos) {
        if (commitInfos == null || commitInfos.isEmpty()) {
            return;
        }

        // Log commit info patterns that might indicate schema evolution
        if (log.isDebugEnabled()) {
            log.debug(
                    "Validating {} commit infos for schema evolution patterns", commitInfos.size());

            // Check for potential schema evolution indicators
            for (int i = 0; i < commitInfos.size(); i++) {
                CommT commitInfo = commitInfos.get(i);
                if (commitInfo != null) {
                    log.debug("Commit info [{}]: {}", i, commitInfo.toString());

                    // Additional validation can be added here based on specific commit info types
                    // For example, checking for DDL operations, table structure changes, etc.
                }
            }
        }
    }

    /** Enhanced combine operation with schema evolution awareness. */
    private GlobalCommT combineWithSchemaEvolutionSupport(List<CommT> commitInfos)
            throws Exception {
        // Validate commit infos before combining
        validateCommitInfosForSchemaEvolution(commitInfos);

        // Perform the actual combine operation
        GlobalCommT globalCommit = aggregatedCommitter.combine(commitInfos);

        // Log the result for schema evolution debugging
        if (globalCommit != null) {
            log.debug(
                    "Successfully combined {} commit infos into global commit for schema evolution scenario",
                    commitInfos.size());
        } else {
            log.debug(
                    "Combine operation returned null - this may be normal for empty commits in schema evolution");
        }

        return globalCommit;
    }

    @Override
    public void close() throws Exception {
        log.debug("Closing FlinkSimpleAggregatedCommitter");

        Exception firstException = null;

        try {
            if (aggregatedCommitter != null) {
                aggregatedCommitter.close();
                log.debug("Aggregated committer closed successfully");
            }
        } catch (Exception e) {
            log.error("Error closing aggregated committer", e);
            firstException = e;
        }

        try {
            if (resourceManager != null) {
                resourceManager.close();
                log.debug("Resource manager closed successfully");
            }
        } catch (Exception e) {
            log.error("Error closing resource manager", e);
            if (firstException == null) {
                firstException = e;
            }
        }

        if (firstException != null) {
            throw firstException;
        }
    }
}
