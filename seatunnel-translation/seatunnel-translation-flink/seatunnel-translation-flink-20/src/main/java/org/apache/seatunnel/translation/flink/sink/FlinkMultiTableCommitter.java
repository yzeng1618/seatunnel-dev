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

import org.apache.flink.api.connector.sink2.Committer;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Committer implementation that handles multi-table commits using FlinkMultiTableSinkManager. This
 * provides a bridge between Flink 1.20 sink2 API and SeaTunnel's aggregated commit mechanism.
 */
@Slf4j
public class FlinkMultiTableCommitter<CommT, GlobalCommT>
        implements Committer<CommitWrapper<CommT>> {

    private final FlinkMultiTableSinkManager<CommT, GlobalCommT> multiTableManager;

    public FlinkMultiTableCommitter(
            FlinkMultiTableSinkManager<CommT, GlobalCommT> multiTableManager) {
        this.multiTableManager = multiTableManager;
        log.debug("FlinkMultiTableCommitter created");
    }

    @Override
    public void commit(Collection<Committer.CommitRequest<CommitWrapper<CommT>>> committables)
            throws IOException, InterruptedException {
        if (committables == null || committables.isEmpty()) {
            log.debug("No committables to commit");
            return;
        }

        if (!multiTableManager.isInitialized()) {
            log.error("Multi-table manager is not initialized");
            for (Committer.CommitRequest<CommitWrapper<CommT>> request : committables) {
                if (request != null) {
                    request.signalFailedWithKnownReason(
                            new IOException("Multi-table manager is not initialized"));
                }
            }
            throw new IOException("Multi-table manager is not initialized");
        }

        if (multiTableManager.isClosed()) {
            log.error("Multi-table manager is closed");
            for (Committer.CommitRequest<CommitWrapper<CommT>> request : committables) {
                if (request != null) {
                    request.signalFailedWithKnownReason(
                            new IOException("Multi-table manager is closed"));
                }
            }
            throw new IOException("Multi-table manager is closed");
        }

        log.debug("Committing {} committables using multi-table manager", committables.size());

        // Group commits by table ID
        Map<String, List<CommT>> commitsByTable = new HashMap<>();
        Map<CommT, Committer.CommitRequest<CommitWrapper<CommT>>> commitToRequestMap =
                new HashMap<>();

        for (Committer.CommitRequest<CommitWrapper<CommT>> request : committables) {
            if (request == null || request.getCommittable() == null) {
                log.warn("Found null request or committable, skipping");
                continue;
            }

            CommitWrapper<CommT> wrapper = request.getCommittable();
            CommT commit = wrapper.getCommit();

            if (commit == null) {
                log.warn("Found null commit in wrapper, marking as failed");
                request.signalFailedWithKnownReason(new IOException("Null commit in wrapper"));
                continue;
            }

            // For now, we use a default table ID since we don't have table information in the
            // commit
            // In a real multi-table scenario, the commit object should contain table information
            String tableId = extractTableId(commit);

            commitsByTable.computeIfAbsent(tableId, k -> new ArrayList<>()).add(commit);
            commitToRequestMap.put(commit, request);
        }

        if (commitsByTable.isEmpty()) {
            log.warn("No valid commits found");
            return;
        }

        // Add commits to multi-table manager
        for (Map.Entry<String, List<CommT>> entry : commitsByTable.entrySet()) {
            String tableId = entry.getKey();
            List<CommT> commits = entry.getValue();

            try {
                multiTableManager.addCommits(tableId, commits);
                log.debug("Added {} commits for table: {}", commits.size(), tableId);
            } catch (Exception e) {
                log.error("Failed to add commits for table: {}", tableId, e);
                // Mark all commits for this table as failed
                for (CommT commit : commits) {
                    Committer.CommitRequest<CommitWrapper<CommT>> request =
                            commitToRequestMap.get(commit);
                    if (request != null) {
                        request.signalFailedWithKnownReason(e);
                    }
                }
            }
        }

        // Perform the actual commit
        try {
            List<CommT> failedCommits = multiTableManager.combineAndCommit();

            // Signal success/failure for each commit
            for (Map.Entry<String, List<CommT>> entry : commitsByTable.entrySet()) {
                List<CommT> commits = entry.getValue();

                for (CommT commit : commits) {
                    Committer.CommitRequest<CommitWrapper<CommT>> request =
                            commitToRequestMap.get(commit);
                    if (request != null) {
                        if (failedCommits.contains(commit)) {
                            request.signalFailedWithKnownReason(
                                    new IOException("Commit failed during multi-table commit"));
                        } else {
                            request.signalAlreadyCommitted();
                        }
                    }
                }
            }

            int successfulCommits = committables.size() - failedCommits.size();
            log.info(
                    "Multi-table commit completed: {} successful, {} failed",
                    successfulCommits,
                    failedCommits.size());

        } catch (Exception e) {
            log.error("Error during multi-table commit", e);
            // Mark all remaining commits as failed
            for (Committer.CommitRequest<CommitWrapper<CommT>> request :
                    commitToRequestMap.values()) {
                if (request != null) {
                    request.signalFailedWithKnownReason(e);
                }
            }
            throw new IOException("Failed to perform multi-table commit", e);
        }
    }

    /**
     * Extract table ID from commit object. This is a placeholder implementation - in a real
     * scenario, the commit object should contain table identification information.
     */
    private String extractTableId(CommT commit) {
        // For now, return a default table ID
        // In a real implementation, this would extract the table ID from the commit object
        return "default_table";
    }

    @Override
    public void close() throws Exception {
        log.debug("Closing FlinkMultiTableCommitter");
        // The multi-table manager will be closed by the sink
    }
}
