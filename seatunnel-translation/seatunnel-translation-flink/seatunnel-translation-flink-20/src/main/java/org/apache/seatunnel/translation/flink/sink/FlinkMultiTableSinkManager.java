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
import org.apache.seatunnel.api.sink.SupportMultiTableSinkAggregatedCommitter;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manager for handling multi-table sink operations in Flink 1.20. Since Flink 1.20 sink2 API
 * doesn't support GlobalCommitter, this class provides a workaround for multi-table scenarios.
 *
 * <p>Key features: - Manages multiple table sinks with shared resources - Simulates GlobalCommitter
 * behavior for aggregated commits - Handles multi-table resource sharing - Provides error isolation
 * between tables
 */
@Slf4j
public class FlinkMultiTableSinkManager<CommT, GlobalCommT> {

    private final Map<String, SinkAggregatedCommitter<CommT, GlobalCommT>> aggregatedCommitters;
    private final Map<String, List<CommT>> pendingCommits;
    private final Map<String, Integer> tableCommitCounts;
    private final Object commitLock = new Object();
    private MultiTableResourceManager<Object> resourceManager;
    private volatile boolean initialized = false;
    private volatile boolean closed = false;

    public FlinkMultiTableSinkManager() {
        this.aggregatedCommitters = new ConcurrentHashMap<>();
        this.pendingCommits = new ConcurrentHashMap<>();
        this.tableCommitCounts = new ConcurrentHashMap<>();
    }

    /** Initialize the manager with aggregated committers for each table. */
    public void initialize(Map<String, SinkAggregatedCommitter<CommT, GlobalCommT>> committers) {
        synchronized (commitLock) {
            if (initialized) {
                log.warn("FlinkMultiTableSinkManager already initialized");
                return;
            }

            if (closed) {
                throw new IllegalStateException("Manager has been closed");
            }

            this.aggregatedCommitters.putAll(committers);

            // Initialize commit counters
            for (String tableId : committers.keySet()) {
                tableCommitCounts.put(tableId, 0);
            }

            initializeResourceManager();
            initializeCommitters();
            initialized = true;
            log.info("FlinkMultiTableSinkManager initialized with {} tables", committers.size());
        }
    }

    private void initializeResourceManager() {
        for (Map.Entry<String, SinkAggregatedCommitter<CommT, GlobalCommT>> entry :
                aggregatedCommitters.entrySet()) {
            SinkAggregatedCommitter<CommT, GlobalCommT> committer = entry.getValue();

            if (committer instanceof SupportMultiTableSinkAggregatedCommitter) {
                @SuppressWarnings("unchecked")
                SupportMultiTableSinkAggregatedCommitter<Object> supportCommitter =
                        (SupportMultiTableSinkAggregatedCommitter<Object>) committer;
                resourceManager =
                        supportCommitter.initMultiTableResourceManager(
                                aggregatedCommitters.size(), 1);
                break;
            }
        }

        // Set resource manager for all committers
        if (resourceManager != null) {
            int index = 0;
            for (Map.Entry<String, SinkAggregatedCommitter<CommT, GlobalCommT>> entry :
                    aggregatedCommitters.entrySet()) {
                SinkAggregatedCommitter<CommT, GlobalCommT> committer = entry.getValue();

                if (committer instanceof SupportMultiTableSinkAggregatedCommitter) {
                    @SuppressWarnings("unchecked")
                    SupportMultiTableSinkAggregatedCommitter<Object> supportCommitter =
                            (SupportMultiTableSinkAggregatedCommitter<Object>) committer;
                    supportCommitter.setMultiTableResourceManager(resourceManager, index++);
                }
            }
            log.debug("Resource manager set for {} committers", aggregatedCommitters.size());
        }
    }

    /** Initialize all aggregated committers. */
    private void initializeCommitters() {
        for (Map.Entry<String, SinkAggregatedCommitter<CommT, GlobalCommT>> entry :
                aggregatedCommitters.entrySet()) {
            String tableId = entry.getKey();
            SinkAggregatedCommitter<CommT, GlobalCommT> committer = entry.getValue();

            try {
                committer.init();
                log.debug("Initialized committer for table: {}", tableId);
            } catch (Exception e) {
                log.error("Failed to initialize committer for table: {}", tableId, e);
                throw new RuntimeException(
                        "Failed to initialize committer for table: " + tableId, e);
            }
        }
    }

    /** Add commits for a specific table. */
    public void addCommits(String tableId, List<CommT> commits) {
        synchronized (commitLock) {
            if (!initialized) {
                throw new IllegalStateException("Manager not initialized");
            }

            if (closed) {
                throw new IllegalStateException("Manager has been closed");
            }

            if (commits == null || commits.isEmpty()) {
                log.debug("No commits to add for table: {}", tableId);
                return;
            }

            if (!aggregatedCommitters.containsKey(tableId)) {
                log.warn("No aggregated committer found for table: {}, ignoring commits", tableId);
                return;
            }

            pendingCommits.computeIfAbsent(tableId, k -> new ArrayList<>()).addAll(commits);
            tableCommitCounts.merge(tableId, commits.size(), Integer::sum);
            log.debug(
                    "Added {} commits for table: {}, total pending: {}",
                    commits.size(),
                    tableId,
                    pendingCommits.get(tableId).size());
        }
    }

    /** Check if there are pending commits for any table. */
    public boolean hasPendingCommits() {
        synchronized (commitLock) {
            return pendingCommits.values().stream().anyMatch(list -> !list.isEmpty());
        }
    }

    /** Get the number of pending commits for a specific table. */
    public int getPendingCommitCount(String tableId) {
        synchronized (commitLock) {
            List<CommT> commits = pendingCommits.get(tableId);
            return commits != null ? commits.size() : 0;
        }
    }

    /**
     * Combine and commit all pending commits. This simulates the GlobalCommitter behavior in Flink
     * 1.20.
     */
    public List<CommT> combineAndCommit() throws IOException {
        synchronized (commitLock) {
            if (!initialized) {
                throw new IllegalStateException("Manager not initialized");
            }

            if (closed) {
                throw new IllegalStateException("Manager has been closed");
            }

            if (pendingCommits.isEmpty()) {
                log.debug("No pending commits to process");
                return new ArrayList<>();
            }

            List<CommT> failedCommits = new ArrayList<>();
            Map<String, GlobalCommT> globalCommits = new HashMap<>();
            Map<String, List<CommT>> currentCommits = new HashMap<>();

            // Create a snapshot of current pending commits
            for (Map.Entry<String, List<CommT>> entry : pendingCommits.entrySet()) {
                String tableId = entry.getKey();
                List<CommT> commits = new ArrayList<>(entry.getValue());
                if (!commits.isEmpty()) {
                    currentCommits.put(tableId, commits);
                }
            }

            log.debug("Processing commits for {} tables", currentCommits.size());

            // Step 1: Combine commits for each table
            for (Map.Entry<String, List<CommT>> entry : currentCommits.entrySet()) {
                String tableId = entry.getKey();
                List<CommT> commits = entry.getValue();

                SinkAggregatedCommitter<CommT, GlobalCommT> committer =
                        aggregatedCommitters.get(tableId);
                if (committer == null) {
                    log.warn("No aggregated committer found for table: {}", tableId);
                    failedCommits.addAll(commits);
                    continue;
                }

                try {
                    log.debug("Combining {} commits for table: {}", commits.size(), tableId);
                    GlobalCommT globalCommit = committer.combine(commits);
                    if (globalCommit != null) {
                        globalCommits.put(tableId, globalCommit);
                        log.debug("Successfully combined commits for table: {}", tableId);
                    } else {
                        log.warn("Combine returned null for table: {}", tableId);
                        failedCommits.addAll(commits);
                    }
                } catch (Exception e) {
                    log.error("Failed to combine commits for table: {}", tableId, e);
                    failedCommits.addAll(commits);
                }
            }

            // Step 2: Commit global commits
            int successfulCommits = 0;
            for (Map.Entry<String, GlobalCommT> entry : globalCommits.entrySet()) {
                String tableId = entry.getKey();
                GlobalCommT globalCommit = entry.getValue();

                SinkAggregatedCommitter<CommT, GlobalCommT> committer =
                        aggregatedCommitters.get(tableId);
                try {
                    log.debug("Committing global commit for table: {}", tableId);
                    List<GlobalCommT> reCommittable =
                            committer.commit(java.util.Collections.singletonList(globalCommit));

                    if (reCommittable != null && !reCommittable.isEmpty()) {
                        log.warn(
                                "Table {} has failed global commits, but re-commit is not supported in Flink 1.20",
                                tableId);
                        // Add original commits back to failed list
                        List<CommT> originalCommits = currentCommits.get(tableId);
                        if (originalCommits != null) {
                            failedCommits.addAll(originalCommits);
                        }
                    } else {
                        // Successful commit, remove from pending
                        List<CommT> originalCommits = currentCommits.get(tableId);
                        if (originalCommits != null) {
                            pendingCommits.get(tableId).removeAll(originalCommits);
                            successfulCommits++;
                            log.debug(
                                    "Successfully committed {} commits for table: {}",
                                    originalCommits.size(),
                                    tableId);
                        }
                    }
                } catch (Exception e) {
                    log.error("Failed to commit global commit for table: {}", tableId, e);
                    List<CommT> originalCommits = currentCommits.get(tableId);
                    if (originalCommits != null) {
                        failedCommits.addAll(originalCommits);
                    }
                }
            }

            // Clean up empty pending commit lists
            pendingCommits.entrySet().removeIf(entry -> entry.getValue().isEmpty());

            log.info(
                    "Commit completed: {} tables successful, {} commits failed",
                    successfulCommits,
                    failedCommits.size());
            return failedCommits;
        }
    }

    /** Get the resource manager instance. */
    public MultiTableResourceManager<Object> getResourceManager() {
        return resourceManager;
    }

    /** Check if the manager is initialized. */
    public boolean isInitialized() {
        return initialized;
    }

    /** Check if the manager is closed. */
    public boolean isClosed() {
        return closed;
    }

    /** Get statistics about the manager state. */
    public Map<String, Object> getStatistics() {
        synchronized (commitLock) {
            Map<String, Object> stats = new HashMap<>();
            stats.put("initialized", initialized);
            stats.put("closed", closed);
            stats.put("tableCount", aggregatedCommitters.size());
            stats.put(
                    "totalPendingCommits",
                    pendingCommits.values().stream().mapToInt(List::size).sum());
            stats.put("tableCommitCounts", new HashMap<>(tableCommitCounts));
            return stats;
        }
    }

    /** Close the manager and release resources. */
    public void close() throws Exception {
        synchronized (commitLock) {
            if (closed) {
                log.debug("FlinkMultiTableSinkManager already closed");
                return;
            }

            log.info("Closing FlinkMultiTableSinkManager...");
            Exception firstException = null;

            // Try to commit any remaining pending commits before closing
            if (hasPendingCommits()) {
                log.warn("Closing manager with pending commits, attempting final commit");
                try {
                    List<CommT> failedCommits = combineAndCommit();
                    if (!failedCommits.isEmpty()) {
                        log.warn("Failed to commit {} items during close", failedCommits.size());
                    }
                } catch (Exception e) {
                    log.error("Error during final commit on close", e);
                    firstException = e;
                }
            }

            // Close resource manager
            if (resourceManager != null) {
                try {
                    resourceManager.close();
                    log.debug("Resource manager closed");
                } catch (Exception e) {
                    log.error("Error closing resource manager", e);
                    if (firstException == null) {
                        firstException = e;
                    }
                }
            }

            // Close all aggregated committers
            for (Map.Entry<String, SinkAggregatedCommitter<CommT, GlobalCommT>> entry :
                    aggregatedCommitters.entrySet()) {
                String tableId = entry.getKey();
                SinkAggregatedCommitter<CommT, GlobalCommT> committer = entry.getValue();
                try {
                    committer.close();
                    log.debug("Closed committer for table: {}", tableId);
                } catch (Exception e) {
                    log.error("Error closing aggregated committer for table: {}", tableId, e);
                    if (firstException == null) {
                        firstException = e;
                    }
                }
            }

            // Clear all data structures
            aggregatedCommitters.clear();
            pendingCommits.clear();
            tableCommitCounts.clear();
            resourceManager = null;
            initialized = false;
            closed = true;

            log.info("FlinkMultiTableSinkManager closed");

            // Throw the first exception if any occurred
            if (firstException != null) {
                throw firstException;
            }
        }
    }
}
