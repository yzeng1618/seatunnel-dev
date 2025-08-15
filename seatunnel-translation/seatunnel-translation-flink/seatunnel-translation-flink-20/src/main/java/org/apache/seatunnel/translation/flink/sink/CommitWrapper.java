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

import lombok.extern.slf4j.Slf4j;

/**
 * The commit message wrapper, which is used to wrapper the different commit messages and unify the
 * different implementations of {@link CommitT}. This version includes null safety checks for Flink 2.0.
 *
 * @param <CommitT> The generic type of commit message
 */
@Slf4j
public class CommitWrapper<CommitT> {
    private final CommitT commit;

    public CommitWrapper(CommitT commit) {
        if (commit == null) {
            log.warn("Creating CommitWrapper with null commit");
        }
        this.commit = commit;
    }

    public CommitT getCommit() {
        return commit;
    }

    public boolean hasCommit() {
        return commit != null;
    }

    @Override
    public String toString() {
        return "CommitWrapper{" +
                "commit=" + (commit != null ? commit.toString() : "null") +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommitWrapper<?> that = (CommitWrapper<?>) o;
        return commit != null ? commit.equals(that.commit) : that.commit == null;
    }

    @Override
    public int hashCode() {
        return commit != null ? commit.hashCode() : 0;
    }
}
