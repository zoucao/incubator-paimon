/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

/**
 * {@link StartingScanner} for the {@link CoreOptions.StartupMode#FROM_SNAPSHOT} startup mode of a
 * streaming read.
 */
public class ContinuousFromSnapshotStartingScanner implements StartingScanner {

    private final long snapshotId;

    public ContinuousFromSnapshotStartingScanner(long snapshotId) {
        this.snapshotId = snapshotId;
    }

    @Override
    @Nullable
    public Result scan(SnapshotManager snapshotManager, SnapshotSplitReader snapshotSplitReader) {
        Long earliestSnapshotId = snapshotManager.earliestSnapshotId();
        if (earliestSnapshotId == null) {
            return null;
        }
        // We should use `snapshotId - 1` here to start to scan delta data from specific snapshot
        // id. If the snapshotId < earliestSnapshotId, start to scan from the earliest.
        return new Result(
                snapshotId >= earliestSnapshotId ? snapshotId - 1 : earliestSnapshotId - 1);
    }
}
