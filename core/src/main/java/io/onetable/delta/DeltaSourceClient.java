/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package io.onetable.delta;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import lombok.extern.log4j.Log4j2;

import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.delta.DeltaHistoryManager;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.actions.Action;
import org.apache.spark.sql.delta.actions.AddFile;
import org.apache.spark.sql.delta.actions.RemoveFile;

import io.delta.tables.DeltaTable;

import io.onetable.model.CurrentCommitState;
import io.onetable.model.InstantsForIncrementalSync;
import io.onetable.model.OneSnapshot;
import io.onetable.model.OneTable;
import io.onetable.model.TableChange;
import io.onetable.model.schema.SchemaCatalog;
import io.onetable.spi.extractor.SourceClient;

@Log4j2
public class DeltaSourceClient implements SourceClient<Long> {
  private final SparkSession sparkSession;
  private final DeltaLog deltaLog;
  private final DeltaTable deltaTable;
  private DeltaIncrementalChangesCacheStore deltaIncrementalChangesCacheStore;

  public DeltaSourceClient(SparkSession sparkSession, String basePath) {
    this.sparkSession = sparkSession;
    this.deltaLog = DeltaLog.forTable(sparkSession, basePath);
    this.deltaTable = DeltaTable.forPath(sparkSession, basePath);
  }

  @Override
  public OneTable getTable(Long versionNumber) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public SchemaCatalog getSchemaCatalog(OneTable table, Long versionNumber) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public OneSnapshot getCurrentSnapshot() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public TableChange getTableChangeForCommit(Long versionNumber) {
    // Expects client to call getCurrentCommitState and call this method.
    List<Action> actionsForVersion =
        deltaIncrementalChangesCacheStore.getActionsForVersion(versionNumber);
    List<AddFile> addFileActions = new ArrayList<>();
    List<RemoveFile> removeFileActions = new ArrayList<>();
    for (Action action : actionsForVersion) {
      if (action instanceof AddFile) {
        addFileActions.add((AddFile) action);
      } else if (action instanceof RemoveFile) {
        removeFileActions.add((RemoveFile) action);
      }
    }
    // TODO(vamshigv): Handle no updates to add file or remove files.
    return null;
  }

  @Override
  public CurrentCommitState<Long> getCurrentCommitState(
      InstantsForIncrementalSync instantsForIncrementalSync) {
    DeltaHistoryManager.Commit deltaCommitAtLastSyncInstant =
        deltaLog
            .history()
            .getActiveCommitAtTime(
                Timestamp.from(instantsForIncrementalSync.getLastSyncInstant()), true, false, true);
    Long versionNumberAtLastSyncInstant = deltaCommitAtLastSyncInstant.version();
    deltaIncrementalChangesCacheStore.initializeOrReload(deltaLog, versionNumberAtLastSyncInstant);
    return CurrentCommitState.<Long>builder()
        .commitsToProcess(deltaIncrementalChangesCacheStore.getVersionsInSortedOrder())
        .build();
  }
}
