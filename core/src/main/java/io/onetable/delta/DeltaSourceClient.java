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
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import lombok.Builder;
import lombok.extern.log4j.Log4j2;

import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.delta.DeltaHistoryManager;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.actions.Action;
import org.apache.spark.sql.delta.actions.AddFile;
import org.apache.spark.sql.delta.actions.RemoveFile;

import scala.Option;
import io.delta.tables.DeltaTable;

import io.onetable.exception.OneIOException;
import io.onetable.model.*;
import io.onetable.model.CommitsBacklog;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.SchemaCatalog;
import io.onetable.model.schema.SchemaVersion;
import io.onetable.model.storage.FileFormat;
import io.onetable.model.storage.OneDataFile;
import io.onetable.model.storage.OneDataFilesDiff;
import io.onetable.model.storage.OneFileGroup;
import io.onetable.spi.extractor.DataFileIterator;
import io.onetable.spi.extractor.SourceClient;

@Log4j2
@Builder
public class DeltaSourceClient implements SourceClient<Long> {
  @Builder.Default
  private final DeltaDataFileExtractor dataFileExtractor = DeltaDataFileExtractor.builder().build();

  @Builder.Default
  private final DeltaActionsConverter actionsConverter = DeltaActionsConverter.getInstance();

  @Builder.Default
  private final DeltaTableExtractor tableExtractor = DeltaTableExtractor.builder().build();

  private Optional<DeltaIncrementalChangesState> deltaIncrementalChangesState = Optional.empty();

  private final SparkSession sparkSession;
  private final DeltaLog deltaLog;
  private final DeltaTable deltaTable;
  private final String tableName;
  private final String basePath;

  @Override
  public OneTable getTable(Long version) {
    return tableExtractor.table(deltaLog, tableName, version);
  }

  @Override
  public SchemaCatalog getSchemaCatalog(OneTable table, Long version) {
    // TODO: Does not support schema versions for now
    Map<SchemaVersion, OneSchema> schemas = new HashMap<>();
    SchemaVersion schemaVersion = new SchemaVersion(1, "");
    schemas.put(schemaVersion, table.getReadSchema());
    return SchemaCatalog.builder().schemas(schemas).build();
  }

  @Override
  public OneSchema getSchema(OneTable table) {
    return table.getReadSchema();
  }

  @Override
  public OneSnapshot getCurrentSnapshot() {
    DeltaLog deltaLog = DeltaLog.forTable(sparkSession, basePath);
    Snapshot snapshot = deltaLog.snapshot();
    OneTable table = getTable(snapshot.version());
    return OneSnapshot.builder()
        .table(table)
        .schemaCatalog(getSchemaCatalog(table, snapshot.version()))
        .partitionedDataFiles(getOneDataFiles(snapshot, table.getReadSchema()))
        .build();
  }

  @Override
  public TableChange getTableChangeForCommit(Long versionNumber) {
    OneTable tableAtVersion = tableExtractor.table(deltaLog, tableName, versionNumber);
    // Client to call getCommitsBacklog and call this method.
    List<Action> actionsForVersion = getChangesState().getActionsForVersion(versionNumber);
    Snapshot snapshotAtVersion =
        deltaLog.getSnapshotAt(versionNumber, Option.empty(), Option.empty());
    FileFormat fileFormat =
        actionsConverter.convertToOneTableFileFormat(
            snapshotAtVersion.metadata().format().provider());
    Set<OneDataFile> addedFiles = new HashSet<>();
    Set<OneDataFile> removedFiles = new HashSet<>();
    for (Action action : actionsForVersion) {
      if (action instanceof AddFile) {
        addedFiles.add(
            actionsConverter.convertAddActionToOneDataFile(
                (AddFile) action,
                snapshotAtVersion,
                fileFormat,
                tableAtVersion.getPartitioningFields(),
                tableAtVersion.getReadSchema().getFields(),
                true,
                DeltaPartitionExtractor.getInstance(),
                DeltaStatsExtractor.getInstance()));
      } else if (action instanceof RemoveFile) {
        removedFiles.add(
            actionsConverter.convertRemoveActionToOneDataFile(
                (RemoveFile) action,
                snapshotAtVersion,
                fileFormat,
                tableAtVersion.getPartitioningFields(),
                DeltaPartitionExtractor.getInstance()));
      }
    }
    OneDataFilesDiff dataFilesDiff =
        OneDataFilesDiff.builder().filesAdded(addedFiles).filesRemoved(removedFiles).build();
    return TableChange.builder().tableAsOfChange(tableAtVersion).filesDiff(dataFilesDiff).build();
  }

  @Override
  public CommitsBacklog<Long> getCommitsBacklog(
      InstantsForIncrementalSync instantsForIncrementalSync) {
    DeltaHistoryManager.Commit deltaCommitAtLastSyncInstant =
        deltaLog
            .history()
            .getActiveCommitAtTime(
                Timestamp.from(instantsForIncrementalSync.getLastSyncInstant()), true, false, true);
    long versionNumberAtLastSyncInstant = deltaCommitAtLastSyncInstant.version();
    resetState(versionNumberAtLastSyncInstant + 1);
    return CommitsBacklog.<Long>builder()
        .commitsToProcess(getChangesState().getVersionsInSortedOrder())
        .build();
  }

  /*
   * In Delta Lake, each commit is a self-describing one i.e. it contains list of new files while
   * also containing list of files that were deleted. So, vacuum has no special effect on the
   * incremental sync. Hence, existence of commit is the only check required.
   */
  @Override
  public boolean isIncrementalSyncSafeFrom(Instant instant) {
    DeltaHistoryManager.Commit deltaCommitAtOrBeforeInstant =
        deltaLog.history().getActiveCommitAtTime(Timestamp.from(instant), true, false, true);
    // There is a chance earliest commit of the table is returned if the instant is before the
    // earliest commit of the table, hence the additional check.
    Instant deltaCommitInstant = Instant.ofEpochMilli(deltaCommitAtOrBeforeInstant.getTimestamp());
    return deltaCommitInstant.equals(instant) || deltaCommitInstant.isBefore(instant);
  }

  private DeltaIncrementalChangesState getChangesState() {
    return deltaIncrementalChangesState.orElseThrow(
        () -> new IllegalStateException("DeltaIncrementalChangesState is not initialized"));
  }

  private void resetState(long versionToStartFrom) {
    deltaIncrementalChangesState =
        Optional.of(
            DeltaIncrementalChangesState.builder()
                .deltaLog(deltaLog)
                .versionToStartFrom(versionToStartFrom)
                .build());
  }

  private List<OneFileGroup> getOneDataFiles(Snapshot snapshot, OneSchema schema) {
    try (DataFileIterator fileIterator = dataFileExtractor.iterator(snapshot, schema)) {
      List<OneDataFile> dataFiles = new ArrayList<>();
      fileIterator.forEachRemaining(dataFiles::add);
      return OneFileGroup.fromFiles(dataFiles);
    } catch (Exception e) {
      throw new OneIOException("Failed to iterate through Delta data files", e);
    }
  }

  @Override
  public void close() {
    // nothing to close
  }
}
