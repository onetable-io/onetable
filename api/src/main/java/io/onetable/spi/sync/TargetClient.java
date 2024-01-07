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
 
package io.onetable.spi.sync;

import java.util.List;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;

import io.onetable.model.OneTable;
import io.onetable.model.OneTableMetadata;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.storage.OneDataFilesDiff;
import io.onetable.model.storage.OneFileGroup;

/** A client that provides the major functionality for syncing changes to a target system. */
public interface TargetClient {

  /**
   * Syncs the current schema state.
   *
   * @param schema the current schema
   */
  void syncSchema(OneSchema schema);

  /**
   * Syncs the current partition spec.
   *
   * @param partitionSpec the current partition spec
   */
  void syncPartitionSpec(List<OnePartitionField> partitionSpec);

  /**
   * Syncs the {@link OneTableMetadata} to the target for tracking metadata between runs. This is
   * required for incremental sync.
   *
   * @param metadata the current metadata
   */
  void syncMetadata(OneTableMetadata metadata);

  /**
   * Syncs the provided snapshot files to the target system. This method is required to both add and
   * remove files.
   *
   * @param partitionedDataFiles the files to sync, grouped by partition
   */
  void syncFilesForSnapshot(List<OneFileGroup> partitionedDataFiles);

  /**
   * Syncs the changes in files to the target system. This method is required to both add and remove
   * files.
   *
   * @param oneDataFilesDiff the diff that needs to be synced
   */
  void syncFilesForDiff(OneDataFilesDiff oneDataFilesDiff);

  /**
   * Starts the sync and performs any initialization required
   *
   * @param table the table that will be synced
   */
  void beginSync(OneTable table);

  /** Completes the sync and performs any cleanup required. */
  void completeSync();

  /** Returns the onetable metadata persisted in the target */
  Optional<OneTableMetadata> getTableMetadata();

  /** Returns the TableFormat name the client syncs to */
  String getTableFormat();

  /** Initializes the client with provided configuration */
  void init(Configuration configuration);
}
