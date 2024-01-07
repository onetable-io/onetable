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
 
package io.onetable.iceberg;

import static io.onetable.model.OneTableMetadata.INFLIGHT_COMMITS_TO_CONSIDER_FOR_NEXT_SYNC_PROP;
import static io.onetable.model.OneTableMetadata.ONETABLE_LAST_INSTANT_SYNCED_PROP;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotFoundException;

import io.onetable.client.PerTableConfig;
import io.onetable.model.OneTable;
import io.onetable.model.OneTableMetadata;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.storage.OneDataFilesDiff;
import io.onetable.model.storage.OneFileGroup;
import io.onetable.model.storage.TableFormat;
import io.onetable.spi.sync.TargetClient;

@Log4j2
public class IcebergClient implements TargetClient {
  private static final String METADATA_DIR_PATH = "/metadata/";
  private IcebergSchemaExtractor schemaExtractor;
  private IcebergSchemaSync schemaSync;
  private IcebergPartitionSpecExtractor partitionSpecExtractor;
  private IcebergPartitionSpecSync partitionSpecSync;
  private IcebergDataFileUpdatesSync dataFileUpdatesExtractor;
  private IcebergTableManager tableManager;
  private String tableName;
  private String[] namespace;
  private String basePath;
  private TableIdentifier tableIdentifier;
  private IcebergCatalogConfig catalogConfig;
  private Configuration configuration;
  private int snapshotRetentionInHours;
  private Transaction transaction;
  private Table table;
  private OneTable internalTableState;

  public IcebergClient() {}

  IcebergClient(
      PerTableConfig perTableConfig,
      Configuration configuration,
      IcebergSchemaExtractor schemaExtractor,
      IcebergSchemaSync schemaSync,
      IcebergPartitionSpecExtractor partitionSpecExtractor,
      IcebergPartitionSpecSync partitionSpecSync,
      IcebergDataFileUpdatesSync dataFileUpdatesExtractor,
      IcebergTableManager tableManager) {
    this.tableName = perTableConfig.getTableName();
    this.basePath = perTableConfig.getTableBasePath();
    this.configuration = configuration;
    this.snapshotRetentionInHours = perTableConfig.getTargetMetadataRetentionInHours();
    namespace = perTableConfig.getNamespace();
    this.tableIdentifier =
        namespace == null
            ? TableIdentifier.of(tableName)
            : TableIdentifier.of(Namespace.of(namespace), tableName);
    this.tableManager = tableManager;
    this.catalogConfig = (IcebergCatalogConfig) perTableConfig.getIcebergCatalogConfig();
    _init(
        configuration,
        schemaExtractor,
        schemaSync,
        partitionSpecExtractor,
        partitionSpecSync,
        dataFileUpdatesExtractor,
        tableManager);
  }

  private void _init(
      Configuration configuration,
      IcebergSchemaExtractor schemaExtractor,
      IcebergSchemaSync schemaSync,
      IcebergPartitionSpecExtractor partitionSpecExtractor,
      IcebergPartitionSpecSync partitionSpecSync,
      IcebergDataFileUpdatesSync dataFileUpdatesExtractor,
      IcebergTableManager tableManager) {
    this.schemaExtractor = schemaExtractor;
    this.schemaSync = schemaSync;
    this.partitionSpecExtractor = partitionSpecExtractor;
    this.partitionSpecSync = partitionSpecSync;
    this.dataFileUpdatesExtractor = dataFileUpdatesExtractor;
    this.configuration = configuration;
    this.tableManager = tableManager;

    if (tableManager.tableExists(catalogConfig, tableIdentifier, basePath)) {
      // Load the table state if it already exists
      this.table = tableManager.getTable(catalogConfig, tableIdentifier, basePath);
    }
    // Clear any corrupted state before using the target client
    rollbackCorruptCommits();
  }

  @Override
  public void init(Configuration configuration) {
    _init(
        configuration,
        IcebergSchemaExtractor.getInstance(),
        IcebergSchemaSync.getInstance(),
        IcebergPartitionSpecExtractor.getInstance(),
        IcebergPartitionSpecSync.getInstance(),
        IcebergDataFileUpdatesSync.of(
            IcebergColumnStatsConverter.getInstance(),
            IcebergPartitionValueConverter.getInstance()),
        IcebergTableManager.of(configuration));
  }

  /**
   * For injection purposes from TableFormatClientFactory. To be set prior to calling the init
   * method.
   *
   * @param tableName
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * For injection purposes from TableFormatClientFactory. To be set prior to calling the init
   * method.
   *
   * @param namespace
   */
  public void setNamespace(String[] namespace) {
    this.namespace = namespace;
  }

  /**
   * For injection purposes from TableFormatClientFactory. To be set prior to calling the init
   * method.
   *
   * @param basePath
   */
  public void setTableBasePath(String basePath) {
    this.basePath = basePath;
  }

  /**
   * For injection purposes from TableFormatClientFactory. To be set prior to calling the init
   * method.
   *
   * @param catalogConfig
   */
  public void setIcebergCatalogConfig(IcebergCatalogConfig catalogConfig) {
    this.catalogConfig = catalogConfig;
  }

  /**
   * For injection purposes from TableFormatClientFactory. To be set prior to calling the init
   * method.
   *
   * @param snapshotRetentionInHours
   */
  public void setTargetMetadataRetentionInHours(int snapshotRetentionInHours) {
    this.snapshotRetentionInHours = snapshotRetentionInHours;
  }

  @Override
  public void beginSync(OneTable oneTable) {
    initializeTableIfRequired(oneTable);
    transaction = table.newTransaction();
    internalTableState = oneTable;
  }

  private void initializeTableIfRequired(OneTable oneTable) {
    if (table == null) {
      table =
          tableManager.getOrCreateTable(
              catalogConfig,
              tableIdentifier,
              basePath,
              schemaExtractor.toIceberg(oneTable.getReadSchema()),
              partitionSpecExtractor.toIceberg(
                  oneTable.getPartitioningFields(),
                  schemaExtractor.toIceberg(oneTable.getReadSchema())));
    }
  }

  @Override
  public void syncSchema(OneSchema schema) {
    Schema latestSchema = schemaExtractor.toIceberg(schema);
    schemaSync.sync(transaction.table().schema(), latestSchema, transaction);
  }

  @Override
  public void syncPartitionSpec(List<OnePartitionField> partitionSpec) {
    PartitionSpec latestPartitionSpec =
        partitionSpecExtractor.toIceberg(partitionSpec, transaction.table().schema());
    partitionSpecSync.sync(table.spec(), latestPartitionSpec, transaction);
  }

  @Override
  public void syncMetadata(OneTableMetadata metadata) {
    UpdateProperties updateProperties = transaction.updateProperties();
    for (Map.Entry<String, String> stateProperty : metadata.asMap().entrySet()) {
      updateProperties.set(stateProperty.getKey(), stateProperty.getValue());
    }
    if (!table.properties().containsKey(TableProperties.WRITE_DATA_LOCATION)) {
      // Required for a consistent write location when writing back to the table as Iceberg
      updateProperties.set(TableProperties.WRITE_DATA_LOCATION, basePath);
    }
    if (!Boolean.parseBoolean(
        table
            .properties()
            .getOrDefault(
                TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, Boolean.FALSE.toString()))) {
      // Helps control the number of metadata files for frequently updated tables
      updateProperties.set(
          TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, Boolean.TRUE.toString());
    }
    updateProperties.commit();
  }

  @Override
  public void syncFilesForSnapshot(List<OneFileGroup> partitionedDataFiles) {
    dataFileUpdatesExtractor.applySnapshot(
        table,
        internalTableState,
        transaction,
        partitionedDataFiles,
        transaction.table().schema(),
        transaction.table().spec());
  }

  @Override
  public void syncFilesForDiff(OneDataFilesDiff oneDataFilesDiff) {
    dataFileUpdatesExtractor.applyDiff(
        transaction, oneDataFilesDiff, transaction.table().schema(), transaction.table().spec());
  }

  @Override
  public void completeSync() {
    transaction
        .expireSnapshots()
        .expireOlderThan(
            Instant.now().minus(snapshotRetentionInHours, ChronoUnit.HOURS).toEpochMilli())
        .deleteWith(this::safeDelete) // ensures that only metadata files are deleted
        .cleanExpiredFiles(true)
        .commit();
    transaction.commitTransaction();
    transaction = null;
    internalTableState = null;
  }

  private void safeDelete(String file) {
    if (file.startsWith(new Path(basePath) + METADATA_DIR_PATH)) {
      table.io().deleteFile(file);
    }
  }

  @Override
  public Optional<OneTableMetadata> getTableMetadata() {
    if (table == null) {
      return Optional.empty();
    }
    return OneTableMetadata.fromMap(table.properties());
  }

  @Override
  public String getTableFormat() {
    return TableFormat.ICEBERG;
  }

  private void rollbackCorruptCommits() {
    if (table == null) {
      // there is no existing table so exit early
      return;
    }
    // There are cases when using the HadoopTables API where the table metadata json is updated but
    // the manifest file is not written, causing corruption. This is a workaround to fix the issue.
    if (catalogConfig == null && table.currentSnapshot() != null) {
      boolean validSnapshot = false;
      while (!validSnapshot) {
        try {
          table.currentSnapshot().allManifests(table.io());
          validSnapshot = true;
        } catch (NotFoundException ex) {
          Snapshot currentSnapshot = table.currentSnapshot();
          log.warn(
              "Corrupt snapshot detected for table: {} snapshotId: {}. Rolling back to previous snapshot: {}.",
              tableIdentifier,
              currentSnapshot.snapshotId(),
              currentSnapshot.parentId());
          // if we need to rollback, we must also clear the last sync state since that sync is no
          // longer considered valid. This will force OneTable to fall back to a snapshot sync in
          // the subsequent sync round.
          table.manageSnapshots().rollbackTo(currentSnapshot.parentId()).commit();
          Transaction transaction = table.newTransaction();
          transaction
              .updateProperties()
              .remove(ONETABLE_LAST_INSTANT_SYNCED_PROP)
              .remove(INFLIGHT_COMMITS_TO_CONSIDER_FOR_NEXT_SYNC_PROP)
              .commit();
          transaction.commitTransaction();
        }
      }
    }
  }
}
