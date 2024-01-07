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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import org.apache.spark.sql.delta.DeltaConfigs;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.DeltaOperations;
import org.apache.spark.sql.delta.OptimisticTransaction;
import org.apache.spark.sql.delta.actions.Action;
import org.apache.spark.sql.delta.actions.AddFile;
import org.apache.spark.sql.delta.actions.Format;
import org.apache.spark.sql.delta.actions.Metadata;
import org.apache.spark.sql.delta.actions.RemoveFile;

import scala.Option;
import scala.Some;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import com.google.common.annotations.VisibleForTesting;

import io.onetable.client.PerTableConfig;
import io.onetable.exception.NotSupportedException;
import io.onetable.model.OneTable;
import io.onetable.model.OneTableMetadata;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.storage.OneDataFilesDiff;
import io.onetable.model.storage.OneFileGroup;
import io.onetable.model.storage.TableFormat;
import io.onetable.spi.sync.TargetClient;

public class DeltaClient implements TargetClient {
  private static final String MIN_READER_VERSION = String.valueOf(1);
  // gets access to generated columns.
  private static final String MIN_WRITER_VERSION = String.valueOf(4);

  private DeltaLog deltaLog;
  private DeltaSchemaExtractor schemaExtractor;
  private DeltaPartitionExtractor partitionExtractor;
  private DeltaDataFileUpdatesExtractor dataFileUpdatesExtractor;
  private String tableName;
  private String tableDataPath;
  private int logRetentionInHours;
  private TransactionState transactionState;

  public DeltaClient() {}

  public DeltaClient(PerTableConfig perTableConfig, SparkSession sparkSession) {
    this(
        perTableConfig.getTableDataPath(),
        perTableConfig.getTableName(),
        perTableConfig.getTargetMetadataRetentionInHours(),
        sparkSession,
        DeltaSchemaExtractor.getInstance(),
        DeltaPartitionExtractor.getInstance(),
        DeltaDataFileUpdatesExtractor.builder().build());
  }

  @VisibleForTesting
  DeltaClient(
      String tableDataPath,
      String tableName,
      int logRetentionInHours,
      SparkSession sparkSession,
      DeltaSchemaExtractor schemaExtractor,
      DeltaPartitionExtractor partitionExtractor,
      DeltaDataFileUpdatesExtractor dataFileUpdatesExtractor) {
    this.tableDataPath = tableDataPath;
    this.tableName = tableName;
    this.logRetentionInHours = logRetentionInHours;
    _init(sparkSession, schemaExtractor, partitionExtractor, dataFileUpdatesExtractor);
  }

  private void _init(
      SparkSession sparkSession,
      DeltaSchemaExtractor schemaExtractor,
      DeltaPartitionExtractor partitionExtractor,
      DeltaDataFileUpdatesExtractor dataFileUpdatesExtractor) {
    DeltaLog deltaLog = DeltaLog.forTable(sparkSession, tableDataPath);
    boolean deltaTableExists = deltaLog.tableExists();
    if (!deltaTableExists) {
      deltaLog.ensureLogDirectoryExist();
    }
    this.schemaExtractor = schemaExtractor;
    this.partitionExtractor = partitionExtractor;
    this.dataFileUpdatesExtractor = dataFileUpdatesExtractor;
    this.deltaLog = deltaLog;
    this.tableName = tableName;
  }

  @Override
  public void init(Configuration configuration) {
    SparkSession sparkSession = DeltaClientUtils.buildSparkSession(configuration);

    _init(
        sparkSession,
        DeltaSchemaExtractor.getInstance(),
        DeltaPartitionExtractor.getInstance(),
        DeltaDataFileUpdatesExtractor.builder().build());
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
   * @param tableDataPath
   */
  public void setTableDataPath(String tableDataPath) {
    this.tableDataPath = tableDataPath;
  }

  /**
   * For injection purposes from TableFormatClientFactory. To be set prior to calling the init
   * method.
   *
   * @param logRetentionInHours
   */
  public void setTargetMetadataRetentionInHours(int logRetentionInHours) {
    this.logRetentionInHours = logRetentionInHours;
  }

  @Override
  public void beginSync(OneTable table) {
    this.transactionState =
        new TransactionState(deltaLog, tableName, table.getLatestCommitTime(), logRetentionInHours);
  }

  @Override
  public void syncSchema(OneSchema schema) {
    transactionState.setLatestSchema(schema);
  }

  @Override
  public void syncPartitionSpec(List<OnePartitionField> partitionSpec) {
    Map<String, StructField> spec = partitionExtractor.convertToDeltaPartitionFormat(partitionSpec);
    if (partitionSpec != null) {
      for (Map.Entry<String, StructField> e : spec.entrySet()) {
        transactionState.getPartitionColumns().add(e.getKey());
        if (e.getValue() != null
            && transactionState.getLatestSchema().getFieldIndex(e.getValue().name()).isEmpty()) {
          // add generated columns to schema.
          transactionState.addColumn(e.getValue());
        }
      }
    }
  }

  @Override
  public void syncMetadata(OneTableMetadata metadata) {
    transactionState.setMetadata(metadata);
  }

  @Override
  public void syncFilesForSnapshot(List<OneFileGroup> partitionedDataFiles) {
    transactionState.setActions(
        dataFileUpdatesExtractor.applySnapshot(
            deltaLog, partitionedDataFiles, transactionState.getLatestSchemaInternal()));
  }

  @Override
  public void syncFilesForDiff(OneDataFilesDiff oneDataFilesDiff) {
    transactionState.setActions(
        dataFileUpdatesExtractor.applyDiff(
            oneDataFilesDiff,
            transactionState.getLatestSchemaInternal(),
            deltaLog.dataPath().toString()));
  }

  @Override
  public void completeSync() {
    transactionState.commitTransaction();
    transactionState = null;
  }

  @Override
  public Optional<OneTableMetadata> getTableMetadata() {
    return OneTableMetadata.fromMap(
        JavaConverters.mapAsJavaMapConverter(deltaLog.metadata().configuration()).asJava());
  }

  @Override
  public String getTableFormat() {
    return TableFormat.DELTA;
  }

  @EqualsAndHashCode
  @ToString
  private class TransactionState {
    private final OptimisticTransaction transaction;
    private final Instant commitTime;
    private final DeltaLog deltaLog;
    private final int retentionInHours;
    @Getter private final List<String> partitionColumns;
    private final String tableName;
    @Getter private StructType latestSchema;
    @Getter private OneSchema latestSchemaInternal;
    @Setter private OneTableMetadata metadata;
    @Setter private Seq<Action> actions;

    private TransactionState(
        DeltaLog deltaLog, String tableName, Instant latestCommitTime, int retentionInHours) {
      this.deltaLog = deltaLog;
      this.transaction = deltaLog.startTransaction();
      this.latestSchema = deltaLog.snapshot().schema();
      this.commitTime = latestCommitTime;
      this.partitionColumns = new ArrayList<>();
      this.tableName = tableName;
      this.retentionInHours = retentionInHours;
    }

    private void addColumn(StructField field) {
      latestSchema = latestSchema.add(field);
      latestSchemaInternal = schemaExtractor.toOneSchema(latestSchema);
    }

    private void setLatestSchema(OneSchema schema) {
      this.latestSchemaInternal = schema;
      this.latestSchema = schemaExtractor.fromOneSchema(schema);
    }

    private void commitTransaction() {
      Metadata metadata =
          new Metadata(
              deltaLog.tableId(),
              tableName,
              "",
              getFileFormat(),
              latestSchema.json(),
              JavaConverters.asScalaBuffer(partitionColumns).toList(),
              ScalaUtils.convertJavaMapToScala(getConfigurationsForDeltaSync()),
              new Some<>(commitTime.toEpochMilli()));
      transaction.updateMetadata(metadata);
      transaction.commit(actions, new DeltaOperations.Update(Option.apply("onetable-delta-sync")));
    }

    private Map<String, String> getConfigurationsForDeltaSync() {
      Map<String, String> configMap = new HashMap<>(metadata.asMap());
      configMap.put(DeltaConfigs.MIN_READER_VERSION().key(), MIN_READER_VERSION);
      configMap.put(DeltaConfigs.MIN_WRITER_VERSION().key(), MIN_WRITER_VERSION);
      // Sets retention for the Delta Log, does not impact underlying files in the table
      configMap.put(
          DeltaConfigs.LOG_RETENTION().key(), String.format("interval %d hours", retentionInHours));
      return configMap;
    }

    private Format getFileFormat() {
      if (actions.iterator().hasNext()) {
        // set file format based on action
        Action action = actions.iterator().next();
        String path = null;
        if (action instanceof AddFile) {
          path = ((AddFile) action).path();
        } else if (action instanceof RemoveFile) {
          path = ((RemoveFile) action).path();
        }
        if (path != null) {
          if (path.contains(".parquet")) {
            return new Format("parquet", ScalaUtils.convertJavaMapToScala(Collections.emptyMap()));
          } else if (path.contains(".orc")) {
            return new Format("orc", ScalaUtils.convertJavaMapToScala(Collections.emptyMap()));
          }
          throw new NotSupportedException("Fileformat is not supported for delta sync");
        }
      }
      // fallback to existing deltalog value
      return deltaLog.metadata().format();
    }
  }
}
