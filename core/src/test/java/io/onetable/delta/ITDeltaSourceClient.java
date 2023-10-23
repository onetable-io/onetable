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

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.text.ParseException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.onetable.TestSparkDeltaTable;
import io.onetable.client.PerTableConfig;
import io.onetable.model.CurrentCommitState;
import io.onetable.model.InstantsForIncrementalSync;
import io.onetable.model.OneSnapshot;
import io.onetable.model.OneTable;
import io.onetable.model.TableChange;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;
import io.onetable.model.schema.PartitionTransformType;
import io.onetable.model.schema.SchemaCatalog;
import io.onetable.model.schema.SchemaVersion;
import io.onetable.model.storage.DataLayoutStrategy;
import io.onetable.model.storage.OneDataFile;
import io.onetable.model.storage.OneDataFiles;
import io.onetable.model.storage.TableFormat;
import io.onetable.spi.DefaultSnapshotVisitor;

/*
 * All Tests planning to support.
 * 1. Inserts, Updates, Deletes combination.
 * 2. Add  partition.
 * 3. Change schema as per allowed evolution (adding columns etc...)
 * 4. Drop partition.
 * 5. Vaccum.
 * 6. Clustering.
 */
public class ITDeltaSourceClient {
  @TempDir private static Path tempDir;
  private static SparkSession sparkSession;

  private DeltaSourceClientProvider clientProvider;

  @BeforeAll
  public static void setupOnce() {
    sparkSession =
        SparkSession.builder()
            .appName("TestDeltaTable")
            .master("local[4]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
            .getOrCreate();
  }

  @AfterAll
  public static void teardown() {
    if (sparkSession != null) {
      sparkSession.close();
    }
  }

  @BeforeEach
  void setUp() {
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("fs.defaultFS", "file:///");

    clientProvider = new DeltaSourceClientProvider();
    clientProvider.init(hadoopConf, null);
  }

  @Test
  void getCurrentSnapshotNonPartitionedTest() {
    // Table name
    final String tableName = "test_" + Instant.now().toEpochMilli();
    final Path basePath = tempDir.resolve(tableName);
    // Create table with a single row using Spark
    sparkSession.sql(
        "CREATE TABLE `"
            + tableName
            + "` USING DELTA LOCATION '"
            + basePath
            + "' AS SELECT * FROM VALUES (1, 2)");
    // Create Delta source client
    PerTableConfig tableConfig =
        PerTableConfig.builder()
            .tableName(tableName)
            .tableBasePath(basePath.toString())
            .targetTableFormats(Collections.singletonList(TableFormat.ICEBERG))
            .build();
    DeltaSourceClient client = clientProvider.getSourceClientInstance(tableConfig);
    // Get current snapshot
    OneSnapshot snapshot = client.getCurrentSnapshot();
    // Validate table
    validateTable(
        snapshot.getTable(),
        tableName,
        TableFormat.DELTA,
        OneSchema.builder()
            .name("struct")
            .dataType(OneType.RECORD)
            .fields(
                Arrays.asList(
                    OneField.builder()
                        .name("col1")
                        .schema(
                            OneSchema.builder()
                                .name("integer")
                                .dataType(OneType.INT)
                                .isNullable(true)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    OneField.builder()
                        .name("col2")
                        .schema(
                            OneSchema.builder()
                                .name("integer")
                                .dataType(OneType.INT)
                                .isNullable(true)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .build(),
        DataLayoutStrategy.FLAT,
        "file:" + basePath,
        Collections.emptyList());
    // Validate schema catalog
    SchemaCatalog oneSchemaCatalog = snapshot.getSchemaCatalog();
    validateSchemaCatalog(
        oneSchemaCatalog,
        Collections.singletonMap(new SchemaVersion(1, ""), snapshot.getTable().getReadSchema()));
    // TODO: Validate data files (see https://github.com/onetable-io/onetable/issues/96)
  }

  @Test
  void getCurrentSnapshotPartitionedTest() {
    // Table name
    final String tableName = "test_" + Instant.now().toEpochMilli();
    final Path basePath = tempDir.resolve(tableName);
    // Create table with a single row using Spark
    sparkSession.sql(
        "CREATE TABLE `"
            + tableName
            + "` USING DELTA PARTITIONED BY (part_col)\n"
            + "LOCATION '"
            + basePath
            + "' AS SELECT 'SingleValue' AS part_col, 1 AS col1, 2 AS col2");
    // Create Delta source client
    PerTableConfig tableConfig =
        PerTableConfig.builder()
            .tableName(tableName)
            .tableBasePath(basePath.toString())
            .targetTableFormats(Collections.singletonList(TableFormat.ICEBERG))
            .build();
    DeltaSourceClient client = clientProvider.getSourceClientInstance(tableConfig);
    // Get current snapshot
    OneSnapshot snapshot = client.getCurrentSnapshot();
    // Validate table
    OneField partCol =
        OneField.builder()
            .name("part_col")
            .schema(
                OneSchema.builder()
                    .name("string")
                    .dataType(OneType.STRING)
                    .isNullable(true)
                    .build())
            .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
            .build();
    validateTable(
        snapshot.getTable(),
        tableName,
        TableFormat.DELTA,
        OneSchema.builder()
            .name("struct")
            .dataType(OneType.RECORD)
            .fields(
                Arrays.asList(
                    partCol,
                    OneField.builder()
                        .name("col1")
                        .schema(
                            OneSchema.builder()
                                .name("integer")
                                .dataType(OneType.INT)
                                .isNullable(true)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    OneField.builder()
                        .name("col2")
                        .schema(
                            OneSchema.builder()
                                .name("integer")
                                .dataType(OneType.INT)
                                .isNullable(true)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .build(),
        DataLayoutStrategy.DIR_HIERARCHY_PARTITION_VALUES,
        "file:" + basePath,
        Collections.singletonList(
            OnePartitionField.builder()
                .sourceField(partCol)
                .transformType(PartitionTransformType.VALUE)
                .build()));
    // Validate schema catalog
    SchemaCatalog oneSchemaCatalog = snapshot.getSchemaCatalog();
    validateSchemaCatalog(
        oneSchemaCatalog,
        Collections.singletonMap(new SchemaVersion(1, ""), snapshot.getTable().getReadSchema()));
    // TODO: Validate data files (see https://github.com/onetable-io/onetable/issues/96)
  }

  @Disabled("Requires Spark 3.4.0+")
  @Test
  void getCurrentSnapshotGenColPartitionedTest() {
    // Table name
    final String tableName = "test_" + Instant.now().toEpochMilli();
    final Path basePath = tempDir.resolve(tableName);
    // Create table with a single row using Spark
    sparkSession.sql(
        "CREATE TABLE `"
            + tableName
            + "` (id BIGINT, event_time TIMESTAMP, day INT GENERATED ALWAYS AS (DATE_FORMAT(event_time, 'YYYY-MM-dd')))"
            + " USING DELTA LOCATION '"
            + basePath
            + "'");
    sparkSession.sql(
        "INSERT INTO TABLE `"
            + tableName
            + "` VALUES(1, CAST('2012-02-12 00:12:34' AS TIMESTAMP))");
    // Create Delta source client
    PerTableConfig tableConfig =
        PerTableConfig.builder()
            .tableName(tableName)
            .tableBasePath(basePath.toString())
            .targetTableFormats(Collections.singletonList(TableFormat.ICEBERG))
            .build();
    DeltaSourceClient client = clientProvider.getSourceClientInstance(tableConfig);
    // Get current snapshot
    OneSnapshot snapshot = client.getCurrentSnapshot();
    // TODO: Complete and enable test (see https://github.com/onetable-io/onetable/issues/90)
  }

  @Test
  public void insertsUpsertsAndDeletes() throws ParseException {
    TestSparkDeltaTable testSparkDeltaTable =
        new TestSparkDeltaTable("some_table", tempDir, sparkSession);
    List<List<String>> allActiveFiles = new ArrayList<>();
    List<TableChange> allTableChanges = new ArrayList<>();
    List<Row> rows = testSparkDeltaTable.insertRows(50);
    Long timestamp1 = testSparkDeltaTable.getLastCommitTimestamp();
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    List<Row> rows1 = testSparkDeltaTable.insertRows(50);
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    testSparkDeltaTable.upsertRows(rows.subList(0, 20));
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    testSparkDeltaTable.insertRows(50);
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    testSparkDeltaTable.deleteRows(rows1.subList(0, 20));
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    testSparkDeltaTable.insertRows(50);
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());
    PerTableConfig tableConfig =
        PerTableConfig.builder()
            .tableName(testSparkDeltaTable.getTableName())
            .tableBasePath(testSparkDeltaTable.getBasePath())
            .targetTableFormats(Arrays.asList(TableFormat.HUDI, TableFormat.ICEBERG))
            .build();
    DeltaSourceClient deltaSourceClient = clientProvider.getSourceClientInstance(tableConfig);
    assertEquals(180L, testSparkDeltaTable.getNumRows());
    OneSnapshot oneSnapshot = deltaSourceClient.getCurrentSnapshot();
    validateOneSnapshot(oneSnapshot, allActiveFiles.get(allActiveFiles.size() - 1));
    // Get changes in incremental format.
    InstantsForIncrementalSync instantsForIncrementalSync =
        InstantsForIncrementalSync.builder()
            .lastSyncInstant(Instant.ofEpochMilli(timestamp1))
            .build();
    CurrentCommitState<Long> currentCommitState =
        deltaSourceClient.getCurrentCommitState(instantsForIncrementalSync);
    for (Long version : currentCommitState.getCommitsToProcess()) {
      TableChange tableChange = deltaSourceClient.getTableChangeForCommit(version);
      allTableChanges.add(tableChange);
    }
    validateTableChanges(allActiveFiles, allTableChanges);
  }

  @Test
  public void testVacuum() throws ParseException {
    TestSparkDeltaTable testSparkDeltaTable =
        new TestSparkDeltaTable("some_table", tempDir, sparkSession);
    List<List<String>> allActiveFiles = new ArrayList<>();
    List<TableChange> allTableChanges = new ArrayList<>();
    List<Row> rows = testSparkDeltaTable.insertRows(50);
    Long timestamp1 = testSparkDeltaTable.getLastCommitTimestamp();
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    testSparkDeltaTable.insertRows(50);
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    testSparkDeltaTable.deleteRows(rows.subList(0, 20));
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    testSparkDeltaTable.runVacuum();
    // vacuum has two commits, one for start and one for end, hence adding twice.
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    testSparkDeltaTable.insertRows(50);
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    PerTableConfig tableConfig =
        PerTableConfig.builder()
            .tableName(testSparkDeltaTable.getTableName())
            .tableBasePath(testSparkDeltaTable.getBasePath())
            .targetTableFormats(Arrays.asList(TableFormat.HUDI, TableFormat.ICEBERG))
            .build();
    DeltaSourceClient deltaSourceClient = clientProvider.getSourceClientInstance(tableConfig);
    assertEquals(130L, testSparkDeltaTable.getNumRows());
    OneSnapshot oneSnapshot = deltaSourceClient.getCurrentSnapshot();
    validateOneSnapshot(oneSnapshot, allActiveFiles.get(allActiveFiles.size() - 1));
    // Get changes in incremental format.
    InstantsForIncrementalSync instantsForIncrementalSync =
        InstantsForIncrementalSync.builder()
            .lastSyncInstant(Instant.ofEpochMilli(timestamp1))
            .build();
    CurrentCommitState<Long> currentCommitState =
        deltaSourceClient.getCurrentCommitState(instantsForIncrementalSync);
    for (Long version : currentCommitState.getCommitsToProcess()) {
      TableChange tableChange = deltaSourceClient.getTableChangeForCommit(version);
      allTableChanges.add(tableChange);
    }
    validateTableChanges(allActiveFiles, allTableChanges);
  }

  @Test
  public void addColumns() throws ParseException {
    TestSparkDeltaTable testSparkDeltaTable =
        new TestSparkDeltaTable("some_table", tempDir, sparkSession);
    List<List<String>> allActiveFiles = new ArrayList<>();
    List<TableChange> allTableChanges = new ArrayList<>();
    List<Row> rows = testSparkDeltaTable.insertRows(50);
    Long timestamp1 = testSparkDeltaTable.getLastCommitTimestamp();
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    testSparkDeltaTable.insertRows(50);
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    testSparkDeltaTable.insertRowsWithAdditionalColumns(50);
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    PerTableConfig tableConfig =
        PerTableConfig.builder()
            .tableName(testSparkDeltaTable.getTableName())
            .tableBasePath(testSparkDeltaTable.getBasePath())
            .targetTableFormats(Arrays.asList(TableFormat.HUDI, TableFormat.ICEBERG))
            .build();
    DeltaSourceClient deltaSourceClient = clientProvider.getSourceClientInstance(tableConfig);
    assertEquals(150L, testSparkDeltaTable.getNumRows());
    OneSnapshot oneSnapshot = deltaSourceClient.getCurrentSnapshot();
    validateOneSnapshot(oneSnapshot, allActiveFiles.get(allActiveFiles.size() - 1));
    // Get changes in incremental format.
    InstantsForIncrementalSync instantsForIncrementalSync =
        InstantsForIncrementalSync.builder()
            .lastSyncInstant(Instant.ofEpochMilli(timestamp1))
            .build();
    CurrentCommitState<Long> currentCommitState =
        deltaSourceClient.getCurrentCommitState(instantsForIncrementalSync);
    for (Long version : currentCommitState.getCommitsToProcess()) {
      TableChange tableChange = deltaSourceClient.getTableChangeForCommit(version);
      allTableChanges.add(tableChange);
    }
    validateTableChanges(allActiveFiles, allTableChanges);
  }

  private void validateOneSnapshot(OneSnapshot oneSnapshot, List<String> allActivePaths) {
    assertNotNull(oneSnapshot);
    assertNotNull(oneSnapshot.getTable());
    List<String> onetablePaths =
        new ArrayList<>(
            DefaultSnapshotVisitor.extractDataFilePaths(oneSnapshot.getDataFiles()).keySet());
    replaceFileScheme(allActivePaths);
    replaceFileScheme(onetablePaths);
    Collections.sort(allActivePaths);
    Collections.sort(onetablePaths);
    assertEquals(allActivePaths, onetablePaths);
  }

  private void validateTableChanges(
      List<List<String>> allActiveFiles, List<TableChange> allTableChanges) {
    if (allTableChanges.isEmpty() && allActiveFiles.size() <= 1) {
      return;
    }
    assertEquals(
        allTableChanges.size(),
        allActiveFiles.size() - 1,
        "Number of table changes should be equal to number of commits - 1");
    IntStream.range(0, allActiveFiles.size() - 1)
        .forEach(
            i ->
                validateTableChange(
                    allActiveFiles.get(i), allActiveFiles.get(i + 1), allTableChanges.get(i)));
  }

  private void validateTableChange(
      List<String> filePathsBefore, List<String> filePathsAfter, TableChange tableChange) {
    assertNotNull(tableChange);
    assertNotNull(tableChange.getCurrentTableState());
    replaceFileScheme(filePathsBefore);
    replaceFileScheme(filePathsAfter);
    Set<String> filesForCommitBefore = new HashSet<>(filePathsBefore);
    Set<String> filesForCommitAfter = new HashSet<>(filePathsAfter);
    // Get files added by diffing filesForCommitAfter and filesForCommitBefore.
    Set<String> filesAdded =
        filesForCommitAfter.stream()
            .filter(file -> !filesForCommitBefore.contains(file))
            .collect(Collectors.toSet());
    // Get files removed by diffing filesForCommitBefore and filesForCommitAfter.
    Set<String> filesRemoved =
        filesForCommitBefore.stream()
            .filter(file -> !filesForCommitAfter.contains(file))
            .collect(Collectors.toSet());
    assertEquals(filesAdded, extractPathsFromDataFile(tableChange.getFilesDiff().getFilesAdded()));
    assertEquals(
        filesRemoved, extractPathsFromDataFile(tableChange.getFilesDiff().getFilesRemoved()));
  }

  private Set<String> extractPathsFromDataFile(Set<OneDataFile> dataFiles) {
    return dataFiles.stream().map(OneDataFile::getPhysicalPath).collect(Collectors.toSet());
  }

  private void replaceFileScheme(List<String> filePaths) {
    // if file paths start with file:///, replace it with file:/.
    filePaths.replaceAll(path -> path.replaceFirst("file:///", "file:/"));
  }

  private static void validateTable(
      OneTable oneTable,
      String tableName,
      TableFormat tableFormat,
      OneSchema readSchema,
      DataLayoutStrategy dataLayoutStrategy,
      String basePath,
      List<OnePartitionField> partitioningFields) {
    Assertions.assertEquals(tableName, oneTable.getName());
    Assertions.assertEquals(tableFormat, oneTable.getTableFormat());
    Assertions.assertEquals(readSchema, oneTable.getReadSchema());
    Assertions.assertEquals(dataLayoutStrategy, oneTable.getLayoutStrategy());
    Assertions.assertEquals(basePath, oneTable.getBasePath());
    Assertions.assertEquals(partitioningFields, oneTable.getPartitioningFields());
  }

  private void validateSchemaCatalog(
      SchemaCatalog oneSchemaCatalog, Map<SchemaVersion, OneSchema> schemas) {
    Assertions.assertEquals(schemas, oneSchemaCatalog.getSchemas());
  }

  private void validateDataFiles(OneDataFiles oneDataFiles, List<OneDataFile> files) {
    Assertions.assertEquals(files, oneDataFiles.getFiles());
  }
}