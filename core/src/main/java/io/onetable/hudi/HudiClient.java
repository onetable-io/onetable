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
 
package io.onetable.hudi;

import static io.onetable.collectors.CustomCollectors.toList;
import static io.onetable.hudi.HudiInstantUtils.convertInstantToCommit;
import static io.onetable.hudi.HudiInstantUtils.parseFromInstantTime;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.Option;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import io.onetable.exception.OneIOException;
import io.onetable.model.*;
import io.onetable.model.CommitsBacklog;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.SchemaCatalog;
import io.onetable.spi.extractor.SourceClient;

public class HudiClient implements SourceClient<HoodieInstant> {

  private final HoodieTableMetaClient metaClient;
  private final HudiTableExtractor tableExtractor;
  private final HudiDataFileExtractor dataFileExtractor;

  public HudiClient(
      HoodieTableMetaClient metaClient,
      HudiSourcePartitionSpecExtractor sourcePartitionSpecExtractor) {
    this.metaClient = metaClient;
    this.tableExtractor =
        new HudiTableExtractor(new HudiSchemaExtractor(), sourcePartitionSpecExtractor);
    this.dataFileExtractor =
        new HudiDataFileExtractor(
            metaClient,
            new HudiPartitionValuesExtractor(
                sourcePartitionSpecExtractor.getPathToPartitionFieldFormat()),
            new HudiFileStatsExtractor(metaClient));
  }

  @Override
  public OneTable getTable(HoodieInstant commit) {
    return tableExtractor.table(metaClient, commit);
  }

  @Override
  public SchemaCatalog getSchemaCatalog(OneTable table, HoodieInstant commit) {
    return HudiSchemaCatalogExtractor.catalogWithTableSchema(table);
  }

  @Override
  public OneSchema getSchema(OneTable table) {
    return table.getReadSchema();
  }

  @Override
  public OneSnapshot getCurrentSnapshot() {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieTimeline completedTimeline = activeTimeline.filterCompletedInstants();
    // get latest commit
    HoodieInstant latestCommit =
        completedTimeline
            .lastInstant()
            .orElseThrow(
                () -> new OneIOException("Unable to read latest commit from Hudi source table"));
    List<HoodieInstant> pendingInstants =
        activeTimeline
            .filterInflightsAndRequested()
            .findInstantsBefore(latestCommit.getTimestamp())
            .getInstants();
    OneTable table = getTable(latestCommit);
    return OneSnapshot.builder()
        .table(table)
        .schemaCatalog(getSchemaCatalog(table, latestCommit))
        .partitionedDataFiles(dataFileExtractor.getFilesCurrentState(table))
        .pendingCommits(
            pendingInstants.stream()
                .map(hoodieInstant -> parseFromInstantTime(hoodieInstant.getTimestamp()))
                .collect(toList(pendingInstants.size())))
        .build();
  }

  @Override
  public TableChange getTableChangeForCommit(HoodieInstant hoodieInstantForDiff) {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieTimeline visibleTimeline =
        activeTimeline
            .filterCompletedInstants()
            .findInstantsBeforeOrEquals(hoodieInstantForDiff.getTimestamp());
    OneTable table = getTable(hoodieInstantForDiff);
    return TableChange.builder()
        .tableAsOfChange(table)
        .filesDiff(
            dataFileExtractor.getDiffForCommit(
                hoodieInstantForDiff, table, hoodieInstantForDiff, visibleTimeline))
        .build();
  }

  @Override
  public CommitsBacklog<HoodieInstant> getCommitsBacklog(
      InstantsForIncrementalSync instantsForIncrementalSync) {
    Instant lastSyncInstant = instantsForIncrementalSync.getLastSyncInstant();
    List<Instant> lastPendingInstants = instantsForIncrementalSync.getPendingCommits();
    HoodieInstant lastInstantSynced = getCommitAtInstant(lastSyncInstant);
    CommitsPair commitsPair = getCompletedAndPendingCommitsAfterInstant(lastInstantSynced);
    CommitsPair lastPendingHoodieInstantsCommitsPair =
        getCompletedAndPendingCommitsForInstants(lastPendingInstants);
    List<HoodieInstant> commitsToProcessNext =
        mergeAndDedupLists(
            lastPendingHoodieInstantsCommitsPair.getCompletedCommits(),
            commitsPair.getCompletedCommits());
    List<Instant> pendingInstantsToProcessNext =
        mergeAndDedupLists(
            lastPendingHoodieInstantsCommitsPair.getPendingCommits(),
            commitsPair.getPendingCommits());
    return CommitsBacklog.<HoodieInstant>builder()
        .commitsToProcess(commitsToProcessNext)
        .inFlightInstants(pendingInstantsToProcessNext)
        .build();
  }

  @Override
  public boolean isIncrementalSyncSafeFrom(Instant instant) {
    return doesCommitExistsAsOfInstant(instant) && !isAffectedByCleanupProcess(instant);
  }

  private boolean doesCommitExistsAsOfInstant(Instant instant) {
    HoodieInstant hoodieInstant = getCommitAtInstant(instant);
    return hoodieInstant != null;
  }

  @SneakyThrows
  private boolean isAffectedByCleanupProcess(Instant instant) {
    Option<HoodieInstant> lastCleanInstant =
        metaClient.getActiveTimeline().getCleanerTimeline().filterCompletedInstants().lastInstant();
    if (!lastCleanInstant.isPresent()) {
      return false;
    }
    HoodieCleanMetadata cleanMetadata =
        TimelineMetadataUtils.deserializeHoodieCleanMetadata(
            metaClient.getActiveTimeline().getInstantDetails(lastCleanInstant.get()).get());
    String earliestCommitToRetain = cleanMetadata.getEarliestCommitToRetain();
    Instant earliestCommitToRetainInstant =
        HudiInstantUtils.parseFromInstantTime(earliestCommitToRetain);
    return earliestCommitToRetainInstant.isAfter(instant);
  }

  private CommitsPair getCompletedAndPendingCommitsForInstants(List<Instant> lastPendingInstants) {
    List<HoodieInstant> lastPendingHoodieInstants = getCommitsForInstants(lastPendingInstants);
    List<HoodieInstant> lastPendingHoodieInstantsCompleted =
        lastPendingHoodieInstants.stream()
            .filter(HoodieInstant::isCompleted)
            .collect(Collectors.toList());
    List<Instant> lastPendingHoodieInstantsStillPending =
        lastPendingHoodieInstants.stream()
            .filter(hoodieInstant -> hoodieInstant.isInflight() || hoodieInstant.isRequested())
            .map(hoodieInstant -> parseFromInstantTime(hoodieInstant.getTimestamp()))
            .collect(Collectors.toList());
    return CommitsPair.builder()
        .completedCommits(lastPendingHoodieInstantsCompleted)
        .pendingCommits(lastPendingHoodieInstantsStillPending)
        .build();
  }

  private HoodieTimeline getCompletedCommits() {
    return metaClient.getActiveTimeline().filterCompletedInstants();
  }

  private CommitsPair getCompletedAndPendingCommitsAfterInstant(HoodieInstant commitInstant) {
    List<HoodieInstant> allInstants =
        metaClient
            .getActiveTimeline()
            .findInstantsAfter(commitInstant.getTimestamp())
            .getInstants();
    // collect the completed instants & inflight instants from all the instants.
    List<HoodieInstant> completedInstants =
        allInstants.stream().filter(HoodieInstant::isCompleted).collect(Collectors.toList());
    // Nothing to sync as there are only pending commits.
    if (completedInstants.isEmpty()) {
      return CommitsPair.builder().completedCommits(completedInstants).build();
    }
    // remove from pending instants that are larger than the last completed instant.
    List<Instant> pendingInstants =
        allInstants.stream()
            .filter(hoodieInstant -> hoodieInstant.isInflight() || hoodieInstant.isRequested())
            .filter(
                hoodieInstant ->
                    hoodieInstant.compareTo(completedInstants.get(completedInstants.size() - 1))
                        <= 0)
            .map(hoodieInstant -> parseFromInstantTime(hoodieInstant.getTimestamp()))
            .collect(Collectors.toList());
    return CommitsPair.builder()
        .completedCommits(completedInstants)
        .pendingCommits(pendingInstants)
        .build();
  }

  private HoodieInstant getCommitAtInstant(Instant instant) {
    return getCompletedCommits()
        .findInstantsBeforeOrEquals(convertInstantToCommit(instant))
        .lastInstant()
        .orElse(null);
  }

  private List<HoodieInstant> getCommitsForInstants(List<Instant> instants) {
    if (instants == null || instants.isEmpty()) {
      return Collections.emptyList();
    }
    // Savepoint commits are not processed and commit time can overlap with other actions, hence
    // filtering.
    Map<Instant, HoodieInstant> instantHoodieInstantMap =
        metaClient.getActiveTimeline().getInstants().stream()
            .filter(instant -> !HoodieTimeline.SAVEPOINT_ACTION.equals(instant.getAction()))
            .collect(
                Collectors.toMap(
                    hoodieInstant -> parseFromInstantTime(hoodieInstant.getTimestamp()),
                    hoodieInstant -> hoodieInstant));
    return instants.stream()
        .map(instantHoodieInstantMap::get)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  /**
   * Merges commits from two lists and returns new list of sorted commits by eliminating duplicates.
   *
   * @param list1 First sorted input list of commits
   * @param list2 Second sorted input list of commits.
   * @return merged list of commits in sorted order.
   */
  private <T extends Comparable<T>> List<T> mergeAndDedupLists(
      @NonNull List<T> list1, @NonNull List<T> list2) {
    List<T> mergedList = new ArrayList<>();
    PeekingIterator<T> itr1 = Iterators.peekingIterator(list1.iterator());
    PeekingIterator<T> itr2 = Iterators.peekingIterator(list2.iterator());
    while (itr1.hasNext() || itr2.hasNext()) {
      if (!itr2.hasNext()) {
        mergedList.add(itr1.next());
      } else if (!itr1.hasNext()) {
        mergedList.add(itr2.next());
      } else {
        T element1 = itr1.peek();
        T element2 = itr2.peek();
        if (element1.compareTo(element2) < 0) {
          mergedList.add(element1);
          itr1.next();
        } else if (element1.compareTo(element2) > 0) {
          mergedList.add(element2);
          itr2.next();
        } else {
          mergedList.add(element1);
          itr1.next();
          itr2.next();
        }
      }
    }
    return mergedList;
  }

  @Override
  public void close() {
    dataFileExtractor.close();
  }

  @Value
  @Builder
  private static class CommitsPair {
    @Builder.Default List<HoodieInstant> completedCommits = Collections.emptyList();
    @Builder.Default List<Instant> pendingCommits = Collections.emptyList();
  }
}
