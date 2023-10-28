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
 
package io.onetable.model.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.AllArgsConstructor;
import lombok.Value;

import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.stat.Range;

/** Represents a grouping of {@link OneDataFile} with the same partition values. */
@Value
@AllArgsConstructor(staticName = "of")
public class PartitionedDataFiles {
  List<List<OneDataFile>> fileGroupings;

  public List<OneDataFile> getAllFiles() {
    return fileGroupings.stream().flatMap(List::stream).collect(Collectors.toList());
  }

  public static PartitionedDataFiles fromFiles(List<OneDataFile> files) {
    return fromFiles(files.stream());
  }

  public static PartitionedDataFiles fromFiles(Stream<OneDataFile> files) {
    Map<Map<OnePartitionField, Range>, List<OneDataFile>> filesGrouped =
        files.collect(Collectors.groupingBy(OneDataFile::getPartitionValues));
    return PartitionedDataFiles.of(new ArrayList<>(filesGrouped.values()));
  }
}
