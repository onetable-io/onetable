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
 
package io.onetable.model;

import lombok.Builder;
import lombok.Value;

import io.onetable.model.storage.OneDataFilesDiff;

/**
 * Captures a single commit/change done to the table at {@link #currentTableState#instant}.
 *
 * @since 0.1
 */
@Value
@Builder(toBuilder = true)
public class TableChange {
  // Change in files since the last commit in the source format
  OneDataFilesDiff filesDiff;
  // OneTable state at the specified instant
  OneTable currentTableState;
}
