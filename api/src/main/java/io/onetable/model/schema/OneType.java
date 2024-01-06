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
 
package io.onetable.model.schema;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import lombok.Getter;
import lombok.ToString;

/**
 * Collection of supported types
 *
 * @since 0.1
 */
@Getter
@ToString
public enum OneType {
  RECORD,
  ENUM,
  LIST,
  MAP,
  UNION,
  UUID,
  FIXED,
  STRING,
  BYTES,
  INT,
  LONG,
  FLOAT,
  DOUBLE,
  BOOLEAN,
  NULL,
  DATE,
  DECIMAL,
  TIMESTAMP,
  TIMESTAMP_NTZ;
  private final String name;

  OneType() {
    this.name = this.name().toLowerCase();
  }

  public static final Set<OneType> NON_SCALAR_TYPES =
      Collections.unmodifiableSet(
          new HashSet<OneType>() {
            {
              add(RECORD);
              add(LIST);
              add(MAP);
              add(UNION);
            }
          });
}
