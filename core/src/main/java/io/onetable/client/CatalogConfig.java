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
 
package io.onetable.client;

import java.util.Map;

/**
 * Catologs are responsible for the management of and providing access to the metadata associated
 * with a given set of Tables, typically within the context of a namespaces. This interface
 * represents the configuration required to initialize a client for a particualr data engine such
 * that it can access the necessary metadata for table conversions and semantic details.
 */
public interface CatalogConfig {
  String getCatalogImpl();

  String getCatalogName();

  Map<String, String> getCatalogOptions();
}
