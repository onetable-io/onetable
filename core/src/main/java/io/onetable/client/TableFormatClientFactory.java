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

import java.beans.Expression;
import java.beans.Statement;
import java.lang.reflect.Method;
import java.util.ServiceLoader;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.hadoop.conf.Configuration;

import io.onetable.exception.NotSupportedException;
import io.onetable.spi.sync.TargetClient;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TableFormatClientFactory {
  private static final TableFormatClientFactory INSTANCE = new TableFormatClientFactory();
  public static final String IO_ONETABLE_CLIENT_PER_TABLE_CONFIG =
      "io.onetable.client.PerTableConfig";

  public static TableFormatClientFactory getInstance() {
    return INSTANCE;
  }

  /**
   * Create a fully initialized instance of the TargetClient represented by the given Table Format
   * name. Initialization is done with the config provided through PerTableConfig and Configuration
   * params.
   *
   * @param tableFormat
   * @param perTableConfig
   * @param configuration
   * @return
   */
  public TargetClient createForFormat(
      String tableFormat, PerTableConfig perTableConfig, Configuration configuration) {
    TargetClient targetClient = createTargetClientForName(tableFormat);

    injectPerTableConfigAsNeeded(perTableConfig, targetClient);

    targetClient.init(configuration);
    return targetClient;
  }

  private void injectPerTableConfigAsNeeded(
      PerTableConfig perTableConfig, TargetClient targetClient) {
    Method[] methods = null;
    try {
      Class cls = Class.forName(IO_ONETABLE_CLIENT_PER_TABLE_CONFIG);
      methods = cls.getMethods();
    } catch (Throwable e) {
      // TODO: add proper logging and error handling
      System.err.println(e);
    }

    for (Method method : methods) {
      String targetMethodName = method.getName().replaceFirst("get", "set");
      try {
        Statement stmt =
            new Statement(
                targetClient.getClass().cast(targetClient),
                targetMethodName,
                new Object[] {getConfigValue(perTableConfig, method)});
        stmt.execute();
      } catch (NoSuchMethodException nsme) {
        // NOP - each client only implements setters for the params it needs
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private Object getConfigValue(PerTableConfig perTableConfig, Method method) {
    try {
      Class cls = perTableConfig.getClass();

      String targetMethodName = method.getName().replaceFirst("get", "set");
      Expression expression = new Expression(perTableConfig, method.getName(), new Object[0]);
      expression.execute();
      return expression.getValue();
    } catch (Throwable e) {
      // TODO: add proper logging and error handling
      System.err.println(e);
    }
    return null;
  }

  /**
   * Create an instance of the TargetClient via the default no-arg constructor. Expectation is that
   * target client specific settings may be provided prior to the calling of TargetClient.init()
   * which must be called prior to actual use of the target client returned by this factory method.
   *
   * @param tableFormatName
   * @return
   */
  public TargetClient createTargetClientForName(String tableFormatName) {
    ServiceLoader<TargetClient> loader = ServiceLoader.load(TargetClient.class);
    for (TargetClient target : loader) {
      if (target.getTableFormat().equalsIgnoreCase(tableFormatName)) {
        return target;
      }
    }
    throw new NotSupportedException("Target format is not yet supported: " + tableFormatName);
  }
}
