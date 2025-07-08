/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.core.config;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.polaris.core.entity.CatalogEntity;

/** Realm-specific configuration used to retrieve runtime parameters. */
public interface PolarisRealmConfiguration {

  /**
   * Retrieve the current value for a configuration key. May be null if not set.
   *
   * @param configName the name of the configuration key to check
   * @return the current value set for the configuration key, or null if not set
   * @param <T> the type of the configuration value
   */
  <T> @Nullable T getConfiguration(String configName);

  /**
   * Retrieve the current value for a configuration key. If not set, return the non-null default
   * value.
   *
   * @param configName the name of the configuration key to check
   * @param defaultValue the default value if the configuration key has no value
   * @return the current value or the supplied default value
   * @param <T> the type of the configuration value
   */
  <T> @Nonnull T getConfiguration(String configName, @Nonnull T defaultValue);

  /**
   * Retrieve the current value for a configuration.
   *
   * @param config the configuration to load
   * @return the current value set for the configuration key or null if not set
   * @param <T> the type of the configuration value
   */
  <T> @Nonnull T getConfiguration(PolarisConfiguration<T> config);

  /**
   * Retrieve the current value for a configuration, overriding with a catalog config if it is
   * present.
   *
   * @param catalogEntity the catalog to check for an override
   * @param config the configuration to load
   * @return the current value set for the configuration key or null if not set
   * @param <T> the type of the configuration value
   */
  <T> @Nonnull T getConfiguration(
      @Nonnull CatalogEntity catalogEntity, PolarisConfiguration<T> config);
}
