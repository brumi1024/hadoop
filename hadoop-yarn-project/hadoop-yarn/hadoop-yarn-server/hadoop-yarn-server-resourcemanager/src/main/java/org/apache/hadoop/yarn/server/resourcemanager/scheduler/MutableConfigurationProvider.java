/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationResult;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

/**
 * Interface for allowing changing scheduler configurations.
 */
public interface MutableConfigurationProvider {

  /**
   * Get the acl mutation policy for this configuration provider.
   * @return The acl mutation policy.
   */
  ConfigurationMutationACLPolicy getAclMutationPolicy();

  /**
   * Called when a new ResourceManager is starting/becomes active. Ensures
   * configuration is up-to-date.
   * @throws Exception if configuration could not be refreshed from store
   */
  void reloadConfigurationFromStore() throws Exception;

  /**
   * Validates, stores, and activates one configuration mutation atomically.
   * @param user user requesting the mutation
   * @param confUpdate requested changes
   * @return validation result and resulting configuration version
   * @throws Exception when storing or activating a valid mutation fails
   */
  MutationResult applyMutation(UserGroupInformation user,
      SchedConfUpdateInfo confUpdate) throws Exception;

  /**
   * Applies a mutation only when the supplied version is still current.
   * @param user user requesting the mutation
   * @param confUpdate requested changes
   * @param expectedConfigVersion version observed by the caller
   * @return validation result and resulting configuration version
   * @throws Exception when the version is stale or the mutation fails
   */
  MutationResult applyMutation(UserGroupInformation user,
      SchedConfUpdateInfo confUpdate, long expectedConfigVersion)
      throws Exception;

  /**
   * Builds and validates a proposed mutation under provider serialization.
   * @param confUpdate requested changes
   * @return findings and the source version
   * @throws Exception when the proposed configuration cannot be built
   */
  MutationResult validateMutation(SchedConfUpdateInfo confUpdate)
      throws Exception;

  /**
   * Refreshes the scheduler while serializing against mutable changes.
   * @param configuration refreshed ResourceManager configuration
   * @throws Exception when scheduler refresh fails
   */
  void refreshScheduler(Configuration configuration) throws Exception;

  /** Raised before a mutation when its expected version is stale. */
  final class VersionMismatchException extends IOException {
    private static final long serialVersionUID = 1L;

    private final long expectedVersion;
    private final long actualVersion;

    public VersionMismatchException(long expectedVersion, long actualVersion) {
      super("Expected scheduler configuration version " + expectedVersion
          + " but current version is " + actualVersion);
      this.expectedVersion = expectedVersion;
      this.actualVersion = actualVersion;
    }

    public long getExpectedVersion() {
      return expectedVersion;
    }

    public long getActualVersion() {
      return actualVersion;
    }
  }

  /** Atomic validation and version result for one provider operation. */
  final class MutationResult {
    private final ValidationResult validationResult;
    private final long configVersion;

    public MutationResult(ValidationResult validationResult,
        long configVersion) {
      this.validationResult = validationResult;
      this.configVersion = configVersion;
    }

    public ValidationResult getValidationResult() {
      return validationResult;
    }

    public long getConfigVersion() {
      return configVersion;
    }
  }

  /**
   * Apply the changes on top of the actual configuration.
   * @param oldConfiguration actual configuration
   * @param confUpdate changelist
   * @return new configuration with the applied changed
   * @throws IOException if the merge failed
   */
  Configuration applyChanges(Configuration oldConfiguration,
                             SchedConfUpdateInfo confUpdate) throws IOException;

  /**
   * Returns scheduler configuration cached in this provider.
   * @return scheduler configuration.
   */
  Configuration getConfiguration();

  /**
   * Get the last updated scheduler config version.
   * @return Last updated scheduler config version.
   * @throws Exception exception occurs.
   */
  long getConfigVersion() throws Exception;

  void formatConfigurationInStore(Configuration conf) throws Exception;

  /**
   * Closes the configuration provider, releasing any required resources.
   * @throws IOException on failure to close
   */
  void close() throws IOException;
}
