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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ConfigurationMutationACLPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ConfigurationMutationACLPolicyFactory;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.MutableConfigurationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.CSConfigModel;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.CSConfigValidationEngine;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ClusterFacts;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.CompileResult;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationResult;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.YarnConfigurationStore.LogMutation;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantLock;

/**
 * CS configuration provider which implements
 * {@link MutableConfigurationProvider} for modifying capacity scheduler
 * configuration.
 */
public class MutableCSConfigurationProvider implements CSConfigurationProvider,
    MutableConfigurationProvider {

  public static final Logger LOG =
      LoggerFactory.getLogger(MutableCSConfigurationProvider.class);

  private volatile ConfigSnapshot current;
  private YarnConfigurationStore confStore;
  private ConfigurationMutationACLPolicy aclMutationPolicy;
  private RMContext rmContext;

  private final ReentrantLock mutationPipelineLock = new ReentrantLock();

  public MutableCSConfigurationProvider(RMContext rmContext) {
    this.rmContext = rmContext;
  }

  // Unit test can overwrite this method
  protected Configuration getInitSchedulerConfig() {
    Configuration initialSchedConf = new Configuration(false);
    initialSchedConf.
        addResource(YarnConfiguration.CS_CONFIGURATION_FILE);
    return initialSchedConf;
  }

  @Override
  public void init(Configuration config) throws IOException {
    this.confStore = YarnConfigurationStoreFactory.getStore(config);
    CapacitySchedulerConfiguration initial = initialSchedulerConfiguration();
    try {
      confStore.initialize(config, initial, rmContext);
      confStore.checkVersion();
      // The store may already contain a configuration from an earlier RM.
      current = new ConfigSnapshot(new CapacitySchedulerConfiguration(
          confStore.retrieve(), false), confStore.getConfigVersion());
    } catch (Exception e) {
      throw new IOException(e);
    }
    this.aclMutationPolicy = ConfigurationMutationACLPolicyFactory
        .getPolicy(config);
    aclMutationPolicy.init(config, rmContext);
  }

  @Override
  public void close() throws IOException {
    confStore.close();
  }

  @VisibleForTesting
  protected YarnConfigurationStore getConfStore() {
    return confStore;
  }

  @Override
  public CapacitySchedulerConfiguration loadConfiguration(Configuration
      configuration) throws IOException {
    Configuration loadedConf = new Configuration(current.getConfiguration());
    loadedConf.addResource(configuration);
    return new CapacitySchedulerConfiguration(loadedConf, false);
  }

  @Override
  public Configuration getConfiguration() {
    return new CapacitySchedulerConfiguration(current.getConfiguration(),
        false);
  }

  @Override
  public long getConfigVersion() throws Exception {
    return current.getStoreVersion();
  }

  @Override
  public ConfigurationMutationACLPolicy getAclMutationPolicy() {
    return aclMutationPolicy;
  }

  @Override
  public ValidationResult applyMutation(UserGroupInformation user,
      SchedConfUpdateInfo confUpdate) throws Exception {
    mutationPipelineLock.lock();
    try {
      CapacityScheduler scheduler = (CapacityScheduler) rmContext.getScheduler();
      CapacitySchedulerConfiguration proposed =
          new CapacitySchedulerConfiguration(current.getConfiguration(), false);
      Map<String, String> changes =
          ConfigurationUpdateAssembler.constructKeyValueConfUpdate(
              proposed, confUpdate);
      applyMutation(proposed, changes);
      CSConfigModel model = proposed.getModel();
      ClusterFacts facts = ClusterFacts.capture(scheduler);
      CompileResult compiled = new CSConfigValidationEngine().compile(
          model, facts);
      ValidationResult result = compiled.asValidationResult();
      if (!result.isValid()) {
        return result;
      }

      LogMutation log = new LogMutation(changes, user.getShortUserName());
      confStore.logMutation(log);
      try {
        scheduler.reinitializePreValidated(proposed, rmContext, compiled);
        confStore.confirmMutation(log, true);
        current = new ConfigSnapshot(proposed, confStore.getConfigVersion());
      } catch (Throwable failure) {
        confStore.confirmMutation(log, false);
        if (failure instanceof Exception) {
          throw (Exception) failure;
        }
        throw new IOException("Failed to activate scheduler configuration",
            failure);
      }
      return result;
    } finally {
      mutationPipelineLock.unlock();
    }
  }

  @Override
  public <T> T runUnderMutationLock(Callable<T> operation) throws Exception {
    mutationPipelineLock.lock();
    try {
      return operation.call();
    } finally {
      mutationPipelineLock.unlock();
    }
  }

  @Override
  public boolean isMutationLockHeld() {
    return mutationPipelineLock.isHeldByCurrentThread();
  }

  public Configuration applyChanges(Configuration oldConfiguration,
                           SchedConfUpdateInfo confUpdate) throws IOException {
    CapacitySchedulerConfiguration proposedConf =
            new CapacitySchedulerConfiguration(oldConfiguration, false);
    Map<String, String> kvUpdate
            = ConfigurationUpdateAssembler.constructKeyValueConfUpdate(proposedConf, confUpdate);
    applyMutation(proposedConf, kvUpdate);
    return proposedConf;
  }

  private void applyMutation(Configuration conf, Map<String, String> kvUpdate) {
    for (Map.Entry<String, String> kv : kvUpdate.entrySet()) {
      if (kv.getValue() == null) {
        conf.unset(kv.getKey());
      } else {
        conf.set(kv.getKey(), kv.getValue());
      }
    }
  }

  @Override
  public void formatConfigurationInStore(Configuration config)
      throws Exception {
    mutationPipelineLock.lock();
    ConfigSnapshot beforeFormat = current;
    try {
      confStore.format();
      CapacitySchedulerConfiguration initial = initialSchedulerConfiguration();
      confStore.initialize(config, initial, rmContext);
      confStore.checkVersion();
      current = new ConfigSnapshot(initial, confStore.getConfigVersion());
      rmContext.getRMAdminService().refreshQueues();
    } catch (Exception e) {
      current = beforeFormat;
      try {
        confStore.format();
        confStore.initialize(config, beforeFormat.getConfiguration(),
            rmContext);
        confStore.checkVersion();
        current = new ConfigSnapshot(beforeFormat.getConfiguration(),
            confStore.getConfigVersion());
      } catch (Exception restoreFailure) {
        e.addSuppressed(restoreFailure);
      }
      throw new IOException(e);
    } finally {
      mutationPipelineLock.unlock();
    }
  }

  private CapacitySchedulerConfiguration initialSchedulerConfiguration() {
    Configuration initialSchedConf = getInitSchedulerConfig();
    return new CapacitySchedulerConfiguration(initialSchedConf, false);
  }

  @Override
  public void reloadConfigurationFromStore() throws Exception {
    mutationPipelineLock.lock();
    try {
      current = new ConfigSnapshot(new CapacitySchedulerConfiguration(
          confStore.retrieve(), false), confStore.getConfigVersion());
    } finally {
      mutationPipelineLock.unlock();
    }
  }
}
