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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.LegacyFallbackReason;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationResult;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.YarnConfigurationStore.LogMutation;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
      CompiledMutation compiled = applyCompiledMutation(scheduler, proposed,
          changes, user);
      if (compiled.result() != null) {
        return compiled.result();
      }
      LOG.debug("Using legacy scheduler mutation compatibility: {}",
          compiled.fallbackReasons());
      return applyLegacyMutation(scheduler, proposed, changes, user);
    } finally {
      mutationPipelineLock.unlock();
    }
  }

  private record CompiledMutation(ValidationResult result,
      List<LegacyFallbackReason> fallbackReasons) {
    private CompiledMutation {
      fallbackReasons = List.copyOf(fallbackReasons);
    }
  }

  private CompiledMutation applyCompiledMutation(CapacityScheduler scheduler,
      CapacitySchedulerConfiguration proposed, Map<String, String> changes,
      UserGroupInformation user) throws Exception {
    List<LegacyFallbackReason> reasons = new ArrayList<>();
    // Persistent stores do not promise failure-before-commit. Subclasses may
    // also add fallible confirmation or version lookup, so names are not enough.
    if (confStore.getClass() != InMemoryConfigurationStore.class) {
      reasons.add(new LegacyFallbackReason(
          LegacyFallbackReason.Code.STORE_COMMIT_PROTOCOL, null,
          YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
          confStore.getClass().getName(),
          "Store requires legacy commit acknowledgement"));
    }
    if (scheduler.getClass() != CapacityScheduler.class) {
      reasons.add(new LegacyFallbackReason(
          LegacyFallbackReason.Code.CUSTOM_SCHEDULER, null,
          YarnConfiguration.RM_SCHEDULER, scheduler.getClass().getName(),
          "Custom scheduler requires legacy activation"));
    }
    if (!reasons.isEmpty()) {
      return new CompiledMutation(null, reasons);
    }
    return scheduler.runWithStableQueueConfiguration(() -> {
      CompileResult compiled = new CSConfigValidationEngine().compile(
          proposed.getModel(), ClusterFacts.capture(scheduler));
      if (compiled.requiresLegacyValidation()) {
        return new CompiledMutation(null, compiled.getFallbackReasons());
      }
      ValidationResult validation = compiled.asValidationResult();
      CompiledMutation result = new CompiledMutation(validation, List.of());
      if (!validation.isValid()) {
        return result;
      }
      // Prepare everything before the native in-memory commit. After confirm,
      // only non-rejecting live publication and this snapshot assignment remain.
      ConfigSnapshot next = new ConfigSnapshot(proposed,
          current.getStoreVersion() + 1);
      LogMutation log = new LogMutation(changes, user.getShortUserName());
      confStore.logMutation(log);
      scheduler.applyCompiledConfiguration(proposed, rmContext,
          compiled.getPlan(), () -> {
            confStore.confirmMutation(log, true);
            return null;
          });
      current = next;
      return result;
    });
  }

  private ValidationResult applyLegacyMutation(CapacityScheduler scheduler,
      CapacitySchedulerConfiguration proposed, Map<String, String> changes,
      UserGroupInformation user) throws Exception {
    CSConfigModel model = proposed.getModel();
    ClusterFacts facts = ClusterFacts.capture(scheduler);
    ValidationResult result = new CSConfigValidationEngine().validate(
        model, facts);
    if (!result.isValid()) {
      return result;
    }

    LogMutation log = new LogMutation(changes, user.getShortUserName());
    confStore.logMutation(log);
    try {
      scheduler.reinitializePreValidated(proposed, rmContext,
          model, facts);
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
