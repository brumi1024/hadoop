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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ConfiguredNodeLabels;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueBuildContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.CSConfigModel;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption.PreemptionManager;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

/** Read-only queue construction context used by isolated validation builds. */
public final class ValidationQueueBuildContext implements QueueBuildContext {
  private final CSConfigModel model;
  private final ClusterFacts facts;
  private final CapacitySchedulerConfiguration configuration;
  private final ResourceUsage clusterUsage = new ResourceUsage();
  private final RMNodeLabelsManager labelManager;
  private final ConfiguredNodeLabels configuredNodeLabels;
  private final PreemptionManager preemptionManager = new PreemptionManager();

  public ValidationQueueBuildContext(CSConfigModel model, ClusterFacts facts) {
    this.model = model;
    this.facts = facts;
    this.configuration = new CapacitySchedulerConfiguration(
        new Configuration(false), false);
    model.getRawProperties().forEach(configuration::set);
    QueueMetrics.setConfigurationValidation(configuration, true);
    if (facts.getResourcesByLabel().isEmpty()) {
      labelManager = new RMNodeLabelsManager();
    } else {
      labelManager = new SnapshotNodeLabelsManager(
          facts.getResourcesByLabel(), facts.getNodeLabels());
    }
    labelManager.init(configuration);
    configuredNodeLabels = new ConfiguredNodeLabels(configuration);
  }

  public CapacitySchedulerConfiguration getConfiguration() {
    return configuration;
  }

  public CSConfigModel getConfigModel() {
    return model;
  }

  public Resource getMinimumAllocation() {
    return facts.getMinimumAllocation();
  }

  public Resource getClusterResource() {
    return facts.getClusterResource();
  }

  public ResourceUsage getClusterResourceUsage() {
    return clusterUsage;
  }

  public ResourceCalculator getResourceCalculator() {
    return facts.getResourceCalculator();
  }

  public boolean isHierarchyValidationSkipped() {
    return facts.isHierarchyValidationSkipped();
  }

  public CSQueueMetrics createQueueMetrics(QueuePath path, CSQueue parent,
      boolean enableUserMetrics, Configuration conf) {
    return CSQueueMetrics.forQueue(path.getFullPath(), parent,
        enableUserMetrics, conf);
  }

  public ConfiguredNodeLabels getConfiguredNodeLabelsForAllQueues() {
    return configuredNodeLabels;
  }

  public RMNodeLabelsManager getLabelManager() {
    return labelManager;
  }

  public PreemptionManager getPreemptionManager() {
    return preemptionManager;
  }

}
