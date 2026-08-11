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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.CSConfigModel;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption.PreemptionManager;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

/** Build-time dependencies used while constructing a Capacity Scheduler tree. */
@Private
public interface QueueBuildContext {
  CapacitySchedulerConfiguration getConfiguration();

  CSConfigModel getConfigModel();

  Resource getMinimumAllocation();

  Resource getClusterResource();

  ResourceUsage getClusterResourceUsage();

  ResourceCalculator getResourceCalculator();

  CapacitySchedulerQueueManager getQueueManager();

  ConfiguredNodeLabels getConfiguredNodeLabelsForAllQueues();

  RMNodeLabelsManager getLabelManager();

  PreemptionManager getPreemptionManager();

  ActivitiesManager getActivitiesManager();

  boolean isHierarchyValidationSkipped();

  CSQueueMetrics createQueueMetrics(QueuePath queuePath, CSQueue parent,
      boolean enableUserMetrics, Configuration configuration);
}
