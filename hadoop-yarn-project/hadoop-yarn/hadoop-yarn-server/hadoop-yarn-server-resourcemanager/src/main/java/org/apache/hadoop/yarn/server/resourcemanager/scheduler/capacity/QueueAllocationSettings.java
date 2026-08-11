/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.QueueConfigNode;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.UNDEFINED;

/**
 * This class determines minimum and maximum allocation settings based on the
 * {@link CapacitySchedulerConfiguration} and other queue
 * properties.
 **/
public class QueueAllocationSettings {
  private final Resource minimumAllocation;
  private Resource maximumAllocation;

  public QueueAllocationSettings(Resource minimumAllocation) {
    this.minimumAllocation = minimumAllocation;
  }

  void setupMaximumAllocation(CapacitySchedulerConfiguration configuration,
      QueueConfigNode queueNode, CSQueue parent) {
    QueuePath queuePath = queueNode.getQueuePath();
    Resource clusterMax = ResourceUtils
        .fetchMaximumAllocationFromConfig(configuration);
    String rawMaximum = queueNode.getRawProperty(
        CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION);
    Resource queueMax = rawMaximum == null ? Resources.none()
        : ResourceUtils.createResourceFromString(rawMaximum,
            ResourceUtils.getResourcesTypeInfo());

    maximumAllocation = Resources.clone(
        parent == null ? clusterMax : parent.getMaximumAllocation());

    String errMsg =
        "Queue maximum allocation cannot be larger than the cluster setting"
            + " for queue " + queuePath
            + " max allocation per queue: %s"
            + " cluster setting: " + clusterMax;

    if (queueMax == Resources.none()) {
      // Handle backward compatibility
      long queueMemory = parseLong(queueNode.getRawProperty(
          CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION_MB));
      int queueVcores = (int) parseLong(queueNode.getRawProperty(
          CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION_VCORES));
      if (queueMemory != UNDEFINED) {
        maximumAllocation.setMemorySize(queueMemory);
      }

      if (queueVcores != UNDEFINED) {
        maximumAllocation.setVirtualCores(queueVcores);
      }

      if ((queueMemory != UNDEFINED && queueMemory > clusterMax.getMemorySize()
          || (queueVcores != UNDEFINED
          && queueVcores > clusterMax.getVirtualCores()))) {
        throw new IllegalArgumentException(
            String.format(errMsg, maximumAllocation));
      }
    } else {
      // Queue level maximum-allocation can't be larger than cluster setting
      for (ResourceInformation ri : queueMax.getResources()) {
        if (ri.compareTo(clusterMax.getResourceInformation(ri.getName())) > 0) {
          throw new IllegalArgumentException(String.format(errMsg, queueMax));
        }

        maximumAllocation.setResourceInformation(ri.getName(), ri);
      }
    }
  }

  private static long parseLong(String value) {
    return value == null ? (long) UNDEFINED : Long.parseLong(value);
  }

  public Resource getMinimumAllocation() {
    return minimumAllocation;
  }

  public Resource getMaximumAllocation() {
    return maximumAllocation;
  }
}
