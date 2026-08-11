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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules;

import java.util.Set;
import java.util.function.Consumer;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.QueueConfigNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationRule;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

/** Ensures absolute child minimums fit within the configured parent minimum. */
public final class AbsoluteParentMinCoverageRule implements ValidationRule {
  @Override
  public String id() {
    return "absolute-parent-min-coverage";
  }
  @Override
  public Stage stage() {
    return Stage.MODEL;
  }

  @Override
  public void run(ValidationContext context,
      Consumer<ValidationIssue> sink) {
    if (!context.getModel().isLegacyQueueMode()) {
      return;
    }
    for (QueueConfigNode parent : context.getModel().getNodes().values()) {
      Resource parentMin = absoluteResource(parent.getCapacity(""));
      if (parent.getQueuePath().isRoot() || parentMin == null) {
        continue;
      }
      Resource childrenMin = Resource.newInstance(0, 0);
      boolean childHasInvalidMinMax = false;
      for (QueueConfigNode child : parent.getChildren().values()) {
        Resource childMin = absoluteResource(child.getCapacity(""));
        if (childMin == null) {
          continue;
        }
        Resource childMax = absoluteResource(child.getMaximumCapacity(""));
        if (childMax != null && exceeds(childMin, childMax)) {
          childHasInvalidMinMax = true;
        }
        Resources.addTo(childrenMin, childMin);
      }
      if (!childHasInvalidMinMax && exceeds(childrenMin, parentMin)) {
        sink.accept(new ValidationIssue(parent.getQueuePath(), null, id(),
            ValidationIssue.Severity.ERROR,
            "Parent Queues capacity: " + parentMin
                + " is less than to its children:" + childrenMin
                + " for queue:" + parent.getQueuePath().getLeafName()));
      }
    }
  }

  private Resource absoluteResource(QueueConfigNode.CapacityValue value) {
    if (value == null || !value.getVector().getDefinedCapacityTypes()
        .equals(Set.of(ResourceUnitCapacityType.ABSOLUTE))) {
      return null;
    }
    Resource resource = Resource.newInstance(0, 0);
    for (QueueCapacityVector.QueueCapacityVectorEntry entry
        : value.getVector()) {
      String name = entry.getResourceName();
      long amount = (long) entry.getResourceValue();
      if (ResourceInformation.MEMORY_URI.equals(name)) {
        resource.setMemorySize(amount);
      } else if (ResourceInformation.VCORES_URI.equals(name)) {
        resource.setVirtualCores((int) amount);
      } else if (ResourceUtils.getResourceTypes().containsKey(name)) {
        resource.setResourceInformation(name,
            ResourceInformation.newInstance(name, amount));
      }
    }
    return resource;
  }

  private boolean exceeds(Resource left, Resource right) {
    for (ResourceInformation resource : left.getResources()) {
      if (resource.getValue()
          > right.getResourceInformation(resource.getName()).getValue()) {
        return true;
      }
    }
    return false;
  }
}
