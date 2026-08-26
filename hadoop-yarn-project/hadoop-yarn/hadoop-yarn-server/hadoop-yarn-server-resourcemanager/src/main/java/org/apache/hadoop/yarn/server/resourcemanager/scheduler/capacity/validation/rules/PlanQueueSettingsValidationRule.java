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

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.plan.CompiledQueueSettings;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.plan.CompiledQueueSettings.ResourceSetting;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.plan.ValidatedQueuePlan.CapacitySetting;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.plan.ValidatedQueuePlan.QueueKind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.plan.ValidatedQueuePlan.QueuePlanNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ClusterFacts;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationRule;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Preserves configuration-only queue-constructor checks over a compiled plan.
 */
public final class PlanQueueSettingsValidationRule implements ValidationRule {
  private static final String RULE_ID = "queue-tree-build";

  @Override
  public String id() {
    return RULE_ID;
  }

  @Override
  public Stage stage() {
    return Stage.MODEL;
  }

  @Override
  public void run(ValidationContext context,
      Consumer<ValidationIssue> sink) {
    for (QueuePlanNode queue : context.getPlan().getQueues().values()) {
      ValidationIssue issue = validateQueue(queue, context);
      if (issue != null) {
        sink.accept(issue);
        return;
      }
    }
  }

  private ValidationIssue validateQueue(QueuePlanNode queue,
      ValidationContext context) {
    if (queue.isReservable() && !queue.getChildPaths().isEmpty()) {
      return error(queue, "Only Leaf Queues can be reservable for "
          + queue.getQueuePath().getFullPath());
    }

    QueuePlanNode parent = parent(queue, context);
    ValidationIssue issue = validateState(queue, parent);
    if (issue == null) {
      issue = validateLabels(queue, parent);
    }
    if (issue == null) {
      issue = validateDefaultLabelExpression(queue);
    }
    if (issue == null) {
      issue = validateAbsoluteResourceLimits(queue, parent, context);
    }
    if (issue == null) {
      issue = validateMaximumAllocation(queue, context);
    }
    if (issue == null) {
      issue = validateApplicationLifetime(queue);
    }
    if (issue == null) {
      issue = validateUserWeights(queue);
    }
    return issue;
  }

  private ValidationIssue validateState(QueuePlanNode queue,
      QueuePlanNode parent) {
    if (parent != null && queue.getConfiguredState() == QueueState.RUNNING
        && parent.getInitialState() != QueueState.RUNNING) {
      return error(queue, "The parent queue:"
          + parent.getQueuePath().getFullPath()
          + " cannot be STOPPED as the child queue:"
          + queue.getQueuePath().getFullPath() + " is in RUNNING state.");
    }
    return null;
  }

  private ValidationIssue validateLabels(QueuePlanNode queue,
      QueuePlanNode parent) {
    if (parent == null || parent.getAccessibleNodeLabels() == null
        || parent.getAccessibleNodeLabels().contains(
            RMNodeLabelsManager.ANY)) {
      return null;
    }

    Set<String> labels = queue.getAccessibleNodeLabels();
    if (labels != null && labels.contains(RMNodeLabelsManager.ANY)) {
      return error(queue, "Parent's accessible queue is not ANY(*), "
          + "but child's accessible queue is " + RMNodeLabelsManager.ANY);
    }
    Set<String> difference = new LinkedHashSet<>(labels == null
        ? Set.of() : labels);
    difference.removeAll(parent.getAccessibleNodeLabels());
    if (!difference.isEmpty()) {
      return error(queue, "Some labels of child queue is not a subset of "
          + "parent queue, these labels=[" + String.join(",", difference)
          + "]");
    }
    return null;
  }

  private ValidationIssue validateDefaultLabelExpression(
      QueuePlanNode queue) {
    if (queue.getKind() != QueueKind.LEAF
        || SchedulerUtils.checkQueueLabelExpression(
            queue.getAccessibleNodeLabels(),
            queue.getDefaultNodeLabelExpression(), null)) {
      return null;
    }
    Set<String> labels = queue.getAccessibleNodeLabels();
    String labelsText = labels == null ? "" : String.join(",", labels);
    return error(queue, "Invalid default label expression of  queue="
        + queue.getQueuePath().getFullPath()
        + " doesn't have permission to access all labels in default label "
        + "expression. labelExpression of resource request="
        + queue.getDefaultNodeLabelExpression() + ". Queue labels="
        + labelsText);
  }

  private ValidationIssue validateAbsoluteResourceLimits(QueuePlanNode queue,
      QueuePlanNode parent, ValidationContext context) {
    ClusterFacts facts = context.getFacts();
    Set<String> configuredResourceTypes = configuredResourceTypes(
        context.getPlan().getResourceTypes());
    for (String label : queue.getConfiguredNodeLabels()) {
      Resource minimum = absoluteResource(queue.getCapacity(label),
          configuredResourceTypes);
      Resource maximum = absoluteResource(queue.getMaximumCapacity(label),
          configuredResourceTypes);
      if (parent != null) {
        Resource parentMaximum = absoluteResource(
            parent.getMaximumCapacity(label), configuredResourceTypes);
        if (!Resources.isNone(parentMaximum)
            && exceeds(maximum, parentMaximum, facts)) {
          return error(queue, "Max resource configuration " + maximum
              + " is greater than parents max value:" + parentMaximum
              + " in queue:" + queue.getQueuePath().getFullPath());
        }
      }
      if (!Resources.isNone(maximum) && exceeds(minimum, maximum, facts)) {
        return error(queue, "Min resource configuration " + minimum
            + " is greater than its max value:" + maximum + " in queue:"
            + queue.getQueuePath().getFullPath());
      }
    }
    return null;
  }

  private Resource absoluteResource(CapacitySetting setting,
      Set<String> configuredResourceTypes) {
    Resource resource = Resource.newInstance(0, 0);
    if (setting == null || setting.getRawValue() == null) {
      return resource;
    }
    QueueCapacityVector vector = setting.getVector();
    if (!vector.getDefinedCapacityTypes().equals(
        Set.of(ResourceUnitCapacityType.ABSOLUTE))) {
      return resource;
    }
    for (QueueCapacityVector.QueueCapacityVectorEntry entry : vector) {
      if (entry.getVectorResourceType() == ResourceUnitCapacityType.ABSOLUTE) {
        String name = entry.getResourceName();
        long amount = (long) entry.getResourceValue();
        if (ResourceInformation.MEMORY_URI.equals(name)) {
          resource.setMemorySize(amount);
        } else if (ResourceInformation.VCORES_URI.equals(name)) {
          resource.setVirtualCores((int) amount);
        } else if (configuredResourceTypes.contains(name)
            || ResourceUtils.getResourceTypes().containsKey(name)) {
          resource.setResourceInformation(name,
              ResourceInformation.newInstance(name, amount));
        }
      }
    }
    return resource;
  }

  private Set<String> configuredResourceTypes(String resourceTypes) {
    Set<String> result = new LinkedHashSet<>();
    for (String resourceType : resourceTypes.split(",")) {
      if (!resourceType.trim().isEmpty()) {
        result.add(resourceType.trim());
      }
    }
    return result;
  }

  private boolean exceeds(Resource left, Resource right,
      ClusterFacts facts) {
    return Resources.greaterThan(facts.getResourceCalculator(),
        facts.getClusterResource(), left, right);
  }

  private ValidationIssue validateMaximumAllocation(QueuePlanNode queue,
      ValidationContext context) {
    ResourceSetting maximum = queue.getSettings().getMaximumAllocation();
    ResourceSetting clusterMaximum = context.getPlan().getSchedulerSettings()
        .getMaximumAllocation();
    for (Map.Entry<String, Long> entry : maximum.getValues().entrySet()) {
      if (entry.getValue() > clusterMaximum.getValue(entry.getKey())) {
        return error(queue,
            "Queue maximum allocation cannot be larger than the cluster "
                + "setting for queue " + queue.getQueuePath()
                + " max allocation per queue: " + resourceText(maximum)
                + " cluster setting: " + resourceText(clusterMaximum));
      }
    }
    return null;
  }

  private ValidationIssue validateApplicationLifetime(QueuePlanNode queue) {
    if (queue.getQueuePath().isRoot()) {
      return null;
    }
    CompiledQueueSettings settings = queue.getSettings();
    long maximum = settings.getMaximumApplicationLifetime();
    long defaultLifetime = settings.getDefaultApplicationLifetime();
    if (maximum > 0 && defaultLifetime > maximum) {
      return error(queue, "Default lifetime " + defaultLifetime
          + " can't exceed maximum lifetime " + maximum);
    }
    return null;
  }

  private ValidationIssue validateUserWeights(QueuePlanNode queue) {
    if (queue.getKind() != QueueKind.LEAF) {
      return null;
    }
    float queueUserLimit = Math.min(100.0f, queue.getUserLimit());
    for (Map.Entry<String, Float> entry
        : queue.getSettings().getUserWeights().entrySet()) {
      float weight = entry.getValue();
      if (weight < 0.0f || weight > 100.0f / queueUserLimit) {
        return error(queue, "Weight (" + weight + ") for user \""
            + entry.getKey() + "\" must be between 0 and 100 / "
            + queueUserLimit + " (= " + 100.0f / queueUserLimit
            + ", the number of concurrent active users in "
            + queue.getQueuePath().getFullPath() + ")");
      }
    }
    return null;
  }

  private String resourceText(ResourceSetting setting) {
    StringBuilder result = new StringBuilder("<memory:")
        .append(setting.getValue(ResourceInformation.MEMORY_URI))
        .append(", vCores:")
        .append(setting.getValue(ResourceInformation.VCORES_URI));
    setting.getValues().forEach((name, value) -> {
      if (!ResourceInformation.MEMORY_URI.equals(name)
          && !ResourceInformation.VCORES_URI.equals(name)) {
        result.append(", ").append(name).append(":").append(value);
      }
    });
    return result.append(">").toString();
  }

  private QueuePlanNode parent(QueuePlanNode queue,
      ValidationContext context) {
    QueuePath parentPath = queue.getParentPath();
    return parentPath == null ? null : context.getPlan().getQueue(parentPath);
  }

  private ValidationIssue error(QueuePlanNode queue, String message) {
    return new ValidationIssue(queue.getQueuePath(), null, RULE_ID,
        ValidationIssue.Severity.ERROR, message);
  }
}
