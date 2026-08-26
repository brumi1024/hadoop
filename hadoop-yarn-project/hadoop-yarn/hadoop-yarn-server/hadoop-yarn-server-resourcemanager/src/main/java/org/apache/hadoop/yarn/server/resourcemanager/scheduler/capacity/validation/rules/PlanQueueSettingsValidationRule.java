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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.plan.ValidatedQueuePlan.QueueKind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.plan.ValidatedQueuePlan.QueuePlanNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ClusterFacts;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationRule;
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
      issue = validateAbsoluteResourceLimits(queue, parent,
          context.getFacts());
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
      QueuePlanNode parent, ClusterFacts facts) {
    for (String label : queue.getConfiguredNodeLabels()) {
      Resource minimum = absoluteResource(queue.getCapacity(label).getVector(),
          facts);
      Resource maximum = absoluteResource(
          queue.getMaximumCapacity(label).getVector(), facts);
      if (parent != null) {
        Resource parentMaximum = absoluteResource(
            parent.getMaximumCapacity(label).getVector(), facts);
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

  private Resource absoluteResource(QueueCapacityVector vector,
      ClusterFacts facts) {
    Resource resource = Resource.newInstance(facts.getClusterResource());
    for (ResourceInformation information : resource.getResources()) {
      resource.setResourceValue(information.getName(), 0);
    }
    if (!vector.getDefinedCapacityTypes().equals(
        Set.of(ResourceUnitCapacityType.ABSOLUTE))) {
      return resource;
    }
    for (QueueCapacityVector.QueueCapacityVectorEntry entry : vector) {
      resource.setResourceValue(entry.getResourceName(),
          (long) entry.getResourceValue());
    }
    return resource;
  }

  private boolean exceeds(Resource left, Resource right,
      ClusterFacts facts) {
    return Resources.greaterThan(facts.getResourceCalculator(),
        facts.getClusterResource(), left, right);
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
