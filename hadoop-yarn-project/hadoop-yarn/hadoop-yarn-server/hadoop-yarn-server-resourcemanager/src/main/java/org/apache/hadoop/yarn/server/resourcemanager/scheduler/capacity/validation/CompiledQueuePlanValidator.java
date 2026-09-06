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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueAllocationSettings;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.UserWeights;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.CapacitySetting;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.DiagnosticStage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.OldQueueKind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.OldQueueSnapshot;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.PlanDiagnostic;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.PlacementRuleDefinition;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.QueueKind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.QueueListDefinition;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.QueuePlanNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.ResourceValues;

/** Evaluates every represented validation rule using immutable plan values. */
final class CompiledQueuePlanValidator {
  private static final Pattern PORTABLE_QUEUE_NAME =
      Pattern.compile("[a-zA-Z0-9_-]+");
  private static final Pattern ABSOLUTE_CAPACITY = Pattern.compile(
      CapacitySchedulerConfiguration.PATTERN_FOR_ABSOLUTE_RESOURCE);

  private final CompiledQueuePlanCapacityValidator capacityValidator =
      new CompiledQueuePlanCapacityValidator();

  ValidationResult validate(ValidatedQueuePlan plan) {
    List<ValidationIssue> issues = new ArrayList<>();
    diagnostics(plan, DiagnosticStage.MODEL, issues::add);
    validateAllocation(plan, ResourceInformation.MEMORY_URI,
        "memory-allocation", "memory", issues::add);
    validateAllocation(plan, ResourceInformation.VCORES_URI,
        "vcores-allocation", "vcores", issues::add);
    validatePlacementDuplicates(plan, issues::add);
    validateQueueNames(plan, issues::add);
    capacityValidator.validateModeUniformityRules(plan, issues::add);
    capacityValidator.validatePercentageSumRules(plan, issues::add);
    validateNestedManagedParents(plan, issues::add);
    capacityValidator.validateAbsoluteParentCoverageRules(plan, issues::add);

    if (hasNoErrors(issues)) {
      try {
        validateQueueTree(plan);
      } catch (Exception failure) {
        issues.add(new ValidationIssue(null, null, "queue-tree-build",
            ValidationIssue.Severity.ERROR, message(failure)));
      }
    }
    if (hasNoErrors(issues)) {
      validateTransitions(plan, issues::add);
      validatePlacementRules(plan, issues::add);
      capacityValidator.validateEffectiveResources(plan, issues::add);
    }
    return new ValidationResult(issues);
  }

  private void validateAllocation(ValidatedQueuePlan plan,
      String resourceName, String ruleId, String noun,
      Consumer<ValidationIssue> sink) {
    if (plan.getDiagnostics().stream().anyMatch(diagnostic ->
        diagnostic.stage() == DiagnosticStage.MODEL
            && diagnostic.code().equals(ruleId))) {
      return;
    }
    long minimum = plan.getSchedulerSettings().ruleMinimumAllocation()
        .value(resourceName);
    long maximum = plan.getSchedulerSettings().ruleMaximumAllocation()
        .value(resourceName);
    if (minimum <= 0L || minimum > maximum) {
      sink.accept(new ValidationIssue(null, null, ruleId,
          ValidationIssue.Severity.ERROR,
          "Invalid resource scheduler " + noun
              + " allocation configuration, min and max should be greater "
              + "than 0, max should be no smaller than min."));
    }
  }

  private void validatePlacementDuplicates(ValidatedQueuePlan plan,
      Consumer<ValidationIssue> sink) {
    Set<String> distinct = new HashSet<>();
    for (String rule : plan.getSchedulerSettings().rawPlacementRules()) {
      if (!distinct.add(rule)) {
        sink.accept(new ValidationIssue(null,
            CapacitySchedulerConfiguration.QUEUE_MAPPING,
            "placement-rule-duplicates", ValidationIssue.Severity.ERROR,
            "Invalid PlacementRule inputs which contains duplicate rule "
                + "strings"));
        return;
      }
    }
  }

  private void validateQueueNames(ValidatedQueuePlan plan,
      Consumer<ValidationIssue> sink) {
    for (QueueListDefinition queueList
        : plan.getSchedulerSettings().queueLists()) {
      for (String rawComponent : queueList.components()) {
        String component = rawComponent.trim();
        if (component.isEmpty()) {
          sink.accept(new ValidationIssue(null, queueList.propertyKey(),
              "queue-name", ValidationIssue.Severity.ERROR,
              "Queue list contains an empty component"));
        } else if (component.contains(".")) {
          sink.accept(new ValidationIssue(null, queueList.propertyKey(),
              "queue-name", ValidationIssue.Severity.ERROR,
              "Queue list component '" + component
                  + "' contains an embedded dot"));
        }
      }
    }
    for (QueuePlanNode node : plan.getQueues().values()) {
      QueuePath path = new QueuePath(node.queuePath());
      if (path.hasEmptyPart()) {
        sink.accept(new ValidationIssue(path, null, "queue-name",
            ValidationIssue.Severity.ERROR,
            "Queue path contains an empty component"));
        continue;
      }
      for (String component : node.queuePath().split("\\.", -1)) {
        if (!PORTABLE_QUEUE_NAME.matcher(component).matches()) {
          sink.accept(new ValidationIssue(path, null, "queue-name",
              ValidationIssue.Severity.WARNING,
              "Queue name component '" + component
                  + "' contains characters outside [a-zA-Z0-9_-]"));
        }
      }
    }
  }

  private void validateNestedManagedParents(ValidatedQueuePlan plan,
      Consumer<ValidationIssue> sink) {
    for (QueuePlanNode node : plan.getQueues().values()) {
      QueuePlanNode parent = parent(plan, node);
      if (node.kind() == QueueKind.MANAGED_PARENT && parent != null
          && parent.kind() == QueueKind.MANAGED_PARENT) {
        sink.accept(new ValidationIssue(new QueuePath(node.queuePath()), null,
            "nested-managed-parent", ValidationIssue.Severity.ERROR,
            "Auto creation enabled parent queue " + node.queuePath()
                + " cannot be configured below auto creation enabled parent "
                + parent.queuePath()));
      }
    }
  }

  private void validateQueueTree(ValidatedQueuePlan plan) throws IOException {
    if (plan.getRoot().childPaths().isEmpty()) {
      throw new IllegalStateException(
          "Queue configuration missing child queue names for root");
    }
    validateQueue(plan, plan.getRoot(), Map.of());
  }

  private void validateQueue(ValidatedQueuePlan plan, QueuePlanNode queue,
      Map<String, Map<String, Long>> parentMaximums) throws IOException {
    firstQueueDiagnostic(plan, queue.queuePath());
    validateCapacityTypeAcrossLabels(plan, queue);
    Map<String, Map<String, Long>> maximums =
        validateAbsoluteLimits(plan, queue, parentMaximums);
    validateMaximumAllocation(plan, queue);
    if (queue.kind() == QueueKind.LEAF) {
      validateDefaultLabelExpression(queue);
      UserWeights.validate(queue.settings().userWeights(),
          Math.min(100F, queue.settings().applications().userLimit()),
          queue.queuePath());
    }
    capacityValidator.validateManagedParentTemplate(plan, queue);
    for (String childPath : queue.childPaths()) {
      validateQueue(plan, plan.getQueue(childPath), maximums);
    }
    capacityValidator.validateConstructorBranch(plan, queue);
  }

  private void firstQueueDiagnostic(ValidatedQueuePlan plan, String path) {
    for (PlanDiagnostic diagnostic : plan.getDiagnostics()) {
      if (diagnostic.stage() == DiagnosticStage.QUEUE_SETTINGS
          && (path.equals(diagnostic.queuePath())
              || diagnostic.queuePath() == null
                  && plan.getQueue(path).kind() == QueueKind.LEAF)) {
        throw new IllegalArgumentException(diagnostic.message());
      }
    }
  }

  private void validateCapacityTypeAcrossLabels(ValidatedQueuePlan plan,
      QueuePlanNode queue) {
    if (!plan.getSchedulerSettings().legacyQueueMode()
        || queue.queuePath().equals("root")) {
      return;
    }
    Boolean absolute = null;
    for (String label : queue.configuredNodeLabels()) {
      CapacitySetting setting = queue.capacity(label);
      boolean localAbsolute = setting != null && setting.rawValue() != null
          && ABSOLUTE_CAPACITY.matcher(setting.rawValue()).find();
      if (absolute == null) {
        absolute = localAbsolute;
      } else if (absolute != localAbsolute) {
        throw new IllegalArgumentException("Queue '" + queue.queuePath()
            + "' should use either percentage based capacity configuration "
            + "or absolute resource.");
      }
    }
  }

  private Map<String, Map<String, Long>> validateAbsoluteLimits(
      ValidatedQueuePlan plan, QueuePlanNode queue,
      Map<String, Map<String, Long>> parentMaximums) {
    Map<String, Map<String, Long>> maximums = new LinkedHashMap<>();
    for (String label : queue.configuredNodeLabels()) {
      Map<String, Long> minimum = constructorAbsolute(queue.capacity(label));
      Map<String, Long> maximum = constructorAbsolute(
          queue.maximumCapacity(label));
      if (!parentMaximums.isEmpty()) {
        Map<String, Long> parentMaximum = parentMaximums.getOrDefault(label, Map.of());
        if (!allZero(parentMaximum) && capacityValidator.greater(maximum,
            parentMaximum, plan.getFacts().clusterResource(),
            plan.getFacts().calculatorSemantics())) {
          throw new IllegalArgumentException("Max resource configuration "
              + resourceText(maximum) + " is greater than parents max value:"
              + resourceText(parentMaximum) + " in queue:"
              + queue.queuePath());
        }
        if (allZero(maximum) && !allZero(minimum)
            && !allZero(parentMaximum)) {
          maximum = parentMaximum;
        }
      }
      if (!allZero(maximum) && capacityValidator.greater(minimum, maximum,
          plan.getFacts().clusterResource(),
          plan.getFacts().calculatorSemantics())) {
        throw new IllegalArgumentException("Min resource configuration "
            + resourceText(minimum) + " is greater than its max value:"
            + resourceText(maximum) + " in queue:" + queue.queuePath());
      }
      maximums.put(label, maximum);
    }
    return maximums;
  }

  private Map<String, Long> constructorAbsolute(CapacitySetting setting) {
    Map<String, Long> absolute = capacityValidator.absolute(setting);
    if (absolute == null) {
      return Map.of(ResourceInformation.MEMORY_URI, 0L,
          ResourceInformation.VCORES_URI, 0L);
    }
    return absolute;
  }

  private void validateMaximumAllocation(ValidatedQueuePlan plan,
      QueuePlanNode queue) {
    ResourceValues queueMaximum = queue.settings().maximumAllocation();
    ResourceValues clusterMaximum =
        plan.getSchedulerSettings().maximumAllocation();
    if (QueueAllocationSettings.firstResourceExceeding(queueMaximum.values(),
        clusterMaximum.values()) != null) {
      throw new IllegalArgumentException(
          "Queue maximum allocation cannot be larger than the cluster setting"
              + " for queue " + queue.queuePath()
              + " max allocation per queue: "
              + resourceText(queueMaximum.values())
              + " cluster setting: "
              + resourceText(clusterMaximum.values()));
    }
  }

  private void validateDefaultLabelExpression(QueuePlanNode queue)
      throws IOException {
    String expression = queue.defaultNodeLabelExpression();
    if (expression == null) {
      return;
    }
    for (String rawLabel : expression.split("&&")) {
      String label = rawLabel.trim();
      if (!label.isEmpty()
          && !queue.accessibleNodeLabels().contains(label)
          && !queue.accessibleNodeLabels().contains(
              RMNodeLabelsManager.ANY)) {
        throw new IOException("Invalid default label expression of  queue="
            + queue.queuePath()
            + " doesn't have permission to access all labels in default "
            + "label expression. labelExpression of resource request="
            + expression + ". Queue labels="
            + String.join(",", queue.accessibleNodeLabels()));
      }
    }
  }

  private void validateTransitions(ValidatedQueuePlan plan,
      Consumer<ValidationIssue> sink) {
    if (plan.getFacts().hierarchyValidationSkipped()) {
      return;
    }
    for (Map.Entry<String, OldQueueSnapshot> entry
        : plan.getFacts().oldHierarchy().entrySet()) {
      String path = entry.getKey();
      OldQueueSnapshot old = entry.getValue();
      if (old.autoCreatedLeaf()) {
        continue;
      }
      QueuePlanNode proposed = plan.getQueue(path);
      QueueState proposedState = plan.getSchedulerSettings()
          .transitionStates().get(path);
      if (proposed == null) {
        if (!old.dynamic() && old.state() != QueueState.STOPPED
            && proposedState != QueueState.STOPPED) {
          sink.accept(issue(path, "queue-deletion-requires-stopped",
              path + " cannot be deleted from the capacity scheduler "
                  + "configuration, as the queue is not yet in stopped state. "
                  + "Current State : " + old.state()));
        }
        continue;
      }
      if (old.kind() == OldQueueKind.PARENT
          && proposed.kind() == QueueKind.MANAGED_PARENT) {
        sink.accept(issue(path, "parent-managedparent-conversion",
            "Can not convert parent queue: " + path
                + " to auto create enabled parent queue since it could have "
                + "other pre-configured queues which is not supported"));
      }
      if (old.kind() == OldQueueKind.MANAGED_PARENT
          && proposed.kind() != QueueKind.MANAGED_PARENT) {
        sink.accept(issue(path, "parent-managedparent-conversion",
            "Cannot convert auto create enabled parent queue: " + path
                + " to leaf queue. Please check parent queue's configuration "
                + CapacitySchedulerConfiguration
                    .AUTO_CREATE_CHILD_QUEUE_ENABLED
                + " is set to true"));
      }
      if (old.kind() == OldQueueKind.LEAF
          && proposed.kind() != QueueKind.LEAF
          && old.state() != QueueState.STOPPED
          && proposedState != QueueState.STOPPED) {
        sink.accept(issue(path, "leaf-parent-conversion-requires-stopped",
            "Can not convert the leaf queue: " + path
                + " to parent queue since it is not yet in stopped state. "
                + "Current State : " + old.state()));
      }
    }
  }

  private void validatePlacementRules(ValidatedQueuePlan plan,
      Consumer<ValidationIssue> sink) {
    PlanDiagnostic parseDiagnostic = plan.getDiagnostics().stream()
        .filter(diagnostic -> diagnostic.stage() == DiagnosticStage.HIERARCHY)
        .findFirst().orElse(null);
    if (parseDiagnostic != null) {
      sink.accept(ValidationDiagnosticSupport.issue(parseDiagnostic));
      return;
    }
    for (PlacementRuleDefinition rule
        : plan.getSchedulerSettings().placementRules()) {
      String target = rule.target();
      if (!target.contains("%") && !queueExists(plan, target)) {
        sink.accept(new ValidationIssue(null,
            CapacitySchedulerConfiguration.QUEUE_MAPPING,
            "placement-rules-parse", ValidationIssue.Severity.ERROR,
            "Path root '" + target + "' does not exist. Path '" + target
                + "' is invalid"));
        return;
      }
    }
  }

  private boolean queueExists(ValidatedQueuePlan plan, String target) {
    boolean configured = plan.getQueues().values().stream().anyMatch(queue ->
        queue.queuePath().equals(target)
            || leafName(queue.queuePath()).equals(target));
    if (configured) {
      return true;
    }
    int separator = target.lastIndexOf('.');
    if (separator > 0) {
      String parentTarget = target.substring(0, separator);
      return plan.getQueues().values().stream().anyMatch(queue ->
          queue.kind() == QueueKind.MANAGED_PARENT
              && (queue.queuePath().equals(parentTarget)
                  || queue.queuePath().equals("root." + parentTarget)
                  || leafName(queue.queuePath()).equals(parentTarget)));
    }
    return false;
  }

  private void diagnostics(ValidatedQueuePlan plan, DiagnosticStage stage,
      Consumer<ValidationIssue> sink) {
    plan.getDiagnostics().stream()
        .filter(diagnostic -> diagnostic.stage() == stage)
        .forEach(diagnostic -> sink.accept(
            ValidationDiagnosticSupport.issue(diagnostic)));
  }

  private QueuePlanNode parent(ValidatedQueuePlan plan, QueuePlanNode node) {
    return node.parentPath() == null ? null : plan.getQueue(node.parentPath());
  }

  private ValidationIssue issue(String path, String rule, String message) {
    return new ValidationIssue(new QueuePath(path), null, rule,
        ValidationIssue.Severity.ERROR, message);
  }

  private String resourceText(Map<String, Long> values) {
    StringBuilder result = new StringBuilder("<memory:")
        .append(values.getOrDefault(ResourceInformation.MEMORY_URI, 0L))
        .append(", vCores:")
        .append(values.getOrDefault(ResourceInformation.VCORES_URI, 0L));
    values.forEach((name, value) -> {
      if (!ResourceInformation.MEMORY_URI.equals(name)
          && !ResourceInformation.VCORES_URI.equals(name)) {
        result.append(", ").append(name).append(":").append(value);
      }
    });
    return result.append(">").toString();
  }

  private boolean allZero(Map<String, Long> values) {
    return values.values().stream().allMatch(value -> value == 0L);
  }

  private String leafName(String path) {
    int separator = path.lastIndexOf('.');
    return separator < 0 ? path : path.substring(separator + 1);
  }

  private boolean hasNoErrors(List<ValidationIssue> issues) {
    return issues.stream().noneMatch(issue ->
        issue.getSeverity() == ValidationIssue.Severity.ERROR);
  }

  private String message(Throwable failure) {
    return failure.getMessage() == null
        ? failure.getClass().getSimpleName() : failure.getMessage();
  }
}
