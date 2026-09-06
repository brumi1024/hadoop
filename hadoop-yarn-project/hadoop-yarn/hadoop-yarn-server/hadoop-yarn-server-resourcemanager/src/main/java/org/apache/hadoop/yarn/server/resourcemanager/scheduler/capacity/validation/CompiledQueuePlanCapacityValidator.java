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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractCSQueue.CapacityConfigType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityCalculationKernel;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityCalculationKernel.BranchInput;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityCalculationKernel.BranchResult;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityCalculationKernel.CapacityKind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityCalculationKernel.ChildInput;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityCalculationKernel.ChildResult;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityCalculationKernel.VectorEntry;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityModeKernel;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityModeKernel.ModeUse;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.LegacyCapacityDerivations;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.CalculatorSemantics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.CapacitySetting;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.CapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.QueueKind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.QueuePlanNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.ResourceValues;

/** Plan-only capacity checks and effective-resource evaluation. */
final class CompiledQueuePlanCapacityValidator {
  private static final float PERCENTAGE_EPSILON = 0.05F;
  private static final Pattern ABSOLUTE_CAPACITY = Pattern.compile(
      CapacitySchedulerConfiguration.PATTERN_FOR_ABSOLUTE_RESOURCE);
  private static final float FRACTION_EPSILON = 0.0005F;

  void validateModeUniformityRules(ValidatedQueuePlan plan,
      Consumer<ValidationIssue> sink) {
    if (!plan.getSchedulerSettings().legacyQueueMode()) {
      return;
    }
    for (QueuePlanNode parent : plan.getQueues().values()) {
      if (parent.childPaths().isEmpty()) {
        continue;
      }
      validateModeUniformity(plan, parent, sink);
    }
  }

  void validatePercentageSumRules(ValidatedQueuePlan plan,
      Consumer<ValidationIssue> sink) {
    if (!plan.getSchedulerSettings().legacyQueueMode()) {
      return;
    }
    for (QueuePlanNode parent : plan.getQueues().values()) {
      if (!parent.childPaths().isEmpty()) {
        validatePercentageSum(plan, parent, sink);
      }
    }
  }

  void validateAbsoluteParentCoverageRules(ValidatedQueuePlan plan,
      Consumer<ValidationIssue> sink) {
    if (!plan.getSchedulerSettings().legacyQueueMode()) {
      return;
    }
    for (QueuePlanNode parent : plan.getQueues().values()) {
      if (!parent.childPaths().isEmpty()) {
        validateAbsoluteParentCoverage(plan, parent, sink);
      }
    }
  }

  void validateConstructorBranch(ValidatedQueuePlan plan,
      QueuePlanNode parent) throws IOException {
    if (!plan.getSchedulerSettings().legacyQueueMode()
        || parent.childPaths().isEmpty()) {
      return;
    }
    List<QueuePlanNode> children = children(plan, parent);
    AbstractParentQueue.QueueCapacityType childMode = classify(children,
        parent.queuePath(), parent.configuredNodeLabels());
    AbstractParentQueue.QueueCapacityType parentMode = classify(
        List.of(parent), parent.queuePath(), parent.configuredNodeLabels());
    if (childMode == AbstractParentQueue.QueueCapacityType.ABSOLUTE_RESOURCE
        || parentMode
            == AbstractParentQueue.QueueCapacityType.ABSOLUTE_RESOURCE) {
      if (childMode != parentMode && !isRoot(parent)) {
        throw new IOException("Parent=" + parent.queuePath()
            + ": When absolute minResource is used, we must make sure both "
            + "parent and child all use absolute minResource");
      }
      validateAbsoluteChildrenFit(plan, parent, children);
    }
    if (childMode == AbstractParentQueue.QueueCapacityType.PERCENT) {
      validateConstructorPercentageSums(parent, parentMode, children);
    }
  }

  void validateManagedParentTemplate(ValidatedQueuePlan plan,
      QueuePlanNode parent) throws IOException {
    if (parent.kind() != QueueKind.MANAGED_PARENT) {
      return;
    }
    ValidatedQueuePlan.TemplateSettings template =
        parent.settings().dynamicQueues().legacyLeafTemplate();
    Set<String> parentLabels = new LinkedHashSet<>();
    if (parent.accessibleNodeLabels().contains("*")) {
      parentLabels.addAll(parent.configuredNodeLabels());
    } else {
      parentLabels.addAll(parent.accessibleNodeLabels());
    }
    parentLabels.add("");
    if (constructorCapacityType(plan, parent) == CapacityConfigType.PERCENTAGE) {
      for (CapacitySetting capacity : template.capacities().values()) {
        Map<String, Long> absolute = absolute(capacity);
        // Template getters, unlike direct queue construction, discard an
        // absolute resource whose memory value is zero.
        if (absolute != null && absolute.getOrDefault(
            ResourceInformation.MEMORY_URI, 0L) != 0L) {
          throw new IOException("Managed Parent Queue " + parent.queuePath()
              + " config type is different from leaf queue template config "
              + "type");
        }
      }
    }
    QueuePath templatePath = QueuePrefixes.getAutoCreatedQueueObjectTemplateConfPrefix(
        new QueuePath(parent.queuePath()));
    for (String label : template.configuredNodeLabels()) {
      CapacitySetting capacity = template.capacities().get(label);
      LegacyCapacityDerivations.validateCapacity(legacyCapacity(parent, capacity, 0F),
          templatePath, label.isEmpty() ? null : label);
      if (!label.isEmpty()) {
        float maximum = legacyCapacity(parent, template.maximumCapacities().get(label), 100F);
        LegacyCapacityDerivations.validateCapacity(maximum == -1F ? 100F : maximum,
            templatePath, label);
      }
      String raw = capacity == null || capacity.rawValue() == null
          ? "" : capacity.rawValue().trim();
      float weight = raw.endsWith("w")
          ? Float.parseFloat(raw.substring(0, raw.length() - 1)) : -1F;
      LegacyCapacityDerivations.validateWeight(weight, templatePath, label);
    }
    for (String label : template.configuredNodeLabels()) {
      if (!parentLabels.contains(label)) {
        throw new IOException("Invalid node label " + label
            + " on configured leaf template on parent queue "
            + parent.queuePath());
      }
    }
  }

  static CapacityConfigType constructorCapacityType(ValidatedQueuePlan plan,
      QueuePlanNode queue) {
    for (String label : queue.configuredNodeLabels()) {
      CapacitySetting capacity = queue.capacity(label);
      Set<ResourceUnitCapacityType> types = new LinkedHashSet<>();
      if (capacity != null) {
        capacity.vector().types().forEach(type ->
            types.add(ResourceUnitCapacityType.valueOf(type.name())));
      }
      return QueueCapacityModeKernel.constructorCapacityType(
          plan.getSchedulerSettings().legacyQueueMode(),
          capacity != null && capacity.rawValue() != null
              && ABSOLUTE_CAPACITY.matcher(capacity.rawValue()).find(), types);
    }
    return CapacityConfigType.NONE;
  }

  void validateEffectiveResources(ValidatedQueuePlan plan,
      Consumer<ValidationIssue> sink) {
    if (plan.getFacts().hierarchyValidationSkipped()
        || allZero(plan.getFacts().clusterResource())) {
      return;
    }
    Map<String, QueueCapacityCalculationKernel.ResourceValues> rootMinimums =
        new LinkedHashMap<>();
    Map<String, QueueCapacityCalculationKernel.ResourceValues> rootMaximums =
        new LinkedHashMap<>();
    for (String label : plan.getRoot().configuredNodeLabels()) {
      ResourceValues partition = plan.getFacts().resourcesByLabel()
          .getOrDefault(label, new ResourceValues(Map.of()));
      QueueCapacityCalculationKernel.ResourceValues values = values(partition);
      rootMinimums.put(label, values);
      rootMaximums.put(label, values);
    }
    try {
      evaluateBranch(plan, plan.getRoot(), rootMinimums, rootMaximums, sink);
    } catch (RuntimeException failure) {
      sink.accept(new ValidationIssue(null, null, "capacity-update-failure",
          ValidationIssue.Severity.ERROR, message(failure)));
    }
  }

  private void validateModeUniformity(ValidatedQueuePlan plan,
      QueuePlanNode parent, Consumer<ValidationIssue> sink) {
    Set<CapacityType> modes = new LinkedHashSet<>();
    for (QueuePlanNode child : children(plan, parent)) {
      CapacitySetting capacity = child.capacity("");
      if (capacity != null) {
        modes.addAll(capacity.vector().types());
      }
    }
    if (modes.size() > 1) {
      sink.accept(issue(parent.queuePath(), "capacity-mode-uniformity",
          "Queue children use mixed capacity configuration modes"));
    }
  }

  private void validatePercentageSum(ValidatedQueuePlan plan,
      QueuePlanNode parent, Consumer<ValidationIssue> sink) {
    CapacitySetting parentCapacity = parent.capacity("");
    if (!isRoot(parent) && parentCapacity != null
        && legacyCapacity(parent, parentCapacity, 0F) == 0F) {
      return;
    }
    float sum = 0F;
    boolean percentage = true;
    boolean configured = false;
    for (QueuePlanNode child : children(plan, parent)) {
      CapacitySetting capacity = child.capacity("");
      if (capacity == null || capacity.vector().types().size() != 1
          || !capacity.vector().types().contains(CapacityType.PERCENTAGE)) {
        percentage = false;
        break;
      }
      configured = true;
      sum += legacyCapacity(child, capacity, 0F);
    }
    if (percentage && configured
        && Math.abs(sum - 100D) > PERCENTAGE_EPSILON) {
      sink.accept(issue(parent.queuePath(), "children-capacity-sum",
          "Illegal capacity sum of " + sum
              + " for children of queue " + parent.queuePath()
              + ". The capacity sum should be 100.0"));
    }
  }

  private void validateAbsoluteParentCoverage(ValidatedQueuePlan plan,
      QueuePlanNode parent, Consumer<ValidationIssue> sink) {
    Map<String, Long> parentMinimum = absolute(parent.capacity(""));
    if (isRoot(parent) || parentMinimum == null) {
      return;
    }
    Map<String, Long> childrenMinimum = zeros(plan);
    boolean invalidChildMinMax = false;
    for (QueuePlanNode child : children(plan, parent)) {
      Map<String, Long> childMinimum = absolute(child.capacity(""));
      if (childMinimum == null) {
        continue;
      }
      add(childrenMinimum, childMinimum);
      Map<String, Long> childMaximum = absolute(child.maximumCapacity(""));
      if (childMaximum != null
          && anyResourceGreater(childMinimum, childMaximum)) {
        invalidChildMinMax = true;
      }
    }
    if (!invalidChildMinMax
        && anyResourceGreater(childrenMinimum, parentMinimum)) {
      sink.accept(issue(parent.queuePath(), "absolute-parent-min-coverage",
          "Parent Queues capacity: " + resourceText(parentMinimum)
              + " is less than to its children:"
              + resourceText(childrenMinimum) + " for queue:"
              + leafName(parent.queuePath())));
    }
  }

  private AbstractParentQueue.QueueCapacityType classify(
      List<QueuePlanNode> queues, String parentPath,
      Set<String> parentLabels) throws IOException {
    List<ModeUse> uses = new ArrayList<>();
    for (QueuePlanNode queue : queues) {
      for (String label : parentLabels) {
        CapacitySetting setting = queue.capacity(label);
        Set<CapacityType> types = setting == null
            ? Set.of() : setting.vector().types();
        uses.add(new ModeUse(queue.queuePath(), label,
            types.contains(CapacityType.PERCENTAGE),
            types.contains(CapacityType.WEIGHT),
            types.contains(CapacityType.ABSOLUTE)));
      }
    }
    return QueueCapacityModeKernel.classify(parentPath, uses,
        !queues.isEmpty() && isRoot(queues.get(0)), queues.isEmpty());
  }

  private void validateAbsoluteChildrenFit(ValidatedQueuePlan plan,
      QueuePlanNode parent, List<QueuePlanNode> children) throws IOException {
    for (String label : parent.configuredNodeLabels()) {
      Map<String, Long> total = zeros(plan);
      for (QueuePlanNode child : children) {
        Map<String, Long> childMinimum = absolute(child.capacity(label));
        if (childMinimum != null) {
          add(total, childMinimum);
        }
      }
      Map<String, Long> parentMinimum = absolute(parent.capacity(label));
      if (parentMinimum == null || allZero(parentMinimum)) {
        continue;
      }
      ResourceValues cluster = plan.getFacts().resourcesByLabel()
          .getOrDefault(label, plan.getFacts().clusterResource());
      if (greater(total, parentMinimum, cluster,
          plan.getFacts().calculatorSemantics())) {
        throw new IOException("Parent Queues capacity: "
            + resourceText(parentMinimum) + " is less than to its children:"
            + resourceText(total) + " for queue:"
            + leafName(parent.queuePath()));
      }
    }
  }

  private void validateConstructorPercentageSums(QueuePlanNode parent,
      AbstractParentQueue.QueueCapacityType parentMode,
      List<QueuePlanNode> children) throws IOException {
    for (String label : parent.configuredNodeLabels()) {
      float sum = 0F;
      for (QueuePlanNode child : children) {
        sum += legacyCapacity(child, child.capacity(label), 0F) / 100F;
      }
      float parentPercentage = legacyCapacity(parent, parent.capacity(label),
          0F) / 100F;
      if (Math.abs(1F - sum) > FRACTION_EPSILON) {
        if (Math.abs(sum) > FRACTION_EPSILON) {
          throw new IOException("Illegal capacity sum of " + sum
              + " for children of queue " + leafName(parent.queuePath())
              + " for label=" + label + ". It should be either 0 or 1.0");
        }
        if (parentMode == AbstractParentQueue.QueueCapacityType.PERCENT
            && Math.abs(parentPercentage) > FRACTION_EPSILON
            && !parent.settings().scheduling().allowZeroCapacitySum()) {
          throw new IOException("Illegal capacity sum of " + ((float) sum)
              + " for children of queue " + leafName(parent.queuePath())
              + " for label=" + label
              + ". It is set to 0, but parent percent != 0, and doesn't "
              + "allow children capacity to set to 0");
        }
      } else if (parentMode
          == AbstractParentQueue.QueueCapacityType.PERCENT
          && Math.abs(parentPercentage) <= 0F
          && !parent.settings().scheduling().allowZeroCapacitySum()) {
        throw new IOException("Illegal capacity sum of " + ((float) sum)
            + " for children of queue " + leafName(parent.queuePath())
            + " for label=" + label + ". queue="
            + leafName(parent.queuePath())
            + " has zero capacity, but childqueues have positive capacities");
      }
    }
  }

  private void evaluateBranch(ValidatedQueuePlan plan, QueuePlanNode parent,
      Map<String, QueueCapacityCalculationKernel.ResourceValues> minimums,
      Map<String, QueueCapacityCalculationKernel.ResourceValues> maximums,
      Consumer<ValidationIssue> sink) {
    if (parent.childPaths().isEmpty()) {
      return;
    }
    List<ChildInput> inputs = new ArrayList<>();
    for (QueuePlanNode child : children(plan, parent)) {
      Map<String, Map<String, VectorEntry>> childMinimums =
          new LinkedHashMap<>();
      Map<String, Map<String, VectorEntry>> childMaximums =
          new LinkedHashMap<>();
      for (String label : child.configuredNodeLabels()) {
        childMinimums.put(label, vector(child.capacity(label)));
        childMaximums.put(label, vector(child.maximumCapacity(label)));
      }
      inputs.add(new ChildInput(child.queuePath(),
          child.configuredNodeLabels(), childMinimums, childMaximums));
    }
    Map<String, QueueCapacityCalculationKernel.ResourceValues> cluster =
        new LinkedHashMap<>();
    for (String label : parent.configuredNodeLabels()) {
      cluster.put(label, values(plan.getFacts().resourcesByLabel()
          .getOrDefault(label, new ResourceValues(Map.of()))));
    }
    Map<String, String> configuredUnits = new LinkedHashMap<>();
    for (String resource : plan.getFacts().resourceNames()) {
      configuredUnits.put(resource,
          ResourceInformation.MEMORY_URI.equals(resource) ? "Mi" : "");
    }
    BranchResult result = QueueCapacityCalculationKernel.calculate(
        new BranchInput(parent.queuePath(),
            parent.kind() == QueueKind.MANAGED_PARENT,
            new ArrayList<>(parent.configuredNodeLabels()),
            plan.getFacts().resourceNames(), configuredResources(parent),
            cluster, minimums, maximums, plan.getFacts().resourceUnits(),
            configuredUnits, inputs));
    result.warnings().forEach(warning -> sink.accept(new ValidationIssue(
        new QueuePath(warning.queuePath()), null,
        "capacity-update-" + warning.kind().name().toLowerCase(Locale.ROOT)
            .replace('_', '-'), ValidationIssue.Severity.WARNING,
        warningMessage(warning))));
    for (QueuePlanNode child : children(plan, parent)) {
      ChildResult childResult = result.children().get(child.queuePath());
      evaluateBranch(plan, child, childResult.effectiveMinimums(),
          childResult.effectiveMaximums(), sink);
    }
  }

  private String warningMessage(
      QueueCapacityCalculationKernel.Warning warning) {
    String template = switch (warning.kind()) {
    case BRANCH_UNDERUTILIZED ->
        "Remaining resource found in branch under parent queue '%s'. %s";
    case QUEUE_OVERUTILIZED ->
        "Queue '%s' is configured to use more resources than what is "
            + "available under its parent. %s";
    case QUEUE_ZERO_RESOURCE -> "Queue '%s' is assigned zero resource. %s";
    case BRANCH_DOWNSCALED ->
        "Child queues with absolute configured capacity under parent queue "
            + "'%s' are downscaled due to insufficient cluster resource. %s";
    case QUEUE_EXCEEDS_MAX_RESOURCE ->
        "Queue '%s' exceeds its maximum available resources. %s";
    case QUEUE_MAX_RESOURCE_EXCEEDS_PARENT ->
        "Maximum resources of queue '%s' are greater than its parent's. %s";
    };
    return String.format(Locale.ROOT, template, warning.queuePath(),
        warning.info());
  }

  private Map<String, VectorEntry> vector(CapacitySetting setting) {
    if (setting == null) {
      return Map.of();
    }
    Map<String, VectorEntry> result = new LinkedHashMap<>();
    setting.vector().entries().forEach((resource, entry) -> result.put(
        resource, new VectorEntry(entry.value(), kind(entry.type()))));
    return result;
  }

  private Map<String, List<String>> configuredResources(
      QueuePlanNode parent) {
    Map<String, List<String>> result = new LinkedHashMap<>();
    for (String label : parent.configuredNodeLabels()) {
      CapacitySetting setting = parent.capacity(label);
      result.put(label, setting == null ? List.of()
          : new ArrayList<>(setting.vector().entries().keySet()));
    }
    return result;
  }

  private CapacityKind kind(CapacityType type) {
    return switch (type) {
    case ABSOLUTE -> CapacityKind.ABSOLUTE;
    case PERCENTAGE -> CapacityKind.PERCENTAGE;
    case WEIGHT -> CapacityKind.WEIGHT;
    };
  }

  private QueueCapacityCalculationKernel.ResourceValues values(
      ResourceValues source) {
    return new QueueCapacityCalculationKernel.ResourceValues(source.values());
  }

  private List<QueuePlanNode> children(ValidatedQueuePlan plan,
      QueuePlanNode parent) {
    List<QueuePlanNode> result = new ArrayList<>();
    parent.childPaths().forEach(path -> result.add(plan.getQueue(path)));
    return result;
  }

  Map<String, Long> absolute(CapacitySetting setting) {
    if (setting == null
        || !setting.vector().types().equals(Set.of(CapacityType.ABSOLUTE))) {
      return null;
    }
    Map<String, Long> result = new LinkedHashMap<>();
    setting.vector().entries().forEach((resource, entry) -> {
      long value = (long) entry.value();
      result.put(resource, ResourceInformation.VCORES_URI.equals(resource)
          ? (long) (int) value : value);
    });
    return result;
  }

  private float legacyCapacity(QueuePlanNode node, CapacitySetting setting,
      float missingValue) {
    if (isRoot(node)) {
      return 100F;
    }
    if (setting == null || setting.rawValue() == null) {
      return missingValue;
    }
    String raw = setting.rawValue().trim();
    if (raw.startsWith("[") || raw.endsWith("w")
        || !setting.vector().types().equals(Set.of(CapacityType.PERCENTAGE))) {
      return missingValue;
    }
    return Float.parseFloat(raw.replace("%", ""));
  }

  boolean greater(Map<String, Long> left, Map<String, Long> right,
      ResourceValues cluster, CalculatorSemantics calculator) {
    if (calculator == CalculatorSemantics.DEFAULT) {
      return left.getOrDefault(ResourceInformation.MEMORY_URI, 0L)
          > right.getOrDefault(ResourceInformation.MEMORY_URI, 0L);
    }
    if (cluster.values().values().stream().noneMatch(value -> value > 0L)) {
      boolean leftGreater = false;
      boolean rightGreater = false;
      for (String resource : cluster.values().keySet()) {
        int comparison = Long.compare(left.getOrDefault(resource, 0L),
            right.getOrDefault(resource, 0L));
        leftGreater |= comparison > 0;
        rightGreater |= comparison < 0;
      }
      return leftGreater && !rightGreater;
    }
    double[] leftShares = shares(left, cluster);
    double[] rightShares = shares(right, cluster);
    Arrays.sort(leftShares);
    Arrays.sort(rightShares);
    for (int index = leftShares.length - 1; index >= 0; index--) {
      if (leftShares[index] == Float.POSITIVE_INFINITY
          || rightShares[index] == Float.POSITIVE_INFINITY) {
        continue;
      }
      double difference = leftShares[index] - rightShares[index];
      if (difference != 0D) {
        return difference > 0D;
      }
    }
    return false;
  }

  private double[] shares(Map<String, Long> resources,
      ResourceValues cluster) {
    List<String> names = new ArrayList<>(cluster.values().keySet());
    Collections.sort(names);
    double[] result = new double[names.size()];
    for (int index = 0; index < names.size(); index++) {
      String name = names.get(index);
      long divisor = cluster.value(name);
      long value = resources.getOrDefault(name, 0L);
      result[index] = divisor == 0L ? Float.POSITIVE_INFINITY
          : (double) value / divisor;
    }
    return result;
  }

  private Map<String, Long> zeros(ValidatedQueuePlan plan) {
    Map<String, Long> values = new LinkedHashMap<>();
    plan.getFacts().resourceNames().forEach(resource -> values.put(resource, 0L));
    return values;
  }

  private void add(Map<String, Long> target, Map<String, Long> values) {
    values.forEach((resource, value) -> target.merge(resource, value,
        (left, right) -> left + right));
  }

  private boolean anyResourceGreater(Map<String, Long> left,
      Map<String, Long> right) {
    return left.entrySet().stream().anyMatch(entry -> entry.getValue()
        > right.getOrDefault(entry.getKey(), 0L));
  }

  private boolean allZero(ResourceValues values) {
    return values.values().values().stream().allMatch(value -> value == 0L);
  }

  private boolean allZero(Map<String, Long> values) {
    return values.values().stream().allMatch(value -> value == 0L);
  }

  private String resourceText(Map<String, Long> values) {
    StringBuilder result = new StringBuilder("<memory:")
        .append(values.getOrDefault(ResourceInformation.MEMORY_URI, 0L))
        .append(", vCores:")
        .append(values.getOrDefault(ResourceInformation.VCORES_URI, 0L));
    values.entrySet().stream().filter(entry ->
        !ResourceInformation.MEMORY_URI.equals(entry.getKey())
            && !ResourceInformation.VCORES_URI.equals(entry.getKey()))
        .forEach(entry -> result.append(", ").append(entry.getKey())
            .append(":").append(entry.getValue()));
    return result.append("> ").toString().trim();
  }

  private ValidationIssue issue(String path, String rule, String message) {
    return new ValidationIssue(new QueuePath(path), null, rule,
        ValidationIssue.Severity.ERROR, message);
  }

  private boolean isRoot(QueuePlanNode node) {
    return "root".equals(node.queuePath());
  }

  private String leafName(String path) {
    int separator = path.lastIndexOf('.');
    return separator < 0 ? path : path.substring(separator + 1);
  }

  private String message(Throwable failure) {
    return failure.getMessage() == null
        ? failure.getClass().getSimpleName() : failure.getMessage();
  }
}
