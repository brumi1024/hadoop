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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityVectorEntry;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueUpdateWarning;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueUpdateWarning.QueueUpdateWarningType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.plan.ValidatedQueuePlan;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.plan.ValidatedQueuePlan.QueueKind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.plan.ValidatedQueuePlan.QueuePlanNode;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;
import org.apache.hadoop.yarn.util.resource.Resources;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.MEMORY_URI;
import static org.apache.hadoop.yarn.api.records.ResourceInformation.VCORES_URI;

/**
 * Side-effect-free capacity-vector evaluator over an immutable queue plan.
 *
 * <p>The evaluator owns only disposable calculation state.
 * It does not construct queues or access metrics, managers, stores, plugins,
 * authorizers, applications, locks, or static registries.</p>
 */
public final class PlanCapacityEvaluator {
  private static final ResourceUnitCapacityType[] CAPACITY_PRECEDENCE = {
      ResourceUnitCapacityType.ABSOLUTE,
      ResourceUnitCapacityType.PERCENTAGE,
      ResourceUnitCapacityType.WEIGHT
  };
  private static final String MEMORY_UNIT = "Mi";

  /**
   * Evaluates the plan with one immutable runtime-facts snapshot.
   *
   * @param plan compiled queue plan
   * @param facts runtime facts used by validation
   * @return capacity-update warnings in evaluation order
   */
  public List<QueueUpdateWarning> evaluate(ValidatedQueuePlan plan,
      ClusterFacts facts) {
    if (facts.isHierarchyValidationSkipped()
        || Resources.isNone(facts.getClusterResource())) {
      return Collections.emptyList();
    }
    return new Evaluation(plan, facts).run();
  }

  private static final class Evaluation {
    private final ClusterFacts facts;
    private final List<String> resourceNames;
    private final List<QueueUpdateWarning> warnings = new ArrayList<>();
    private final ScratchQueue root;

    private Evaluation(ValidatedQueuePlan plan, ClusterFacts facts) {
      this.facts = facts;
      this.resourceNames = resourceNames(plan, facts);
      this.root = scratchTree(plan, plan.getRoot());
    }

    private List<QueueUpdateWarning> run() {
      initializeRoot(root);
      evaluateBranch(root);
      return List.copyOf(warnings);
    }

    private void initializeRoot(ScratchQueue queue) {
      for (String label : queue.plan.getConfiguredNodeLabels()) {
        Resource labelResource = resourceForLabel(label);
        for (String resourceName : resourceNames) {
          long value = resourceValue(labelResource, resourceName);
          queue.effectiveMinimum(label).set(resourceName, value);
          queue.effectiveMaximum(label).set(resourceName, value);
        }
      }
    }

    private void evaluateBranch(ScratchQueue parent) {
      if (parent.children.isEmpty()) {
        return;
      }

      BranchCalculation calculation = new BranchCalculation(parent);
      calculation.calculate();
      for (ScratchQueue child : parent.children) {
        evaluateBranch(child);
      }
    }

    private Resource resourceForLabel(String label) {
      Resource configured = facts.getResourcesByLabel().get(label);
      if (configured != null) {
        return configured;
      }
      if (RMNodeLabelsManager.NO_LABEL.equals(label)) {
        return facts.getClusterResource();
      }
      return Resources.none();
    }

    private final class BranchCalculation {
      private final ScratchQueue parent;
      private final Map<String, DecimalResources> overallRemaining =
          new LinkedHashMap<>();
      private final Map<String, DecimalResources> batchRemaining =
          new LinkedHashMap<>();
      private final Map<String, Map<String, Double>> weightSums =
          new LinkedHashMap<>();
      private final Map<String, DecimalResources> normalizedRatios =
          new LinkedHashMap<>();

      private BranchCalculation(ScratchQueue parent) {
        this.parent = parent;
      }

      private void calculate() {
        initializeRemainingResources();
        calculateWeightSums();
        calculateNormalizedAbsoluteRatios();

        for (String resourceName : resourceNames) {
          for (ResourceUnitCapacityType capacityType : CAPACITY_PRECEDENCE) {
            Map<String, Double> usedByLabel = new LinkedHashMap<>();
            for (ScratchQueue child : parent.children) {
              calculateChildResource(child, resourceName, capacityType,
                  usedByLabel);
            }
            usedByLabel.forEach((label, used) ->
                batchRemaining.get(label).decrement(resourceName, used));
          }
        }

        for (String label : parent.plan.getConfiguredNodeLabels()) {
          if (!batchRemaining.get(label).isZero()) {
            warn(QueueUpdateWarningType.BRANCH_UNDERUTILIZED, parent,
                "Label: " + label);
          }
        }
      }

      private void initializeRemainingResources() {
        for (String label : parent.plan.getConfiguredNodeLabels()) {
          DecimalResources available = DecimalResources.of(
              parent.effectiveMinimum(label), resourceNames);
          overallRemaining.put(label, available.copy());
          batchRemaining.put(label, available);
        }
      }

      private void calculateWeightSums() {
        for (ScratchQueue child : parent.children) {
          for (String label : child.plan.getConfiguredNodeLabels()) {
            QueueCapacityVector vector = child.minimumVector(label);
            for (String resourceName : resourceNames) {
              QueueCapacityVectorEntry entry = vector.getResource(resourceName);
              if (entry.getVectorResourceType()
                  != ResourceUnitCapacityType.WEIGHT) {
                continue;
              }
              weightSums.computeIfAbsent(label,
                  ignored -> new LinkedHashMap<>())
                  .merge(resourceName, entry.getResourceValue(), Double::sum);
            }
          }
        }
      }

      private void calculateNormalizedAbsoluteRatios() {
        if (parent.plan.getKind() == QueueKind.MANAGED_PARENT) {
          return;
        }
        for (String label : parent.plan.getConfiguredNodeLabels()) {
          QueueCapacityVector parentVector = parent.minimumVector(label);
          for (String resourceName : resourceNames) {
            if (parentVector.getResource(resourceName).getVectorResourceType()
                == null) {
              continue;
            }
            long configuredChildren = configuredAbsoluteChildren(
                label, resourceName);
            if (configuredChildren == 0) {
              continue;
            }
            long effectiveParent = parent.effectiveMinimum(label)
                .get(resourceName);
            double numerator = configuredChildren;
            if (effectiveParent < configuredChildren) {
              numerator = effectiveParent;
              warn(QueueUpdateWarningType.BRANCH_DOWNSCALED, parent, null);
            }
            long converted = convertConfiguredResource(label, resourceName,
                configuredChildren);
            if (converted != 0) {
              normalizedRatios.computeIfAbsent(label,
                  ignored -> new DecimalResources())
                  .set(resourceName, numerator / converted);
            }
          }
        }
      }

      private long configuredAbsoluteChildren(String label,
          String resourceName) {
        long total = 0;
        for (ScratchQueue child : parent.children) {
          if (!child.plan.getConfiguredNodeLabels().contains(label)) {
            continue;
          }
          QueueCapacityVectorEntry entry = child.minimumVector(label)
              .getResource(resourceName);
          if (entry.getVectorResourceType()
              == ResourceUnitCapacityType.ABSOLUTE) {
            total += (long) entry.getResourceValue();
          }
        }
        return total;
      }

      private long convertConfiguredResource(String label,
          String resourceName, long configured) {
        String sourceUnit = MEMORY_URI.equals(resourceName) ? MEMORY_UNIT : "";
        String targetUnit = resourceUnit(resourceForLabel(label), resourceName);
        return UnitsConversionUtil.convert(sourceUnit, targetUnit, configured);
      }

      private void calculateChildResource(ScratchQueue child,
          String resourceName, ResourceUnitCapacityType capacityType,
          Map<String, Double> usedByLabel) {
        for (String label : child.plan.getConfiguredNodeLabels()) {
          QueueCapacityVectorEntry minimumEntry = child.minimumVector(label)
              .getResource(resourceName);
          if (minimumEntry.getVectorResourceType() != capacityType
              || !overallRemaining.containsKey(label)) {
            continue;
          }

          QueueCapacityVectorEntry maximumEntry = child.maximumEntry(label,
              resourceName);
          double minimum = calculateMinimum(child, label, resourceName,
              minimumEntry);
          double maximum = calculateMaximum(child, label, resourceName,
              maximumEntry);
          minimum = round(minimum, minimumEntry);
          maximum = round(maximum, maximumEntry);
          CalculatedResources checked = validate(child, label, resourceName,
              minimum, maximum);
          child.effectiveMinimum(label).set(resourceName,
              (long) checked.minimum);
          child.effectiveMaximum(label).set(resourceName,
              (long) checked.maximum);

          overallRemaining.get(label).decrement(resourceName,
              checked.minimum);
          usedByLabel.merge(label, checked.minimum, Double::sum);
        }
      }

      private double calculateMinimum(ScratchQueue child, String label,
          String resourceName, QueueCapacityVectorEntry entry) {
        switch (entry.getVectorResourceType()) {
        case ABSOLUTE:
          return normalizedRatio(label, resourceName)
              * remainingRatio(label, resourceName)
              * entry.getResourceValue();
        case PERCENTAGE:
          return resourceValue(resourceForLabel(label), resourceName)
              * parentAbsoluteMinimum(label, resourceName)
              * remainingRatio(label, resourceName)
              * entry.getResourceValue() / 100;
        case WEIGHT:
          double normalizedWeight = entry.getResourceValue()
              / weightSums.get(label).get(resourceName);
          double remaining = batchRemaining.get(label).get(resourceName);
          if (normalizedWeight == 1) {
            return remaining;
          }
          return resourceValue(resourceForLabel(label), resourceName)
              * parentAbsoluteMinimum(label, resourceName)
              * remainingRatio(label, resourceName) * normalizedWeight;
        default:
          throw new IllegalStateException("Unsupported capacity type for "
              + child.plan.getQueuePath());
        }
      }

      private double calculateMaximum(ScratchQueue child, String label,
          String resourceName, QueueCapacityVectorEntry entry) {
        switch (entry.getVectorResourceType()) {
        case ABSOLUTE:
          return entry.getResourceValue();
        case PERCENTAGE:
          return resourceValue(resourceForLabel(label), resourceName)
              * parentAbsoluteMaximum(label, resourceName)
              * entry.getResourceValue() / 100;
        case WEIGHT:
          throw new IllegalStateException("Resource " + resourceName
              + " has WEIGHT maximum capacity type, which is not supported");
        default:
          throw new IllegalStateException("Unsupported maximum capacity type "
              + "for " + child.plan.getQueuePath());
        }
      }

      private CalculatedResources validate(ScratchQueue child, String label,
          String resourceName, double minimum, double maximum) {
        long minimumMemory = child.effectiveMinimum(label).get(MEMORY_URI);
        double remaining = overallRemaining.get(label).get(resourceName);
        long parentMaximum = parent.effectiveMaximum(label).get(resourceName);

        if (!MEMORY_URI.equals(resourceName) && minimumMemory == 0) {
          minimum = 0;
        }
        if (maximum != 0 && maximum > parentMaximum) {
          warn(QueueUpdateWarningType.QUEUE_MAX_RESOURCE_EXCEEDS_PARENT,
              child, null);
        }
        maximum = maximum == 0
            ? parentMaximum : Math.min(maximum, parentMaximum);
        if (maximum < minimum) {
          warn(QueueUpdateWarningType.QUEUE_EXCEEDS_MAX_RESOURCE, child, null);
          minimum = maximum;
        }
        if (minimum > remaining) {
          if (parent.plan.getKind() == QueueKind.MANAGED_PARENT) {
            minimum = 0;
          } else {
            warn(QueueUpdateWarningType.QUEUE_OVERUTILIZED, child,
                "Resource name: " + resourceName + " resource value: "
                    + minimum);
            minimum = remaining;
          }
        }
        if (minimum == 0) {
          warn(QueueUpdateWarningType.QUEUE_ZERO_RESOURCE, child,
              "Resource name: " + resourceName);
        }
        return new CalculatedResources(minimum, maximum);
      }

      private double normalizedRatio(String label, String resourceName) {
        DecimalResources ratios = normalizedRatios.get(label);
        return ratios == null || !ratios.contains(resourceName)
            ? 1 : ratios.get(resourceName);
      }

      private double remainingRatio(String label, String resourceName) {
        return batchRemaining.get(label).get(resourceName)
            / parent.effectiveMinimum(label).get(resourceName);
      }

      private double parentAbsoluteMinimum(String label,
          String resourceName) {
        return (double) parent.effectiveMinimum(label).get(resourceName)
            / resourceValue(resourceForLabel(label), resourceName);
      }

      private double parentAbsoluteMaximum(String label,
          String resourceName) {
        return (double) parent.effectiveMaximum(label).get(resourceName)
            / resourceValue(resourceForLabel(label), resourceName);
      }
    }

    private void warn(QueueUpdateWarningType type, ScratchQueue queue,
        String info) {
      QueueUpdateWarning warning = type.ofQueue(
          queue.plan.getQueuePath().getFullPath());
      warnings.add(info == null ? warning : warning.withInfo(info));
    }
  }

  private static final class ScratchQueue {
    private final QueuePlanNode plan;
    private final List<ScratchQueue> children = new ArrayList<>();
    private final Map<String, LongResources> effectiveMinimum =
        new LinkedHashMap<>();
    private final Map<String, LongResources> effectiveMaximum =
        new LinkedHashMap<>();

    private ScratchQueue(QueuePlanNode plan) {
      this.plan = plan;
    }

    private LongResources effectiveMinimum(String label) {
      return effectiveMinimum.computeIfAbsent(label,
          ignored -> new LongResources());
    }

    private LongResources effectiveMaximum(String label) {
      return effectiveMaximum.computeIfAbsent(label,
          ignored -> new LongResources());
    }

    private QueueCapacityVector minimumVector(String label) {
      return plan.getCapacity(label).getVector();
    }

    private QueueCapacityVectorEntry maximumEntry(String label,
        String resourceName) {
      QueueCapacityVector vector = plan.getMaximumCapacity(label).getVector();
      QueueCapacityVectorEntry entry = vector.getResource(resourceName);
      return entry.getVectorResourceType() == null
          ? new QueueCapacityVectorEntry(ResourceUnitCapacityType.ABSOLUTE,
              resourceName, 0)
          : entry;
    }
  }

  private static final class LongResources {
    private final Map<String, Long> values = new LinkedHashMap<>();

    private long get(String resourceName) {
      return values.getOrDefault(resourceName, 0L);
    }

    private void set(String resourceName, long value) {
      values.put(resourceName, value);
    }
  }

  private static final class DecimalResources {
    private final Map<String, Double> values = new LinkedHashMap<>();

    private static DecimalResources of(LongResources source,
        Collection<String> resourceNames) {
      DecimalResources result = new DecimalResources();
      for (String resourceName : resourceNames) {
        result.set(resourceName, source.get(resourceName));
      }
      return result;
    }

    private DecimalResources copy() {
      DecimalResources copy = new DecimalResources();
      copy.values.putAll(values);
      return copy;
    }

    private double get(String resourceName) {
      return values.getOrDefault(resourceName, 0D);
    }

    private void set(String resourceName, double value) {
      values.put(resourceName, value);
    }

    private void decrement(String resourceName, double value) {
      set(resourceName, get(resourceName) - value);
    }

    private boolean contains(String resourceName) {
      return values.containsKey(resourceName);
    }

    private boolean isZero() {
      return values.values().stream().allMatch(value -> value == 0);
    }
  }

  private static final class CalculatedResources {
    private final double minimum;
    private final double maximum;

    private CalculatedResources(double minimum, double maximum) {
      this.minimum = minimum;
      this.maximum = maximum;
    }
  }

  private static ScratchQueue scratchTree(ValidatedQueuePlan plan,
      QueuePlanNode node) {
    ScratchQueue scratch = new ScratchQueue(node);
    for (org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath
        childPath : node.getChildPaths()) {
      scratch.children.add(scratchTree(plan, plan.getQueue(childPath)));
    }
    return scratch;
  }

  private static List<String> resourceNames(ValidatedQueuePlan plan,
      ClusterFacts facts) {
    Set<String> names = new LinkedHashSet<>();
    names.add(MEMORY_URI);
    names.add(VCORES_URI);
    addResourceNames(names, facts.getClusterResource());
    facts.getResourcesByLabel().values().forEach(resource ->
        addResourceNames(names, resource));
    plan.getQueues().values().forEach(queue -> {
      queue.getCapacities().values().forEach(setting ->
          names.addAll(setting.getVector().getResourceNames()));
      queue.getMaximumCapacities().values().forEach(setting ->
          names.addAll(setting.getVector().getResourceNames()));
    });
    return List.copyOf(names);
  }

  private static void addResourceNames(Set<String> names, Resource resource) {
    for (ResourceInformation information : resource.getResources()) {
      names.add(information.getName());
    }
  }

  private static long resourceValue(Resource resource, String resourceName) {
    for (ResourceInformation information : resource.getResources()) {
      if (resourceName.equals(information.getName())) {
        return information.getValue();
      }
    }
    return 0;
  }

  private static String resourceUnit(Resource resource, String resourceName) {
    for (ResourceInformation information : resource.getResources()) {
      if (resourceName.equals(information.getName())) {
        return information.getUnits();
      }
    }
    return "";
  }

  private static double round(double value, QueueCapacityVectorEntry entry) {
    return entry.getVectorResourceType() == ResourceUnitCapacityType.WEIGHT
        ? Math.round(value) : Math.floor(value);
  }
}
