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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;

/**
 * Pure capacity-vector calculation shared by live queues and compiled plans.
 *
 * <p>The input and result own primitive values only. Calculation does not
 * access resource registries, queues, plugins, metrics, locks, or managers.</p>
 */
@InterfaceAudience.Private
public final class QueueCapacityCalculationKernel {
  /** Capacity syntax attached to one resource. */
  public enum CapacityKind {
    ABSOLUTE,
    PERCENTAGE,
    WEIGHT
  }

  /** Stable warning categories produced in calculation order. */
  public enum WarningKind {
    BRANCH_UNDERUTILIZED,
    QUEUE_OVERUTILIZED,
    QUEUE_ZERO_RESOURCE,
    BRANCH_DOWNSCALED,
    QUEUE_EXCEEDS_MAX_RESOURCE,
    QUEUE_MAX_RESOURCE_EXCEEDS_PARENT
  }

  /** One configured vector entry. */
  public record VectorEntry(double value, CapacityKind kind) {
    public VectorEntry {
      Objects.requireNonNull(kind);
    }
  }

  /** Immutable long resource values. */
  public record ResourceValues(Map<String, Long> values) {
    public ResourceValues {
      values = immutableMap(values);
    }

    public long value(String resourceName) {
      return values.getOrDefault(resourceName, 0L);
    }
  }

  /** One child queue's configured minimum and maximum vectors. */
  public record ChildInput(String queuePath, Set<String> configuredLabels,
      Map<String, Map<String, VectorEntry>> minimums,
      Map<String, Map<String, VectorEntry>> maximums) {
    public ChildInput {
      Objects.requireNonNull(queuePath);
      configuredLabels = Collections.unmodifiableSet(
          new LinkedHashSet<>(configuredLabels));
      minimums = immutableNestedMap(minimums);
      maximums = immutableNestedMap(maximums);
    }
  }

  /** One parent branch and all immutable inputs needed for calculation. */
  @SuppressWarnings("checkstyle:ParameterNumber")
  public record BranchInput(String parentPath, boolean managedParent,
      List<String> parentLabels, List<String> resourceNames,
      Map<String, List<String>> parentConfiguredResources,
      Map<String, ResourceValues> clusterResources,
      Map<String, ResourceValues> parentMinimums,
      Map<String, ResourceValues> parentMaximums,
      Map<String, String> clusterUnits,
      Map<String, String> configuredUnits, List<ChildInput> children) {
    public BranchInput {
      Objects.requireNonNull(parentPath);
      parentLabels = List.copyOf(parentLabels);
      resourceNames = List.copyOf(resourceNames);
      parentConfiguredResources = immutableListMap(
          parentConfiguredResources);
      clusterResources = immutableMap(clusterResources);
      parentMinimums = immutableMap(parentMinimums);
      parentMaximums = immutableMap(parentMaximums);
      clusterUnits = immutableMap(clusterUnits);
      configuredUnits = immutableMap(configuredUnits);
      children = List.copyOf(children);
    }
  }

  /** Effective minimum and maximum resources for one child. */
  public record ChildResult(
      Map<String, ResourceValues> effectiveMinimums,
      Map<String, ResourceValues> effectiveMaximums) {
    public ChildResult {
      effectiveMinimums = immutableMap(effectiveMinimums);
      effectiveMaximums = immutableMap(effectiveMaximums);
    }
  }

  /** One ordered non-fatal capacity update warning. */
  public record Warning(WarningKind kind, String queuePath, String info) {
    public Warning {
      Objects.requireNonNull(kind);
      Objects.requireNonNull(queuePath);
      info = info == null ? "" : info;
    }
  }

  /** Pure absolute-capacity normalization decision for one resource. */
  public record AbsoluteNormalization(boolean defined, double ratio,
      boolean downscaled) {
  }

  /** Inputs for the legacy calculated-resource acceptance decision. */
  @SuppressWarnings("checkstyle:ParameterNumber")
  public record CalculatedResourceInput(String resourceName,
      double minimumResource, long effectiveMemoryMinimum,
      double maximumResource, long parentMaximumResource,
      double remainingResourceUnderParent, boolean managedParent) {
    public CalculatedResourceInput {
      Objects.requireNonNull(resourceName);
    }
  }

  /** One ordered warning emitted by a calculated-resource decision. */
  public record DecisionWarning(WarningKind kind, String info) {
    public DecisionWarning {
      Objects.requireNonNull(kind);
      info = info == null ? "" : info;
    }
  }

  /** Validated minimum/maximum resources plus ordered warning decisions. */
  public record CalculatedResourceResult(double minimumResource,
      double maximumResource, List<DecisionWarning> warnings) {
    public CalculatedResourceResult {
      warnings = List.copyOf(warnings);
    }
  }

  /** Immutable branch outputs and calculation prerequisites. */
  public record BranchResult(Map<String, ChildResult> children,
      List<Warning> warnings,
      Map<String, Map<String, Double>> sumWeights,
      Map<String, Map<String, Double>> normalizedAbsoluteRatios) {
    public BranchResult {
      children = immutableMap(children);
      warnings = List.copyOf(warnings);
      sumWeights = immutableNestedMap(sumWeights);
      normalizedAbsoluteRatios = immutableNestedMap(
          normalizedAbsoluteRatios);
    }
  }

  private static final CapacityKind[] PRECEDENCE = {
      CapacityKind.ABSOLUTE, CapacityKind.PERCENTAGE, CapacityKind.WEIGHT};

  private QueueCapacityCalculationKernel() {
  }

  /** Calculates a complete parent branch without side effects. */
  public static BranchResult calculate(BranchInput input) {
    MutableCalculation calculation = new MutableCalculation(input);
    calculation.calculate();
    return calculation.result();
  }

  /** Shared scalar formula used by the legacy percentage adapter. */
  static double percentageMinimum(long cluster, long parentMinimum,
      double batchRemaining, double configuredPercentage) {
    double parentAbsolute = (double) parentMinimum / cluster;
    double remainingRatio = batchRemaining / parentMinimum;
    double absoluteCapacity = parentAbsolute * remainingRatio
        * configuredPercentage / 100D;
    return cluster * absoluteCapacity;
  }

  /** Shared scalar formula used by the legacy percentage adapter. */
  static double percentageMaximum(long cluster, long parentMaximum,
      double configuredPercentage) {
    double parentAbsolute = (double) parentMaximum / cluster;
    double absoluteMaximum = parentAbsolute * configuredPercentage / 100D;
    return cluster * absoluteMaximum;
  }

  /** Shared scalar formula used by the legacy weight adapter. */
  static double weightMinimum(long cluster, long parentMinimum,
      double batchRemaining, double configuredWeight, double totalWeight) {
    double normalizedWeight = configuredWeight / totalWeight;
    if (normalizedWeight == 1D) {
      return batchRemaining;
    }
    double remainingRatio = batchRemaining / parentMinimum;
    double parentAbsolute = (double) parentMinimum / cluster;
    double queueAbsolute = parentAbsolute * remainingRatio * normalizedWeight;
    return cluster * queueAbsolute;
  }

  /** Shared scalar formula used by the legacy absolute adapter. */
  static double absoluteMinimum(long parentMinimum, double batchRemaining,
      double configuredAbsolute, double normalizedRatio) {
    double remainingRatio = batchRemaining / parentMinimum;
    return normalizedRatio * remainingRatio * configuredAbsolute;
  }

  /**
   * Preserves the live absolute-normalization arithmetic without consulting
   * queues, resources, or a mutable resource registry.
   */
  public static AbsoluteNormalization normalizeAbsolute(
      long childrenConfiguredResource, long effectiveMinimumResource,
      String configuredUnit, String clusterUnit) {
    if (childrenConfiguredResource == 0L) {
      return new AbsoluteNormalization(false, 1D, false);
    }
    float numerator = childrenConfiguredResource;
    boolean downscaled = effectiveMinimumResource
        < childrenConfiguredResource;
    if (downscaled) {
      numerator = effectiveMinimumResource;
    }
    long converted = UnitsConversionUtil.convert(configuredUnit, clusterUnit,
        childrenConfiguredResource);
    if (converted == 0L) {
      return new AbsoluteNormalization(false, 1D, downscaled);
    }
    return new AbsoluteNormalization(true, numerator / converted,
        downscaled);
  }

  /** Applies the live rounding rule for a represented capacity kind. */
  public static double round(double value, CapacityKind kind) {
    return round(value, kind == CapacityKind.WEIGHT);
  }

  /** Applies the live rounding rule for a calculator precedence position. */
  public static double round(double value, boolean lastCapacityType) {
    return lastCapacityType ? Math.round(value) : Math.floor(value);
  }

  /**
   * Applies the live calculated-resource clamping and warning decisions in
   * their externally observable order.
   */
  public static CalculatedResourceResult validateCalculatedResources(
      CalculatedResourceInput input) {
    double minimum = input.minimumResource();
    double maximum = input.maximumResource();
    List<DecisionWarning> warnings = new ArrayList<>();
    if (!ResourceInformation.MEMORY_URI.equals(input.resourceName())
        && input.effectiveMemoryMinimum() == 0L) {
      minimum = 0D;
    }
    if (maximum != 0D && maximum > input.parentMaximumResource()) {
      warnings.add(new DecisionWarning(
          WarningKind.QUEUE_MAX_RESOURCE_EXCEEDS_PARENT, ""));
    }
    maximum = maximum == 0D ? input.parentMaximumResource()
        : Math.min(maximum, input.parentMaximumResource());
    if (maximum < minimum) {
      warnings.add(new DecisionWarning(
          WarningKind.QUEUE_EXCEEDS_MAX_RESOURCE, ""));
      minimum = maximum;
    }
    if (minimum > input.remainingResourceUnderParent()) {
      if (input.managedParent()) {
        minimum = 0D;
      } else {
        warnings.add(new DecisionWarning(WarningKind.QUEUE_OVERUTILIZED,
            "Resource name: " + input.resourceName() + " resource value: "
                + minimum));
        minimum = input.remainingResourceUnderParent();
      }
    }
    if (minimum == 0D) {
      warnings.add(new DecisionWarning(WarningKind.QUEUE_ZERO_RESOURCE,
          "Resource name: " + input.resourceName()));
    }
    return new CalculatedResourceResult(minimum, maximum, warnings);
  }

  private static final class MutableCalculation {
    private final BranchInput input;
    private final Map<String, MutableChildResult> children =
        new LinkedHashMap<>();
    private final List<Warning> warnings = new ArrayList<>();
    private final Map<String, Map<String, Double>> weights =
        new LinkedHashMap<>();
    private final Map<String, Map<String, Double>> normalizedRatios =
        new LinkedHashMap<>();
    private final Map<String, Map<String, Double>> overallRemaining =
        new LinkedHashMap<>();
    private final Map<String, Map<String, Double>> batchRemaining =
        new LinkedHashMap<>();

    MutableCalculation(BranchInput input) {
      this.input = input;
      for (ChildInput child : input.children()) {
        children.put(child.queuePath(), new MutableChildResult(child,
            input.resourceNames()));
      }
      for (String label : input.parentLabels()) {
        Map<String, Double> overall = new LinkedHashMap<>();
        Map<String, Double> batch = new LinkedHashMap<>();
        for (String resource : input.resourceNames()) {
          double value = input.parentMinimums().getOrDefault(label,
              new ResourceValues(Map.of())).value(resource);
          overall.put(resource, value);
          batch.put(resource, value);
        }
        overallRemaining.put(label, overall);
        batchRemaining.put(label, batch);
      }
    }

    void calculate() {
      calculatePrerequisites();
      for (String resourceName : input.resourceNames()) {
        for (CapacityKind capacityKind : PRECEDENCE) {
          Map<String, Double> usedByLabel = new LinkedHashMap<>();
          for (ChildInput child : input.children()) {
            calculateChildResource(child, resourceName, capacityKind,
                usedByLabel);
          }
          usedByLabel.forEach((label, used) -> batchRemaining.get(label)
              .compute(resourceName, (ignored, current) -> current - used));
        }
      }
      for (String label : input.parentLabels()) {
        boolean remaining = batchRemaining.get(label).values().stream()
            .anyMatch(value -> value != 0D);
        if (remaining) {
          warnings.add(new Warning(WarningKind.BRANCH_UNDERUTILIZED,
              input.parentPath(), "Label: " + label));
        }
      }
    }

    private void calculatePrerequisites() {
      for (ChildInput child : input.children()) {
        for (String label : child.configuredLabels()) {
          Map<String, VectorEntry> minimum = child.minimums().getOrDefault(
              label, Map.of());
          minimum.forEach((resource, entry) -> {
            if (entry.kind() == CapacityKind.WEIGHT) {
              weights.computeIfAbsent(label, ignored -> new LinkedHashMap<>())
                  .merge(resource, entry.value(), Double::sum);
            }
          });
        }
      }
      if (input.managedParent()) {
        return;
      }
      for (String label : input.parentLabels()) {
        for (String resource : input.parentConfiguredResources()
            .getOrDefault(label, List.of())) {
          long configuredChildren = 0L;
          for (ChildInput child : input.children()) {
            VectorEntry entry = child.minimums().getOrDefault(label, Map.of())
                .get(resource);
            if (entry != null && entry.kind() == CapacityKind.ABSOLUTE) {
              configuredChildren += entry.value();
            }
          }
          if (configuredChildren == 0L) {
            continue;
          }
          long parentMinimum = input.parentMinimums().get(label)
              .value(resource);
          AbsoluteNormalization normalization = normalizeAbsolute(
              configuredChildren, parentMinimum,
              input.configuredUnits().getOrDefault(resource, ""),
              input.clusterUnits().getOrDefault(resource, ""));
          if (normalization.downscaled()) {
            warnings.add(new Warning(WarningKind.BRANCH_DOWNSCALED,
                input.parentPath(), ""));
          }
          if (normalization.defined()) {
            normalizedRatios.computeIfAbsent(label,
                ignored -> new LinkedHashMap<>())
                .put(resource, normalization.ratio());
          }
        }
      }
    }

    private void calculateChildResource(ChildInput child,
        String resourceName, CapacityKind capacityKind,
        Map<String, Double> usedByLabel) {
      for (String label : child.configuredLabels()) {
        VectorEntry minimumEntry = child.minimums()
            .getOrDefault(label, Map.of()).get(resourceName);
        if (minimumEntry == null || minimumEntry.kind() != capacityKind
            || !overallRemaining.containsKey(label)) {
          continue;
        }
        VectorEntry maximumEntry = child.maximums()
            .getOrDefault(label, Map.of()).get(resourceName);
        if (maximumEntry == null) {
          maximumEntry = new VectorEntry(0D, CapacityKind.ABSOLUTE);
        }
        long cluster = input.clusterResources().get(label).value(resourceName);
        long parentMinimum = input.parentMinimums().get(label)
            .value(resourceName);
        long parentMaximum = input.parentMaximums().get(label)
            .value(resourceName);
        double batch = batchRemaining.get(label).get(resourceName);
        double minimum = minimum(minimumEntry, label, resourceName, cluster,
            parentMinimum, batch);
        double maximum = maximum(maximumEntry, resourceName, cluster,
            parentMaximum);
        minimum = round(minimum, minimumEntry.kind());
        maximum = round(maximum, maximumEntry.kind());

        MutableChildResult childResult = children.get(child.queuePath());
        double remaining = overallRemaining.get(label).get(resourceName);
        CalculatedResourceResult decision = validateCalculatedResources(
            new CalculatedResourceInput(resourceName, minimum,
                childResult.minimum(label, ResourceInformation.MEMORY_URI),
                maximum, parentMaximum, remaining, input.managedParent()));
        decision.warnings().forEach(warning -> warnings.add(new Warning(
            warning.kind(), child.queuePath(), warning.info())));
        minimum = decision.minimumResource();
        maximum = decision.maximumResource();
        childResult.set(label, resourceName, (long) minimum, (long) maximum);
        double effectiveMinimum = minimum;
        overallRemaining.get(label).compute(resourceName,
            (ignored, current) -> current - effectiveMinimum);
        usedByLabel.merge(label, minimum, Double::sum);
      }
    }

    private double minimum(VectorEntry entry, String label,
        String resourceName, long cluster, long parentMinimum,
        double batch) {
      return switch (entry.kind()) {
      case ABSOLUTE -> absoluteMinimum(parentMinimum, batch, entry.value(),
          normalizedRatios.getOrDefault(label, Map.of())
              .getOrDefault(resourceName, 1D));
      case PERCENTAGE -> percentageMinimum(cluster, parentMinimum, batch,
          entry.value());
      case WEIGHT -> weightMinimum(cluster, parentMinimum, batch,
          entry.value(), weights.get(label).get(resourceName));
      };
    }

    private double maximum(VectorEntry entry, String resourceName,
        long cluster, long parentMaximum) {
      return switch (entry.kind()) {
      case ABSOLUTE -> entry.value();
      case PERCENTAGE -> percentageMaximum(cluster, parentMaximum,
          entry.value());
      case WEIGHT -> throw new IllegalStateException("Resource "
          + resourceName + " has WEIGHT maximum capacity type, which is not "
          + "supported");
      };
    }

    BranchResult result() {
      Map<String, ChildResult> immutableChildren = new LinkedHashMap<>();
      children.forEach((path, child) ->
          immutableChildren.put(path, child.result()));
      return new BranchResult(immutableChildren, warnings, weights,
          normalizedRatios);
    }
  }

  private static final class MutableChildResult {
    private final Map<String, Map<String, Long>> minimums =
        new LinkedHashMap<>();
    private final Map<String, Map<String, Long>> maximums =
        new LinkedHashMap<>();

    MutableChildResult(ChildInput input, List<String> resources) {
      for (String label : input.configuredLabels()) {
        Map<String, Long> minimum = new LinkedHashMap<>();
        Map<String, Long> maximum = new LinkedHashMap<>();
        resources.forEach(resource -> {
          minimum.put(resource, 0L);
          maximum.put(resource, 0L);
        });
        minimums.put(label, minimum);
        maximums.put(label, maximum);
      }
    }

    long minimum(String label, String resource) {
      return minimums.get(label).getOrDefault(resource, 0L);
    }

    void set(String label, String resource, long minimum, long maximum) {
      minimums.get(label).put(resource, minimum);
      maximums.get(label).put(resource, maximum);
    }

    ChildResult result() {
      Map<String, ResourceValues> immutableMinimums = new LinkedHashMap<>();
      Map<String, ResourceValues> immutableMaximums = new LinkedHashMap<>();
      minimums.forEach((label, values) ->
          immutableMinimums.put(label, new ResourceValues(values)));
      maximums.forEach((label, values) ->
          immutableMaximums.put(label, new ResourceValues(values)));
      return new ChildResult(immutableMinimums, immutableMaximums);
    }
  }

  private static <K, V> Map<K, V> immutableMap(Map<K, V> source) {
    return Collections.unmodifiableMap(new LinkedHashMap<>(source));
  }

  private static <K1, K2, V> Map<K1, Map<K2, V>> immutableNestedMap(
      Map<K1, ? extends Map<K2, V>> source) {
    Map<K1, Map<K2, V>> result = new LinkedHashMap<>();
    source.forEach((key, value) -> result.put(key, immutableMap(value)));
    return Collections.unmodifiableMap(result);
  }

  private static <K, V> Map<K, List<V>> immutableListMap(
      Map<K, ? extends List<V>> source) {
    Map<K, List<V>> result = new LinkedHashMap<>();
    source.forEach((key, value) -> result.put(key, List.copyOf(value)));
    return Collections.unmodifiableMap(result);
  }
}
