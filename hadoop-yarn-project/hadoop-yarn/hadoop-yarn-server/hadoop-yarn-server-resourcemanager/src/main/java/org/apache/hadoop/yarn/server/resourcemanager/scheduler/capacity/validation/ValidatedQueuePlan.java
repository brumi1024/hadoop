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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.QueueState;

/**
 * Immutable queue topology and normalized validation inputs for one candidate.
 *
 * <p>The plan owns values only. It deliberately contains no scheduler
 * configuration object, queue, context, resource-calculator instance, metric,
 * application, lock, manager, authorizer, policy instance, mutable store, or
 * static-registry view.</p>
 */
public final class ValidatedQueuePlan {

  /** Queue implementation kind selected by the candidate topology. */
  public enum QueueKind {
    LEAF,
    PARENT,
    MANAGED_PARENT,
    PLAN
  }

  /** Queue kind captured from the existing live hierarchy. */
  public enum OldQueueKind {
    LEAF,
    PARENT,
    MANAGED_PARENT,
    PLAN,
    RESERVATION
  }

  /** Capacity syntax attached to one resource value. */
  public enum CapacityType {
    ABSOLUTE,
    PERCENTAGE,
    WEIGHT
  }

  /** Built-in comparison semantics represented by the pure validator. */
  public enum CalculatorSemantics {
    DEFAULT,
    DOMINANT
  }

  /** Stable identity of the candidate and relevant runtime-facts snapshot. */
  public record PlanIdentity(String candidateFingerprint,
      String factsFingerprint) {
    public PlanIdentity {
      Objects.requireNonNull(candidateFingerprint);
      Objects.requireNonNull(factsFingerprint);
    }
  }

  /** Immutable resource values keyed by normalized resource name. */
  public record ResourceValues(Map<String, Long> values) {
    public ResourceValues {
      values = immutableMap(values);
    }

    public long value(String resourceName) {
      return values.getOrDefault(resourceName, 0L);
    }
  }

  /** One normalized vector entry. */
  public record CapacityEntry(double value, CapacityType type) {
    public CapacityEntry {
      Objects.requireNonNull(type);
    }
  }

  /** Immutable capacity-vector entries keyed by resource name. */
  public record CapacityVector(Map<String, CapacityEntry> entries) {
    public CapacityVector {
      entries = immutableMap(entries);
    }

    public CapacityEntry entry(String resourceName) {
      return entries.get(resourceName);
    }

    public Set<CapacityType> types() {
      Set<CapacityType> types = new LinkedHashSet<>();
      entries.values().forEach(entry -> types.add(entry.type()));
      return Collections.unmodifiableSet(types);
    }
  }

  /** Raw diagnostic source plus its normalized capacity vector. */
  public record CapacitySetting(String rawValue, CapacityVector vector) {
    public CapacitySetting {
      Objects.requireNonNull(vector);
    }
  }

  /** Descriptor for a represented built-in policy. */
  public record PolicyDescriptor(String configuredName, String canonicalName,
      Map<String, String> parameters) {
    public PolicyDescriptor {
      Objects.requireNonNull(configuredName);
      Objects.requireNonNull(canonicalName);
      parameters = immutableMap(parameters);
    }
  }

  /** Application admission and lifetime settings for one queue. */
  @SuppressWarnings("checkstyle:ParameterNumber")
  public record ApplicationSettings(int maximumApplications,
      float maximumApplicationMasterShare, float userLimit,
      float userLimitFactor, long maximumLifetime, long defaultLifetime,
      boolean defaultLifetimeConfigured, int maximumParallelApplications,
      int defaultApplicationPriority,
      Map<String, Float> maximumApplicationMasterShares) {
    public ApplicationSettings {
      maximumApplicationMasterShares = immutableMap(
          maximumApplicationMasterShares);
    }
  }

  /** Preemption and queue-priority settings inherited by one queue. */
  public record SchedulingSettings(boolean preemptionDisabled,
      boolean intraQueuePreemptionDisabled, int priority,
      boolean allowZeroCapacitySum) {
  }

  /** Dynamic queue descriptors and normalized template inputs. */
  @SuppressWarnings("checkstyle:ParameterNumber")
  public record TemplateSettings(
      Map<String, CapacitySetting> capacities,
      Map<String, CapacitySetting> maximumCapacities,
      Integer maximumApplications, PolicyDescriptor orderingPolicy) {
    public TemplateSettings {
      capacities = immutableMap(capacities);
      maximumCapacities = immutableMap(maximumCapacities);
    }

    public Set<String> configuredNodeLabels() {
      Set<String> labels = new LinkedHashSet<>(capacities.keySet());
      labels.addAll(maximumCapacities.keySet());
      return Collections.unmodifiableSet(labels);
    }
  }

  /** Dynamic queue descriptors and normalized template inputs. */
  @SuppressWarnings("checkstyle:ParameterNumber")
  public record DynamicQueueSettings(boolean legacyAutoCreationEnabled,
      boolean flexibleAutoCreationEnabled,
      boolean failCreationWhenCapacityExceeded, int maximumLegacyChildQueues,
      int maximumFlexibleChildQueues, int maximumQueueDepth,
      PolicyDescriptor managementPolicy,
      TemplateSettings legacyLeafTemplate,
      TemplateSettings commonTemplate,
      TemplateSettings leafTemplate,
      TemplateSettings parentTemplate) {
    public DynamicQueueSettings {
      Objects.requireNonNull(managementPolicy);
      Objects.requireNonNull(legacyLeafTemplate);
      Objects.requireNonNull(commonTemplate);
      Objects.requireNonNull(leafTemplate);
      Objects.requireNonNull(parentTemplate);
    }
  }

  /** Reservation inputs. Eligible plans currently carry a disabled value. */
  public record ReservationSettings(boolean reservable,
      Map<String, String> properties) {
    public ReservationSettings {
      properties = immutableMap(properties);
    }
  }

  /** All normalized non-capacity settings needed by pure queue checks. */
  @SuppressWarnings("checkstyle:ParameterNumber")
  public record QueueSettings(ResourceValues maximumAllocation,
      Map<String, Float> userWeights, ApplicationSettings applications,
      SchedulingSettings scheduling, PolicyDescriptor applicationOrdering,
      PolicyDescriptor parentOrdering, String multiNodePolicy,
      Map<String, String> aclProperties,
      DynamicQueueSettings dynamicQueues,
      ReservationSettings reservations) {
    public QueueSettings {
      Objects.requireNonNull(maximumAllocation);
      userWeights = immutableMap(userWeights);
      Objects.requireNonNull(applications);
      Objects.requireNonNull(scheduling);
      Objects.requireNonNull(applicationOrdering);
      Objects.requireNonNull(parentOrdering);
      aclProperties = immutableMap(aclProperties);
      Objects.requireNonNull(dynamicQueues);
      Objects.requireNonNull(reservations);
    }
  }

  /** One configured queue in materialization order. */
  @SuppressWarnings("checkstyle:ParameterNumber")
  public record QueuePlanNode(String queuePath, String parentPath,
      List<String> childPaths, QueueKind kind,
      Map<String, CapacitySetting> capacities,
      Map<String, CapacitySetting> maximumCapacities,
      Set<String> configuredNodeLabels, Set<String> accessibleNodeLabels,
      boolean accessibleNodeLabelsConfigured, QueueState configuredState,
      QueueState initialState, String defaultNodeLabelExpression,
      QueueSettings settings) {
    public QueuePlanNode {
      Objects.requireNonNull(queuePath);
      childPaths = List.copyOf(childPaths);
      Objects.requireNonNull(kind);
      capacities = immutableMap(capacities);
      maximumCapacities = immutableMap(maximumCapacities);
      configuredNodeLabels = immutableSet(configuredNodeLabels);
      accessibleNodeLabels = immutableSet(accessibleNodeLabels);
      Objects.requireNonNull(initialState);
      Objects.requireNonNull(settings);
    }

    public CapacitySetting capacity(String label) {
      return capacities.get(label);
    }

    public CapacitySetting maximumCapacity(String label) {
      return maximumCapacities.get(label);
    }
  }

  /** Existing queue facts required by transition and compatibility checks. */
  public record OldQueueSnapshot(OldQueueKind kind, QueueState state,
      boolean dynamic, boolean autoCreatedLeaf) {
    public OldQueueSnapshot {
      Objects.requireNonNull(kind);
      Objects.requireNonNull(state);
    }
  }

  /** Normalized point-in-time facts used by every plan rule. */
  @SuppressWarnings("checkstyle:ParameterNumber")
  public record FactsSnapshot(ResourceValues clusterResource,
      ResourceValues minimumAllocation, ResourceValues maximumAllocation,
      Set<String> nodeLabels, Map<String, ResourceValues> resourcesByLabel,
      Map<String, OldQueueSnapshot> oldHierarchy,
      boolean hierarchyValidationSkipped,
      CalculatorSemantics calculatorSemantics,
      List<String> resourceNames, Map<String, String> resourceUnits) {
    public FactsSnapshot {
      Objects.requireNonNull(clusterResource);
      Objects.requireNonNull(minimumAllocation);
      Objects.requireNonNull(maximumAllocation);
      nodeLabels = immutableSet(nodeLabels);
      resourcesByLabel = immutableMap(resourcesByLabel);
      oldHierarchy = immutableMap(oldHierarchy);
      Objects.requireNonNull(calculatorSemantics);
      resourceNames = List.copyOf(resourceNames);
      resourceUnits = immutableMap(resourceUnits);
    }
  }

  /** One normalized legacy placement rule. */
  public record PlacementRuleDefinition(String rawRule, String type,
      String source, String target) {
    public PlacementRuleDefinition {
      Objects.requireNonNull(rawRule);
      Objects.requireNonNull(type);
      Objects.requireNonNull(source);
      Objects.requireNonNull(target);
    }
  }

  /** Raw queue-list components retained for queue-name diagnostics. */
  public record QueueListDefinition(String propertyKey,
      List<String> components) {
    public QueueListDefinition {
      Objects.requireNonNull(propertyKey);
      components = List.copyOf(components);
    }
  }

  /** Candidate-wide settings used by plan-only validation. */
  public record SchedulerSettings(boolean legacyQueueMode,
      ResourceValues minimumAllocation, ResourceValues maximumAllocation,
      ResourceValues ruleMinimumAllocation,
      ResourceValues ruleMaximumAllocation,
      String mappingRuleFormat, List<String> rawPlacementRules,
      List<PlacementRuleDefinition> placementRules,
      int maximumAutoCreatedQueueDepth,
      List<QueueListDefinition> queueLists,
      Map<String, QueueState> transitionStates,
      int clusterMaximumApplicationPriority) {
    public SchedulerSettings {
      Objects.requireNonNull(minimumAllocation);
      Objects.requireNonNull(maximumAllocation);
      Objects.requireNonNull(ruleMinimumAllocation);
      Objects.requireNonNull(ruleMaximumAllocation);
      Objects.requireNonNull(mappingRuleFormat);
      rawPlacementRules = List.copyOf(rawPlacementRules);
      placementRules = List.copyOf(placementRules);
      queueLists = List.copyOf(queueLists);
      transitionStates = immutableMap(transitionStates);
    }
  }

  /** Configuration parser diagnostic retained without its mutable model. */
  public enum DiagnosticStage {
    MODEL,
    QUEUE_SETTINGS,
    HIERARCHY
  }

  /** Configuration diagnostic retained at its legacy evaluation stage. */
  public record PlanDiagnostic(DiagnosticStage stage, String queuePath,
      String propertyKey, String code, String message) {
    public PlanDiagnostic {
      Objects.requireNonNull(stage);
      Objects.requireNonNull(code);
      Objects.requireNonNull(message);
    }
  }

  private final PlanIdentity identity;
  private final QueuePlanNode root;
  private final Map<String, QueuePlanNode> queues;
  private final FactsSnapshot facts;
  private final SchedulerSettings schedulerSettings;
  private final List<PlanDiagnostic> diagnostics;

  ValidatedQueuePlan(PlanIdentity identity, QueuePlanNode root,
      Map<String, QueuePlanNode> queues, FactsSnapshot facts,
      SchedulerSettings schedulerSettings,
      List<PlanDiagnostic> diagnostics) {
    this.identity = Objects.requireNonNull(identity);
    this.root = Objects.requireNonNull(root);
    this.queues = immutableMap(queues);
    this.facts = Objects.requireNonNull(facts);
    this.schedulerSettings = Objects.requireNonNull(schedulerSettings);
    this.diagnostics = List.copyOf(diagnostics);
  }

  public PlanIdentity getIdentity() {
    return identity;
  }

  public QueuePlanNode getRoot() {
    return root;
  }

  public QueuePlanNode getQueue(String path) {
    return queues.get(path);
  }

  public Map<String, QueuePlanNode> getQueues() {
    return queues;
  }

  public FactsSnapshot getFacts() {
    return facts;
  }

  public SchedulerSettings getSchedulerSettings() {
    return schedulerSettings;
  }

  public List<PlanDiagnostic> getDiagnostics() {
    return diagnostics;
  }

  private static <K, V> Map<K, V> immutableMap(Map<K, V> source) {
    return Collections.unmodifiableMap(new LinkedHashMap<>(source));
  }

  private static <T> Set<T> immutableSet(Set<T> source) {
    return Collections.unmodifiableSet(new LinkedHashSet<>(source));
  }
}
