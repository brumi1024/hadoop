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
import java.util.Collection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.placement.CSMappingPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractAutoCreatedLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractCSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueueStore;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ManagedParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.PlanQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueStateHelper;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ReservationQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.CSConfigModel;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.QueueConfigNode;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

/** Point-in-time runtime facts used without mutating the live scheduler. */
public final class ClusterFacts {
  private static final String DEFAULT_AUTHORIZER =
      "org.apache.hadoop.yarn.security.ConfiguredYarnAuthorizer";
  private static final String DEFAULT_ORDERING =
      "org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FifoOrderingPolicy";
  private static final String DEFAULT_GROUP_MAPPING =
      "org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback";
  private static final String DEFAULT_RECORD_FACTORY =
      "org.apache.hadoop.yarn.factories.impl.pb.RecordFactoryPBImpl";
  /** Queue shape relevant to refresh transition checks. */
  public enum QueueKind {
    LEAF, PARENT, MANAGED_PARENT, PLAN, RESERVATION
  }

  /** Installed placement dependencies read without executing a rule. */
  public record PlacementPolicyDescriptor(String implementationClass,
      String groupsClass, String groupProviderClass) {
  }

  public static final class OldQueue {
    private final QueueKind kind;
    private final QueueState state;
    private final boolean dynamic;
    private final boolean autoCreatedLeaf;
    private final Map<String, Long> maximumAllocation;
    private final String orderingPolicyClass;
    private final String constructorCapacityType;
    private final boolean allowZeroCapacitySum;
    private final Set<String> configuredNodeLabels;

    OldQueue(QueueKind kind, QueueState state, boolean dynamic,
        boolean autoCreatedLeaf) {
      this(kind, state, dynamic, autoCreatedLeaf, Map.of(), DEFAULT_ORDERING,
          "PERCENTAGE", false, Set.of(""));
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    OldQueue(QueueKind kind, QueueState state, boolean dynamic,
        boolean autoCreatedLeaf, Map<String, Long> maximumAllocation,
        String orderingPolicyClass, String constructorCapacityType,
        boolean allowZeroCapacitySum, Set<String> configuredNodeLabels) {
      this.kind = kind;
      this.state = state;
      this.dynamic = dynamic;
      this.autoCreatedLeaf = autoCreatedLeaf;
      this.maximumAllocation = Map.copyOf(maximumAllocation);
      this.orderingPolicyClass = orderingPolicyClass;
      this.constructorCapacityType = constructorCapacityType;
      this.allowZeroCapacitySum = allowZeroCapacitySum;
      this.configuredNodeLabels = Set.copyOf(configuredNodeLabels);
    }

    public QueueKind getKind() {
      return kind;
    }

    public QueueState getState() {
      return state;
    }

    public boolean isDynamic() {
      return dynamic;
    }

    public boolean isAutoCreatedLeaf() {
      return autoCreatedLeaf;
    }

    public Map<String, Long> getMaximumAllocation() {
      return maximumAllocation;
    }

    public String getOrderingPolicyClass() {
      return orderingPolicyClass;
    }

    public String getConstructorCapacityType() {
      return constructorCapacityType;
    }

    public boolean getAllowZeroCapacitySum() {
      return allowZeroCapacitySum;
    }

    public Set<String> getConfiguredNodeLabels() {
      return configuredNodeLabels;
    }
  }

  private final Resource clusterResource;
  private final Resource minimumAllocation;
  private final Resource maximumAllocation;
  private final ResourceCalculator resourceCalculator;
  private final String resourceCalculatorClassName;
  private final Set<String> nodeLabels;
  private final Map<String, Resource> resourcesByLabel;
  private final Map<String, Long> clusterResourceValues;
  private final Map<String, Long> minimumAllocationValues;
  private final Map<String, Long> maximumAllocationValues;
  private final Map<String, Map<String, Long>> resourceValuesByLabel;
  private final List<String> resourceNames;
  private final Map<String, String> resourceUnits;
  private final Map<QueuePath, OldQueue> oldHierarchy;
  private final boolean hierarchyValidationSkipped;
  private final String authorizerClass;
  private final String groupMappingClass;
  private final String recordFactoryClass;
  private final String defaultRecordFactoryClass;
  private final List<PlacementPolicyDescriptor> placementPolicies;
  private final CSConfigModel previousModel;
  private final String schedulerClass;

  private ClusterFacts(Resource clusterResource, Resource minimumAllocation,
      Resource maximumAllocation, ResourceCalculator resourceCalculator,
      Set<String> nodeLabels, Map<String, Resource> resourcesByLabel,
      Map<QueuePath, OldQueue> oldHierarchy,
      boolean hierarchyValidationSkipped) {
    this(clusterResource, minimumAllocation, maximumAllocation,
        resourceCalculator, nodeLabels, resourcesByLabel, oldHierarchy,
        hierarchyValidationSkipped, DEFAULT_AUTHORIZER, DEFAULT_GROUP_MAPPING,
        DEFAULT_RECORD_FACTORY, DEFAULT_RECORD_FACTORY, List.of(), null,
        CapacityScheduler.class.getName());
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private ClusterFacts(Resource clusterResource, Resource minimumAllocation,
      Resource maximumAllocation, ResourceCalculator resourceCalculator,
      Set<String> nodeLabels, Map<String, Resource> resourcesByLabel,
      Map<QueuePath, OldQueue> oldHierarchy,
      boolean hierarchyValidationSkipped, String authorizerClass,
      String groupMappingClass, String recordFactoryClass,
      String defaultRecordFactoryClass,
      List<PlacementPolicyDescriptor> placementPolicies,
      CSConfigModel previousModel, String schedulerClass) {
    this.clusterResource = cloneOrNone(clusterResource);
    this.minimumAllocation = cloneOrNone(minimumAllocation);
    this.maximumAllocation = cloneOrNone(maximumAllocation);
    this.resourceCalculator = resourceCalculator == null
        ? new DefaultResourceCalculator() : resourceCalculator;
    this.resourceCalculatorClassName = this.resourceCalculator.getClass()
        .getName();
    this.nodeLabels = Collections.unmodifiableSet(new LinkedHashSet<>(
        nodeLabels));
    this.resourcesByLabel = immutableResourceMap(resourcesByLabel);
    this.clusterResourceValues = resourceValues(this.clusterResource);
    this.minimumAllocationValues = resourceValues(this.minimumAllocation);
    this.maximumAllocationValues = resourceValues(this.maximumAllocation);
    this.resourceValuesByLabel = resourceValueMap(this.resourcesByLabel);
    LinkedHashSet<String> names = new LinkedHashSet<>();
    names.addAll(clusterResourceValues.keySet());
    names.addAll(minimumAllocationValues.keySet());
    names.addAll(maximumAllocationValues.keySet());
    resourceValuesByLabel.values().forEach(values ->
        names.addAll(values.keySet()));
    this.resourceNames = List.copyOf(names);
    this.resourceUnits = resourceUnits(this.clusterResource,
        this.minimumAllocation, this.maximumAllocation,
        this.resourcesByLabel);
    this.oldHierarchy = Collections.unmodifiableMap(
        new LinkedHashMap<>(oldHierarchy));
    this.hierarchyValidationSkipped = hierarchyValidationSkipped;
    this.authorizerClass = authorizerClass;
    this.groupMappingClass = groupMappingClass;
    this.recordFactoryClass = recordFactoryClass;
    this.defaultRecordFactoryClass = defaultRecordFactoryClass;
    this.placementPolicies = List.copyOf(placementPolicies);
    this.previousModel = previousModel;
    this.schedulerClass = schedulerClass;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private Resource clusterResource = Resources.none();
    private Resource minimumAllocation = Resources.none();
    private Resource maximumAllocation = Resources.none();
    private ResourceCalculator resourceCalculator;
    private final Set<String> nodeLabels = new LinkedHashSet<>();
    private final Map<String, Resource> resourcesByLabel =
        new LinkedHashMap<>();
    private final Map<QueuePath, OldQueue> oldHierarchy =
        new LinkedHashMap<>();
    private boolean hierarchyValidationSkipped;

    public Builder withResources(Resource cluster, Resource minimum,
        Resource maximum, ResourceCalculator calculator) {
      this.clusterResource = cluster;
      this.minimumAllocation = minimum;
      this.maximumAllocation = maximum;
      this.resourceCalculator = calculator;
      return this;
    }

    public Builder withNodeLabels(Set<String> labels) {
      nodeLabels.clear();
      if (labels != null) {
        nodeLabels.addAll(labels);
      }
      return this;
    }

    public Builder withResourcesByLabel(Map<String, Resource> resources) {
      resourcesByLabel.clear();
      if (resources != null) {
        resourcesByLabel.putAll(resources);
      }
      return this;
    }

    public Builder withOldQueue(QueuePath path, QueueKind kind,
        QueueState state, boolean dynamic) {
      return withOldQueue(path, kind, state, dynamic, false);
    }

    public Builder withOldQueue(QueuePath path, QueueKind kind,
        QueueState state, boolean dynamic, boolean autoCreatedLeaf) {
      oldHierarchy.put(path, new OldQueue(kind, state, dynamic,
          autoCreatedLeaf));
      return this;
    }

    public Builder skipHierarchyValidation(boolean skip) {
      hierarchyValidationSkipped = skip;
      return this;
    }

    public ClusterFacts build() {
      return new ClusterFacts(clusterResource, minimumAllocation,
          maximumAllocation, resourceCalculator, nodeLabels, resourcesByLabel,
          oldHierarchy, hierarchyValidationSkipped);
    }
  }

  public static ClusterFacts empty() {
    return new ClusterFacts(Resources.none(), Resource.newInstance(
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES),
        Resource.newInstance(
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES),
        null, Collections.emptySet(), Collections.emptyMap(),
        Collections.emptyMap(), false);
  }

  public static ClusterFacts capture(CapacitySchedulerContext scheduler) {
    Collection<CSQueue> queues = scheduler.getCapacitySchedulerQueueManager()
        == null ? List.of() : scheduler.getCapacitySchedulerQueueManager()
            .getQueues().values();
    return capture(scheduler, queues);
  }

  /** Captures the exact existing store supplied to live materialization. */
  public static ClusterFacts capture(CapacitySchedulerContext scheduler,
      CSQueueStore queues) {
    return capture(scheduler, queues.getQueues());
  }

  /**
   * Captures materialization facts while the live context exposes a candidate.
   * @param scheduler live scheduler inputs
   * @param queues exact existing queue store
   * @param previousModel immutable configuration retained for rollback
   * @return matching runtime facts without replacing the rollback identity
   */
  public static ClusterFacts capture(CapacitySchedulerContext scheduler,
      CSQueueStore queues, CSConfigModel previousModel) {
    return capture(scheduler, queues.getQueues(), previousModel);
  }

  private static ClusterFacts capture(CapacitySchedulerContext scheduler,
      Collection<CSQueue> queues) {
    CapacitySchedulerConfiguration previous = scheduler.getConfiguration();
    return capture(scheduler, queues,
        previous == null ? null : previous.getModel());
  }

  private static ClusterFacts capture(CapacitySchedulerContext scheduler,
      Collection<CSQueue> queues, CSConfigModel previousModel) {
    Map<QueuePath, OldQueue> hierarchy = new LinkedHashMap<>();
    for (CSQueue queue : queues) {
      QueueKind kind = queue instanceof PlanQueue
          ? QueueKind.PLAN
          : queue instanceof ReservationQueue
              ? QueueKind.RESERVATION
              : queue instanceof ManagedParentQueue
          ? QueueKind.MANAGED_PARENT
          : queue instanceof AbstractParentQueue
              ? QueueKind.PARENT : QueueKind.LEAF;
      boolean autoCreatedLeaf = queue instanceof AbstractAutoCreatedLeafQueue;
      hierarchy.put(queue.getQueuePathObject(), new OldQueue(kind,
          queue.getState(), ((AbstractCSQueue) queue).isDynamicQueue(),
          autoCreatedLeaf, kind == QueueKind.LEAF
              ? resourceValues(queue.getMaximumAllocation()) : Map.of(),
          queue instanceof AbstractLeafQueue leaf && leaf.getOrderingPolicy() != null
              ? leaf.getOrderingPolicy().getClass().getName() : "",
          ((AbstractCSQueue) queue).getCapacityConfigType() == null
              ? "" : ((AbstractCSQueue) queue).getCapacityConfigType().name(),
          queue instanceof AbstractParentQueue parent && parent.getAllowZeroCapacitySum(),
          queue.getConfiguredNodeLabels()));
    }
    Resource clusterResource = scheduler.getClusterResource();
    Set<String> labels = new LinkedHashSet<>();
    Map<String, Resource> resources = new LinkedHashMap<>();
    RMNodeLabelsManager labelManager = scheduler.getRMContext() == null
        ? null : scheduler.getRMContext().getNodeLabelManager();
    if (labelManager != null) {
      labels.addAll(labelManager.getClusterNodeLabelNames());
      resources.put(RMNodeLabelsManager.NO_LABEL,
          labelManager.getResourceByLabel(RMNodeLabelsManager.NO_LABEL,
              clusterResource));
      for (String label : labels) {
        resources.put(label, labelManager.getResourceByLabel(label,
            clusterResource));
      }
    } else {
      resources.put(RMNodeLabelsManager.NO_LABEL, clusterResource);
    }
    boolean skip = scheduler.getQueueContext() != null
        && scheduler.getQueueContext().isHierarchyValidationSkipped();
    return new ClusterFacts(clusterResource,
        scheduler.getMinimumResourceCapability(),
        scheduler.getMaximumResourceCapability(),
        scheduler.getResourceCalculator(), labels, resources, hierarchy,
        skip, scheduler.getCapacitySchedulerQueueManager() == null
            ? DEFAULT_AUTHORIZER : scheduler.getCapacitySchedulerQueueManager()
                .getAuthorizationProviderClassName(),
        Groups.getInitializedProviderClassName(),
        Records.getRecordFactoryClassName(),
        RecordFactoryProvider.getDefaultRecordFactoryClassName(),
        placementPolicies(scheduler), previousModel, scheduler.getClass().getName());
  }

  private static List<PlacementPolicyDescriptor> placementPolicies(
      CapacitySchedulerContext scheduler) {
    if (scheduler.getRMContext() == null
        || scheduler.getRMContext().getQueuePlacementManager() == null) {
      return List.of();
    }
    List<PlacementRule> rules = scheduler.getRMContext()
        .getQueuePlacementManager().getPlacementRules();
    if (rules == null) {
      return List.of();
    }
    List<PlacementPolicyDescriptor> descriptors = new java.util.ArrayList<>();
    for (PlacementRule rule : rules) {
      Groups groups = rule.getClass() == CSMappingPlacementRule.class
          ? ((CSMappingPlacementRule) rule).getGroups() : null;
      descriptors.add(new PlacementPolicyDescriptor(rule.getClass().getName(),
          groups == null ? "" : groups.getClass().getName(),
          groups != null && groups.getClass() == Groups.class
              ? groups.getProviderClassName() : ""));
    }
    return descriptors;
  }

  /** Captures live facts, using a model when no live hierarchy is available. */
  public static ClusterFacts capture(CapacityScheduler scheduler,
      CSConfigModel currentModel) {
    ClusterFacts live = capture(scheduler);
    if (!live.oldHierarchy.isEmpty()) {
      return live;
    }
    Map<QueuePath, OldQueue> hierarchy = new LinkedHashMap<>();
    Map<QueuePath, QueueState> resolvedStates = new LinkedHashMap<>();
    for (QueueConfigNode node : currentModel.getNodes().values()) {
      QueueKind kind = ConfigurationValueParsers.parseBoolean(
          node.getRawProperty(CapacitySchedulerConfiguration.IS_RESERVABLE),
          false)
          ? QueueKind.PLAN
          : node.isAutoCreateChildQueueEnabled()
          ? QueueKind.MANAGED_PARENT
          : node.getChildren().isEmpty() ? QueueKind.LEAF : QueueKind.PARENT;
      QueueConfigNode parent = node.getParent();
      QueueState parentState = parent == null ? null
          : resolvedStates.get(parent.getQueuePath());
      QueueState state = QueueStateHelper.resolveInitialState(
          node.getQueuePath().getFullPath(),
          parent == null ? null : parent.getQueuePath().getFullPath(),
          node.getState(), parentState).state();
      resolvedStates.put(node.getQueuePath(), state);
      hierarchy.put(node.getQueuePath(), new OldQueue(kind, state, false,
          false));
    }
    return new ClusterFacts(live.clusterResource, live.minimumAllocation,
        live.maximumAllocation, live.resourceCalculator, live.nodeLabels,
        live.resourcesByLabel, hierarchy,
        live.hierarchyValidationSkipped, live.authorizerClass,
        live.groupMappingClass, live.recordFactoryClass,
        live.defaultRecordFactoryClass, live.placementPolicies, currentModel,
        live.schedulerClass);
  }

  CSConfigModel getPreviousModel() {
    return previousModel;
  }

  String getSchedulerClass() {
    return schedulerClass;
  }

  private static Resource cloneOrNone(Resource resource) {
    return resource == null ? Resources.none() : Resources.clone(resource);
  }

  public Resource getClusterResource() {
    return Resources.clone(clusterResource);
  }
  public Resource getMinimumAllocation() {
    return Resources.clone(minimumAllocation);
  }
  public Resource getMaximumAllocation() {
    return Resources.clone(maximumAllocation);
  }
  public ResourceCalculator getResourceCalculator() {
    return resourceCalculator;
  }
  /**
   * Returns the captured calculator descriptor without invoking calculator
   * behavior. Compiled validation normalizes this descriptor and never keeps
   * the live plugin instance.
   * @return captured resource-calculator class name
   */
  public String getResourceCalculatorClassName() {
    return resourceCalculatorClassName;
  }
  public Set<String> getNodeLabels() {
    return nodeLabels;
  }
  public Map<String, Resource> getResourcesByLabel() {
    return resourcesByLabel;
  }

  Map<String, Long> getClusterResourceValues() {
    return clusterResourceValues;
  }

  Map<String, Long> getMinimumAllocationValues() {
    return minimumAllocationValues;
  }

  Map<String, Long> getMaximumAllocationValues() {
    return maximumAllocationValues;
  }

  Map<String, Map<String, Long>> getResourceValuesByLabel() {
    return resourceValuesByLabel;
  }

  List<String> getResourceNames() {
    return resourceNames;
  }

  Map<String, String> getResourceUnits() {
    return resourceUnits;
  }
  public Map<QueuePath, OldQueue> getOldHierarchy() {
    return oldHierarchy;
  }
  public boolean isHierarchyValidationSkipped() {
    return hierarchyValidationSkipped;
  }

  public String getAuthorizerClass() {
    return authorizerClass;
  }

  public List<PlacementPolicyDescriptor> getPlacementPolicies() {
    return placementPolicies;
  }

  public String getGroupMappingClass() {
    return groupMappingClass;
  }

  public String getRecordFactoryClass() {
    return recordFactoryClass;
  }

  public String getDefaultRecordFactoryClass() {
    return defaultRecordFactoryClass;
  }

  private static Map<String, Resource> immutableResourceMap(
      Map<String, Resource> source) {
    Map<String, Resource> copy = new LinkedHashMap<>();
    source.forEach((label, resource) -> copy.put(label, cloneOrNone(resource)));
    return Collections.unmodifiableMap(copy);
  }

  private static Map<String, Long> resourceValues(Resource resource) {
    Map<String, Long> values = new LinkedHashMap<>();
    for (ResourceInformation information : resource.getResources()) {
      values.put(information.getName(), information.getValue());
    }
    return Collections.unmodifiableMap(values);
  }

  private static Map<String, Map<String, Long>> resourceValueMap(
      Map<String, Resource> resources) {
    Map<String, Map<String, Long>> values = new LinkedHashMap<>();
    resources.forEach((label, resource) ->
        values.put(label, resourceValues(resource)));
    return Collections.unmodifiableMap(values);
  }

  private static Map<String, String> resourceUnits(Resource cluster,
      Resource minimum, Resource maximum,
      Map<String, Resource> resourcesByLabel) {
    Map<String, String> units = new LinkedHashMap<>();
    List<Resource> resources = new ArrayList<>();
    resources.add(cluster);
    resources.add(minimum);
    resources.add(maximum);
    resources.addAll(resourcesByLabel.values());
    for (Resource resource : resources) {
      for (ResourceInformation information : resource.getResources()) {
        units.putIfAbsent(information.getName(), information.getUnits());
      }
    }
    return Collections.unmodifiableMap(units);
  }
}
