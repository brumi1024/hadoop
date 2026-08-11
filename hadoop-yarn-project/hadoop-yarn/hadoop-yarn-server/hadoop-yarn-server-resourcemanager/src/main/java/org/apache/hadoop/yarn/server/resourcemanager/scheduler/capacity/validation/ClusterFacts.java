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
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractAutoCreatedLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractCSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ManagedParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.CSConfigModel;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.QueueConfigNode;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

/** Point-in-time runtime facts used without mutating the live scheduler. */
public final class ClusterFacts {
  /** Queue shape relevant to refresh transition checks. */
  public enum QueueKind {
    LEAF, PARENT, MANAGED_PARENT
  }

  public static final class OldQueue {
    private final QueueKind kind;
    private final QueueState state;
    private final boolean dynamic;
    private final boolean autoCreatedLeaf;

    OldQueue(QueueKind kind, QueueState state, boolean dynamic,
        boolean autoCreatedLeaf) {
      this.kind = kind;
      this.state = state;
      this.dynamic = dynamic;
      this.autoCreatedLeaf = autoCreatedLeaf;
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
  }

  private final Resource clusterResource;
  private final Resource minimumAllocation;
  private final Resource maximumAllocation;
  private final ResourceCalculator resourceCalculator;
  private final Set<String> nodeLabels;
  private final Map<String, Resource> resourcesByLabel;
  private final Map<QueuePath, OldQueue> oldHierarchy;
  private final boolean hierarchyValidationSkipped;

  private ClusterFacts(Resource clusterResource, Resource minimumAllocation,
      Resource maximumAllocation, ResourceCalculator resourceCalculator,
      Set<String> nodeLabels, Map<String, Resource> resourcesByLabel,
      Map<QueuePath, OldQueue> oldHierarchy,
      boolean hierarchyValidationSkipped) {
    this.clusterResource = cloneOrNone(clusterResource);
    this.minimumAllocation = cloneOrNone(minimumAllocation);
    this.maximumAllocation = cloneOrNone(maximumAllocation);
    this.resourceCalculator = resourceCalculator == null
        ? new DefaultResourceCalculator() : resourceCalculator;
    this.nodeLabels = Collections.unmodifiableSet(new LinkedHashSet<>(
        nodeLabels));
    this.resourcesByLabel = immutableResourceMap(resourcesByLabel);
    this.oldHierarchy = Collections.unmodifiableMap(
        new LinkedHashMap<>(oldHierarchy));
    this.hierarchyValidationSkipped = hierarchyValidationSkipped;
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

  public static ClusterFacts capture(CapacityScheduler scheduler) {
    Map<QueuePath, OldQueue> hierarchy = new LinkedHashMap<>();
    if (scheduler.getCapacitySchedulerQueueManager() != null) {
      for (CSQueue queue : scheduler.getCapacitySchedulerQueueManager()
          .getQueues().values()) {
        QueueKind kind = queue instanceof ManagedParentQueue
            ? QueueKind.MANAGED_PARENT
            : queue instanceof AbstractParentQueue
                ? QueueKind.PARENT : QueueKind.LEAF;
        boolean autoCreatedLeaf = queue instanceof AbstractAutoCreatedLeafQueue;
        hierarchy.put(queue.getQueuePathObject(), new OldQueue(kind,
            queue.getState(), ((AbstractCSQueue) queue).isDynamicQueue(),
            autoCreatedLeaf));
      }
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
        skip);
  }

  /** Captures live facts, using a model when no live hierarchy is available. */
  public static ClusterFacts capture(CapacityScheduler scheduler,
      CSConfigModel currentModel) {
    ClusterFacts live = capture(scheduler);
    if (!live.oldHierarchy.isEmpty()) {
      return live;
    }
    Map<QueuePath, OldQueue> hierarchy = new LinkedHashMap<>();
    for (QueueConfigNode node : currentModel.getNodes().values()) {
      QueueKind kind = node.isAutoCreateChildQueueEnabled()
          ? QueueKind.MANAGED_PARENT
          : node.getChildren().isEmpty() ? QueueKind.LEAF : QueueKind.PARENT;
      hierarchy.put(node.getQueuePath(), new OldQueue(kind, node.getState(),
          false, false));
    }
    return new ClusterFacts(live.clusterResource, live.minimumAllocation,
        live.maximumAllocation, live.resourceCalculator, live.nodeLabels,
        live.resourcesByLabel, hierarchy,
        live.hierarchyValidationSkipped);
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
  public Set<String> getNodeLabels() {
    return nodeLabels;
  }
  public Map<String, Resource> getResourcesByLabel() {
    return resourcesByLabel;
  }
  public Map<QueuePath, OldQueue> getOldHierarchy() {
    return oldHierarchy;
  }
  public boolean isHierarchyValidationSkipped() {
    return hierarchyValidationSkipped;
  }

  private static Map<String, Resource> immutableResourceMap(
      Map<String, Resource> source) {
    Map<String, Resource> copy = new LinkedHashMap<>();
    source.forEach((label, resource) -> copy.put(label, cloneOrNone(resource)));
    return Collections.unmodifiableMap(copy);
  }
}
