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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.plan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.CSConfigModel;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.QueueConfigNode;

/**
 * Immutable, configuration-derived description of a Capacity Scheduler queue
 * hierarchy.
 *
 * <p>The plan deliberately contains no live scheduler objects, queue stores,
 * metrics, managers, applications, locks, authorizers, or plugin instances.
 * Mutable values returned by the legacy capacity-vector type are defensive
 * copies.</p>
 */
public final class ValidatedQueuePlan {

  /** Static queue implementation selected by configuration. */
  public enum QueueKind {
    LEAF,
    PARENT,
    MANAGED_PARENT,
    PLAN
  }

  /** Immutable typed capacity setting for one partition. */
  public static final class CapacitySetting {
    private final String rawValue;
    private final QueueCapacityVector vector;

    private CapacitySetting(QueueConfigNode.CapacityValue value,
        QueueCapacityVector fallback, boolean fallbackWhenEmpty) {
      this.rawValue = value == null ? null : value.getRawValue();
      QueueCapacityVector configured = value == null
          ? null : value.getVector();
      this.vector = copyVector(configured == null
          || fallbackWhenEmpty && configured.isEmpty()
              ? fallback : configured);
    }

    public String getRawValue() {
      return rawValue;
    }

    public QueueCapacityVector getVector() {
      return copyVector(vector);
    }
  }

  /** Immutable settings and topology for one configured queue. */
  public static final class QueuePlanNode {
    private final QueuePath queuePath;
    private final QueuePath parentPath;
    private final List<QueuePath> childPaths;
    private final QueueKind kind;
    private final Map<String, CapacitySetting> capacities;
    private final Map<String, CapacitySetting> maximumCapacities;
    private final Set<String> declaredAccessibleNodeLabels;
    private final Set<String> accessibleNodeLabels;
    private final Set<String> configuredNodeLabels;
    private final QueueState configuredState;
    private final QueueState initialState;
    private final String orderingPolicy;
    private final Map<String, String> orderingPolicyParameters;
    private final int maximumApplications;
    private final float maximumApplicationMasterShare;
    private final float userLimit;
    private final float userLimitFactor;
    private final String declaredDefaultNodeLabelExpression;
    private final String defaultNodeLabelExpression;
    private final boolean autoCreateChildQueueEnabled;
    private final boolean autoQueueCreationV2Enabled;
    private final boolean reservable;
    private final Map<String, String> aclProperties;

    @SuppressWarnings("checkstyle:ParameterNumber")
    private QueuePlanNode(QueueConfigNode source, QueuePlanNode parent,
        List<QueuePath> childPaths, QueueKind kind,
        Map<String, CapacitySetting> capacities,
        Map<String, CapacitySetting> maximumCapacities,
        Set<String> declaredAccessibleNodeLabels,
        Set<String> accessibleNodeLabels, Set<String> configuredNodeLabels,
        QueueState initialState, String defaultNodeLabelExpression) {
      this.queuePath = source.getQueuePath();
      this.parentPath = parent == null ? null : parent.getQueuePath();
      this.childPaths = List.copyOf(childPaths);
      this.kind = kind;
      this.capacities = immutableMap(capacities);
      this.maximumCapacities = immutableMap(maximumCapacities);
      this.declaredAccessibleNodeLabels = immutableNullableSet(
          declaredAccessibleNodeLabels);
      this.accessibleNodeLabels = immutableNullableSet(accessibleNodeLabels);
      this.configuredNodeLabels = Collections.unmodifiableSet(
          new LinkedHashSet<>(configuredNodeLabels));
      this.configuredState = source.getState();
      this.initialState = initialState;
      this.orderingPolicy = source.getOrderingPolicy();
      this.orderingPolicyParameters = Collections.unmodifiableMap(
          new LinkedHashMap<>(source.getOrderingPolicyParameters()));
      this.maximumApplications = source.getMaximumApplications();
      this.maximumApplicationMasterShare =
          source.getMaximumApplicationMasterShare();
      this.userLimit = source.getUserLimit();
      this.userLimitFactor = source.getUserLimitFactor();
      this.declaredDefaultNodeLabelExpression =
          source.getDefaultNodeLabelExpression();
      this.defaultNodeLabelExpression = defaultNodeLabelExpression;
      this.autoCreateChildQueueEnabled =
          source.isAutoCreateChildQueueEnabled();
      this.autoQueueCreationV2Enabled =
          source.isAutoQueueCreationV2Enabled();
      this.reservable = source.isReservable();
      this.aclProperties = Collections.unmodifiableMap(
          new LinkedHashMap<>(source.getAclProperties()));
    }

    public QueuePath getQueuePath() {
      return queuePath;
    }

    public QueuePath getParentPath() {
      return parentPath;
    }

    public List<QueuePath> getChildPaths() {
      return childPaths;
    }

    public QueueKind getKind() {
      return kind;
    }

    public CapacitySetting getCapacity(String label) {
      return capacities.get(label);
    }

    public CapacitySetting getMaximumCapacity(String label) {
      return maximumCapacities.get(label);
    }

    public Map<String, CapacitySetting> getCapacities() {
      return capacities;
    }

    public Map<String, CapacitySetting> getMaximumCapacities() {
      return maximumCapacities;
    }

    public Set<String> getDeclaredAccessibleNodeLabels() {
      return declaredAccessibleNodeLabels;
    }

    public Set<String> getAccessibleNodeLabels() {
      return accessibleNodeLabels;
    }

    public Set<String> getConfiguredNodeLabels() {
      return configuredNodeLabels;
    }

    public QueueState getConfiguredState() {
      return configuredState;
    }

    public QueueState getInitialState() {
      return initialState;
    }

    public String getOrderingPolicy() {
      return orderingPolicy;
    }

    public Map<String, String> getOrderingPolicyParameters() {
      return orderingPolicyParameters;
    }

    public int getMaximumApplications() {
      return maximumApplications;
    }

    public float getMaximumApplicationMasterShare() {
      return maximumApplicationMasterShare;
    }

    public float getUserLimit() {
      return userLimit;
    }

    public float getUserLimitFactor() {
      return userLimitFactor;
    }

    public String getDeclaredDefaultNodeLabelExpression() {
      return declaredDefaultNodeLabelExpression;
    }

    public String getDefaultNodeLabelExpression() {
      return defaultNodeLabelExpression;
    }

    public boolean isAutoCreateChildQueueEnabled() {
      return autoCreateChildQueueEnabled;
    }

    public boolean isAutoQueueCreationV2Enabled() {
      return autoQueueCreationV2Enabled;
    }

    public boolean isReservable() {
      return reservable;
    }

    public Map<String, String> getAclProperties() {
      return aclProperties;
    }
  }

  private final QueuePlanNode root;
  private final Map<QueuePath, QueuePlanNode> queues;
  private final boolean legacyQueueMode;
  private final String resourceTypes;

  private ValidatedQueuePlan(QueuePlanNode root,
      Map<QueuePath, QueuePlanNode> queues, boolean legacyQueueMode,
      String resourceTypes) {
    this.root = root;
    this.queues = Collections.unmodifiableMap(new LinkedHashMap<>(queues));
    this.legacyQueueMode = legacyQueueMode;
    this.resourceTypes = resourceTypes;
  }

  /**
   * Compiles the configuration-derived portion of a model into an immutable
   * queue plan.
   *
   * @param model parsed Capacity Scheduler configuration
   * @return immutable queue plan
   */
  public static ValidatedQueuePlan fromModel(CSConfigModel model) {
    Map<QueuePath, QueuePlanNode> queues = new LinkedHashMap<>();
    QueuePlanNode root = compileNode(model, model.getRoot(), null, queues);
    return new ValidatedQueuePlan(root, queues, model.isLegacyQueueMode(),
        model.getResourceTypes());
  }

  public QueuePlanNode getRoot() {
    return root;
  }

  public QueuePlanNode getQueue(QueuePath path) {
    return queues.get(path);
  }

  public Map<QueuePath, QueuePlanNode> getQueues() {
    return queues;
  }

  public boolean isLegacyQueueMode() {
    return legacyQueueMode;
  }

  public String getResourceTypes() {
    return resourceTypes;
  }

  private static QueuePlanNode compileNode(CSConfigModel model,
      QueueConfigNode source, QueuePlanNode parent,
      Map<QueuePath, QueuePlanNode> queues) {
    List<QueuePath> childPaths = new ArrayList<>();
    source.getChildren().values().forEach(child ->
        childPaths.add(child.getQueuePath()));

    Set<String> configuredLabels = new LinkedHashSet<>(
        source.getConfiguredNodeLabels());
    configuredLabels.add(RMNodeLabelsManager.NO_LABEL);
    if (source.getQueuePath().isRoot()) {
      configuredLabels.addAll(model.getConfiguredNodeLabels(
          source.getQueuePath()));
    }

    Map<String, CapacitySetting> capacities = new LinkedHashMap<>();
    Map<String, CapacitySetting> maximumCapacities = new LinkedHashMap<>();
    for (String label : configuredLabels) {
      QueueCapacityVector rootMinimum = source.getQueuePath().isRoot()
          ? QueueCapacityVector.of(100,
              QueueCapacityVector.ResourceUnitCapacityType.PERCENTAGE)
          : QueueCapacityVector.newInstance();
      QueueCapacityVector defaultMaximum = source.getQueuePath().isRoot()
          ? QueueCapacityVector.of(100,
              QueueCapacityVector.ResourceUnitCapacityType.PERCENTAGE)
          : QueueCapacityVector.newInstance();
      capacities.put(label, new CapacitySetting(source.getCapacity(label),
          rootMinimum, false));
      maximumCapacities.put(label, new CapacitySetting(
          source.getMaximumCapacity(label), defaultMaximum, true));
    }

    Set<String> declaredLabels = source.getAccessibleNodeLabels();
    Set<String> effectiveLabels = declaredLabels != null
        ? declaredLabels
        : parent == null ? null : parent.getAccessibleNodeLabels();
    String defaultExpression = source.getDefaultNodeLabelExpression();
    if (defaultExpression == null && effectiveLabels != null && parent != null
        && effectiveLabels.containsAll(parent.getAccessibleNodeLabels())) {
      defaultExpression = parent.getDefaultNodeLabelExpression();
    }

    QueueState initialState = initialState(source.getState(), parent);
    QueuePlanNode compiled = new QueuePlanNode(source, parent, childPaths,
        kind(source), capacities, maximumCapacities, declaredLabels,
        effectiveLabels, configuredLabels, initialState, defaultExpression);
    queues.put(compiled.getQueuePath(), compiled);
    for (QueueConfigNode child : source.getChildren().values()) {
      compileNode(model, child, compiled, queues);
    }
    return compiled;
  }

  private static QueueKind kind(QueueConfigNode node) {
    if (node.isReservable() && node.getChildren().isEmpty()
        && !node.isAutoCreateChildQueueEnabled()
        && !node.isAutoQueueCreationV2Enabled()) {
      return QueueKind.PLAN;
    }
    if (node.isAutoCreateChildQueueEnabled()) {
      return QueueKind.MANAGED_PARENT;
    }
    if (!node.getChildren().isEmpty()
        || node.isAutoQueueCreationV2Enabled()) {
      return QueueKind.PARENT;
    }
    return QueueKind.LEAF;
  }

  private static QueueState initialState(QueueState configured,
      QueuePlanNode parent) {
    if (configured != null) {
      return configured;
    }
    if (parent == null) {
      return QueueState.RUNNING;
    }
    return parent.getInitialState() == QueueState.DRAINING
        ? QueueState.STOPPED : parent.getInitialState();
  }

  private static QueueCapacityVector copyVector(QueueCapacityVector source) {
    QueueCapacityVector copy = new QueueCapacityVector();
    for (QueueCapacityVector.QueueCapacityVectorEntry entry : source) {
      copy.setResource(entry.getResourceName(), entry.getResourceValue(),
          entry.getVectorResourceType());
    }
    return copy;
  }

  private static <K, V> Map<K, V> immutableMap(Map<K, V> source) {
    return Collections.unmodifiableMap(new LinkedHashMap<>(source));
  }

  private static <T> Set<T> immutableNullableSet(Set<T> source) {
    return source == null ? null : Collections.unmodifiableSet(
        new LinkedHashSet<>(source));
  }
}
