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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;

/** Immutable typed configuration for one queue. */
public final class QueueConfigNode {
  /** Raw and canonical forms of one queue capacity property. */
  public static final class CapacityValue {
    private final String rawValue;
    private final QueueCapacityVector vector;

    CapacityValue(String rawValue, QueueCapacityVector vector) {
      this.rawValue = rawValue;
      this.vector = copyVector(vector);
    }

    public String getRawValue() {
      return rawValue;
    }

    public QueueCapacityVector getVector() {
      return copyVector(vector);
    }

    private static QueueCapacityVector copyVector(QueueCapacityVector source) {
      QueueCapacityVector copy = new QueueCapacityVector();
      for (QueueCapacityVector.QueueCapacityVectorEntry entry : source) {
        copy.setResource(entry.getResourceName(), entry.getResourceValue(),
            entry.getVectorResourceType());
      }
      return copy;
    }
  }

  private final QueuePath queuePath;
  private final QueueConfigNode parent;
  private final Map<String, QueueConfigNode> mutableChildren;
  private final Map<String, QueueConfigNode> children;
  private final Map<String, String> rawProperties;
  private final Map<String, ConfigProvenance> provenance;
  private final Map<String, CapacityValue> capacities;
  private final Map<String, CapacityValue> maximumCapacities;
  private final Set<String> accessibleNodeLabels;
  private final Set<String> configuredNodeLabels;
  private final QueueState state;
  private final String orderingPolicy;
  private final Map<String, String> orderingPolicyParameters;
  private final int maximumApplications;
  private final float maximumApplicationMasterShare;
  private final float userLimit;
  private final float userLimitFactor;
  private final String defaultNodeLabelExpression;
  private final boolean autoCreateChildQueueEnabled;
  private final boolean autoQueueCreationV2Enabled;

  @SuppressWarnings("checkstyle:ParameterNumber")
  QueueConfigNode(QueuePath queuePath, QueueConfigNode parent,
      Map<String, String> rawProperties,
      Map<String, ConfigProvenance> provenance,
      Map<String, CapacityValue> capacities,
      Map<String, CapacityValue> maximumCapacities,
      Set<String> accessibleNodeLabels, Set<String> configuredNodeLabels,
      QueueState state, String orderingPolicy,
      Map<String, String> orderingPolicyParameters, int maximumApplications,
      float maximumApplicationMasterShare, float userLimit,
      float userLimitFactor, String defaultNodeLabelExpression,
      boolean autoCreateChildQueueEnabled,
      boolean autoQueueCreationV2Enabled) {
    this.queuePath = queuePath;
    this.parent = parent;
    this.mutableChildren = new LinkedHashMap<>();
    this.children = Collections.unmodifiableMap(mutableChildren);
    this.rawProperties = Collections.unmodifiableMap(
        new LinkedHashMap<>(rawProperties));
    this.provenance = Collections.unmodifiableMap(new LinkedHashMap<>(provenance));
    this.capacities = Collections.unmodifiableMap(new LinkedHashMap<>(capacities));
    this.maximumCapacities = Collections.unmodifiableMap(
        new LinkedHashMap<>(maximumCapacities));
    this.accessibleNodeLabels = accessibleNodeLabels;
    this.configuredNodeLabels = Collections.unmodifiableSet(configuredNodeLabels);
    this.state = state;
    this.orderingPolicy = orderingPolicy;
    this.orderingPolicyParameters = Collections.unmodifiableMap(
        new LinkedHashMap<>(orderingPolicyParameters));
    this.maximumApplications = maximumApplications;
    this.maximumApplicationMasterShare = maximumApplicationMasterShare;
    this.userLimit = userLimit;
    this.userLimitFactor = userLimitFactor;
    this.defaultNodeLabelExpression = defaultNodeLabelExpression;
    this.autoCreateChildQueueEnabled = autoCreateChildQueueEnabled;
    this.autoQueueCreationV2Enabled = autoQueueCreationV2Enabled;
  }

  public QueuePath getQueuePath() {
    return queuePath;
  }

  public QueueConfigNode getParent() {
    return parent;
  }

  public Map<String, QueueConfigNode> getChildren() {
    return children;
  }

  void addChild(QueueConfigNode child) {
    mutableChildren.put(child.getQueuePath().getLeafName(), child);
  }

  public Map<String, String> getRawProperties() {
    return rawProperties;
  }

  public String getRawProperty(String suffix) {
    return rawProperties.get(suffix);
  }

  public ConfigProvenance getProvenance(String suffix) {
    return provenance.getOrDefault(suffix, ConfigProvenance.DEFAULT);
  }

  public CapacityValue getCapacity(String label) {
    return capacities.get(label);
  }

  public CapacityValue getMaximumCapacity(String label) {
    return maximumCapacities.get(label);
  }

  public Set<String> getCapacityLabels() {
    return capacities.keySet();
  }

  public Set<String> getAccessibleNodeLabels() {
    return accessibleNodeLabels;
  }

  public Set<String> getConfiguredNodeLabels() {
    return configuredNodeLabels;
  }

  public QueueState getState() {
    return state;
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

  public String getDefaultNodeLabelExpression() {
    return defaultNodeLabelExpression;
  }

  public boolean isAutoCreateChildQueueEnabled() {
    return autoCreateChildQueueEnabled;
  }

  public boolean isAutoQueueCreationV2Enabled() {
    return autoQueueCreationV2Enabled;
  }

}
