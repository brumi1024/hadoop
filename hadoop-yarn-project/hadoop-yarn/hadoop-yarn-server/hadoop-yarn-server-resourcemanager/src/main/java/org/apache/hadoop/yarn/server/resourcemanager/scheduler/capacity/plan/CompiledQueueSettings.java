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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Immutable configuration-derived settings used by one queue.
 *
 * <p>This type stores descriptors and scalar values only. Policy instances,
 * queue metrics, managers, authorizers, applications, and mutable resource
 * accounting remain live-materialization concerns.</p>
 */
public final class CompiledQueueSettings {

  /** Immutable normalized resource values keyed by resource name. */
  public static final class ResourceSetting {
    private final Map<String, Long> values;

    ResourceSetting(Map<String, Long> values) {
      this.values = Collections.unmodifiableMap(
          new LinkedHashMap<>(values));
    }

    public Map<String, Long> getValues() {
      return values;
    }

    public long getValue(String resourceName) {
      return values.getOrDefault(resourceName, 0L);
    }
  }

  /** Descriptors used when a parent creates queues dynamically. */
  public static final class DynamicQueueSettings {
    private final boolean failCreationWhenCapacityExceeded;
    private final int maximumLegacyChildQueues;
    private final int maximumFlexibleChildQueues;
    private final String managementPolicyClassName;
    private final Map<String, String> commonTemplateProperties;
    private final Map<String, String> leafTemplateProperties;
    private final Map<String, String> parentTemplateProperties;

    @SuppressWarnings("checkstyle:ParameterNumber")
    DynamicQueueSettings(boolean failCreationWhenCapacityExceeded,
        int maximumLegacyChildQueues, int maximumFlexibleChildQueues,
        String managementPolicyClassName,
        Map<String, String> commonTemplateProperties,
        Map<String, String> leafTemplateProperties,
        Map<String, String> parentTemplateProperties) {
      this.failCreationWhenCapacityExceeded =
          failCreationWhenCapacityExceeded;
      this.maximumLegacyChildQueues = maximumLegacyChildQueues;
      this.maximumFlexibleChildQueues = maximumFlexibleChildQueues;
      this.managementPolicyClassName = managementPolicyClassName;
      this.commonTemplateProperties = immutableMap(commonTemplateProperties);
      this.leafTemplateProperties = immutableMap(leafTemplateProperties);
      this.parentTemplateProperties = immutableMap(parentTemplateProperties);
    }

    public boolean isFailCreationWhenCapacityExceeded() {
      return failCreationWhenCapacityExceeded;
    }

    public int getMaximumLegacyChildQueues() {
      return maximumLegacyChildQueues;
    }

    public int getMaximumFlexibleChildQueues() {
      return maximumFlexibleChildQueues;
    }

    public String getManagementPolicyClassName() {
      return managementPolicyClassName;
    }

    public Map<String, String> getCommonTemplateProperties() {
      return commonTemplateProperties;
    }

    public Map<String, String> getLeafTemplateProperties() {
      return leafTemplateProperties;
    }

    public Map<String, String> getParentTemplateProperties() {
      return parentTemplateProperties;
    }
  }

  private final ResourceSetting maximumAllocation;
  private final Map<String, Float> userWeights;
  private final long maximumApplicationLifetime;
  private final long defaultApplicationLifetime;
  private final boolean defaultApplicationLifetimeConfigured;
  private final int maximumParallelApplications;
  private final boolean preemptionDisabled;
  private final boolean intraQueuePreemptionDisabledInHierarchy;
  private final int priority;
  private final int defaultApplicationPriority;
  private final String applicationOrderingPolicy;
  private final String parentQueueOrderingPolicy;
  private final String multiNodeSortingPolicyClassName;
  private final boolean allowZeroCapacitySum;
  private final boolean showReservationsAsQueues;
  private final Map<String, Float> maximumApplicationMasterShares;
  private final DynamicQueueSettings dynamicQueueSettings;

  @SuppressWarnings("checkstyle:ParameterNumber")
  CompiledQueueSettings(ResourceSetting maximumAllocation,
      Map<String, Float> userWeights, long maximumApplicationLifetime,
      long defaultApplicationLifetime,
      boolean defaultApplicationLifetimeConfigured,
      int maximumParallelApplications, boolean preemptionDisabled,
      boolean intraQueuePreemptionDisabledInHierarchy, int priority,
      int defaultApplicationPriority, String applicationOrderingPolicy,
      String parentQueueOrderingPolicy,
      String multiNodeSortingPolicyClassName, boolean allowZeroCapacitySum,
      boolean showReservationsAsQueues,
      Map<String, Float> maximumApplicationMasterShares,
      DynamicQueueSettings dynamicQueueSettings) {
    this.maximumAllocation = maximumAllocation;
    this.userWeights = immutableMap(userWeights);
    this.maximumApplicationLifetime = maximumApplicationLifetime;
    this.defaultApplicationLifetime = defaultApplicationLifetime;
    this.defaultApplicationLifetimeConfigured =
        defaultApplicationLifetimeConfigured;
    this.maximumParallelApplications = maximumParallelApplications;
    this.preemptionDisabled = preemptionDisabled;
    this.intraQueuePreemptionDisabledInHierarchy =
        intraQueuePreemptionDisabledInHierarchy;
    this.priority = priority;
    this.defaultApplicationPriority = defaultApplicationPriority;
    this.applicationOrderingPolicy = applicationOrderingPolicy;
    this.parentQueueOrderingPolicy = parentQueueOrderingPolicy;
    this.multiNodeSortingPolicyClassName = multiNodeSortingPolicyClassName;
    this.allowZeroCapacitySum = allowZeroCapacitySum;
    this.showReservationsAsQueues = showReservationsAsQueues;
    this.maximumApplicationMasterShares = immutableMap(
        maximumApplicationMasterShares);
    this.dynamicQueueSettings = dynamicQueueSettings;
  }

  public ResourceSetting getMaximumAllocation() {
    return maximumAllocation;
  }

  public Map<String, Float> getUserWeights() {
    return userWeights;
  }

  public long getMaximumApplicationLifetime() {
    return maximumApplicationLifetime;
  }

  public long getDefaultApplicationLifetime() {
    return defaultApplicationLifetime;
  }

  public boolean isDefaultApplicationLifetimeConfigured() {
    return defaultApplicationLifetimeConfigured;
  }

  public int getMaximumParallelApplications() {
    return maximumParallelApplications;
  }

  public boolean isPreemptionDisabled() {
    return preemptionDisabled;
  }

  public boolean isIntraQueuePreemptionDisabledInHierarchy() {
    return intraQueuePreemptionDisabledInHierarchy;
  }

  public int getPriority() {
    return priority;
  }

  public int getDefaultApplicationPriority() {
    return defaultApplicationPriority;
  }

  public String getApplicationOrderingPolicy() {
    return applicationOrderingPolicy;
  }

  public String getParentQueueOrderingPolicy() {
    return parentQueueOrderingPolicy;
  }

  public String getMultiNodeSortingPolicyClassName() {
    return multiNodeSortingPolicyClassName;
  }

  public boolean isAllowZeroCapacitySum() {
    return allowZeroCapacitySum;
  }

  public boolean isShowReservationsAsQueues() {
    return showReservationsAsQueues;
  }

  public Map<String, Float> getMaximumApplicationMasterShares() {
    return maximumApplicationMasterShares;
  }

  public DynamicQueueSettings getDynamicQueueSettings() {
    return dynamicQueueSettings;
  }

  private static <K, V> Map<K, V> immutableMap(Map<K, V> source) {
    return Collections.unmodifiableMap(new LinkedHashMap<>(source));
  }
}
