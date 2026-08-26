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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.CSConfigModel;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.ConfigDiagnostic;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.QueueConfigNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.plan.CompiledQueueSettings.DynamicQueueSettings;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.plan.CompiledQueueSettings.ResourceSetting;

/** Compiles queue-constructor inputs without creating queue objects. */
final class QueuePlanSettingsCompiler {
  private static final Pattern RESOURCE_VALUE =
      Pattern.compile("^(-?[0-9]+)([mMgG]?)$");

  private final CSConfigModel model;
  private final CapacitySchedulerConfiguration configuration;
  private final List<ConfigDiagnostic> diagnostics = new ArrayList<>();
  private final CompiledSchedulerSettings schedulerSettings;
  private final boolean systemPreemptionEnabled;
  private final boolean systemIntraQueuePreemptionEnabled;

  QueuePlanSettingsCompiler(CSConfigModel model) {
    this.model = model;
    this.configuration = new CapacitySchedulerConfiguration(
        new Configuration(false), false);
    model.getRawProperties().forEach(configuration::set);
    this.schedulerSettings = compileSchedulerSettings();
    this.systemPreemptionEnabled = configuration.getBoolean(
        YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ENABLE_MONITORS);
    this.systemIntraQueuePreemptionEnabled = configuration.getBoolean(
        CapacitySchedulerConfiguration.INTRAQUEUE_PREEMPTION_ENABLED,
        CapacitySchedulerConfiguration.DEFAULT_INTRAQUEUE_PREEMPTION_ENABLED);
  }

  CompiledSchedulerSettings getSchedulerSettings() {
    return schedulerSettings;
  }

  List<ConfigDiagnostic> getDiagnostics() {
    return List.copyOf(diagnostics);
  }

  CompiledQueueSettings compile(QueueConfigNode source,
      CompiledQueueSettings parent, Iterable<String> configuredLabels) {
    QueuePath path = source.getQueuePath();
    ResourceSetting maximumAllocation = maximumAllocation(source, parent);
    Map<String, Float> userWeights = userWeights(source, parent);

    long configuredMaximumLifetime = queueLong(source,
        CapacitySchedulerConfiguration.MAXIMUM_LIFETIME_SUFFIX, -1L);
    long maximumLifetime = path.isRoot() || configuredMaximumLifetime >= 0
        ? configuredMaximumLifetime
        : parent.getMaximumApplicationLifetime();
    long configuredDefaultLifetime = queueLong(source,
        CapacitySchedulerConfiguration.DEFAULT_LIFETIME_SUFFIX, -1L);
    boolean defaultLifetimeConfigured = configuredDefaultLifetime >= 0
        || !path.isRoot()
            && parent.isDefaultApplicationLifetimeConfigured();
    long defaultLifetime = defaultApplicationLifetime(path, parent,
        maximumLifetime, configuredDefaultLifetime,
        defaultLifetimeConfigured);

    int maximumParallelApplications = parsed(
        () -> configuration.getMaxParallelAppsForQueue(path),
        CapacitySchedulerConfiguration.DEFAULT_MAX_PARALLEL_APPLICATIONS,
        path, fullKey(path,
            CapacitySchedulerConfiguration.MAX_PARALLEL_APPLICATIONS),
        "invalid-integer");

    boolean parentPreemptionDisabled = parent != null
        && parent.isPreemptionDisabled();
    boolean preemptionDisabled = !systemPreemptionEnabled
        || configuration.getPreemptionDisabled(path,
            parentPreemptionDisabled);
    boolean parentIntraQueuePreemptionDisabled = parent != null
        && parent.isIntraQueuePreemptionDisabledInHierarchy();
    boolean intraQueuePreemptionDisabled =
        !systemIntraQueuePreemptionEnabled
            || configuration.getIntraQueuePreemptionDisabled(path,
                parentIntraQueuePreemptionDisabled);

    int priority = parsed(
        () -> configuration.getQueuePriority(path).getPriority(), 0, path,
        fullKey(path, "priority"), "invalid-integer");
    int defaultApplicationPriority = parsed(
        () -> configuration.getDefaultApplicationPriorityConfPerQueue(path),
        CapacitySchedulerConfiguration
            .DEFAULT_CONFIGURATION_APPLICATION_PRIORITY,
        path, fullKey(path,
            CapacitySchedulerConfiguration.DEFAULT_APPLICATION_PRIORITY),
        "invalid-integer");

    String applicationOrderingPolicy = source.getOrderingPolicy().trim();
    String configuredParentPolicy = source.getRawProperty(
        CapacitySchedulerConfiguration.ORDERING_POLICY);
    String parentQueueOrderingPolicy = configuredParentPolicy == null
        ? parent == null
            ? CapacitySchedulerConfiguration.DEFAULT_QUEUE_ORDERING_POLICY
            : parent.getParentQueueOrderingPolicy()
        : configuredParentPolicy.trim();

    String multiNodePolicyClassName = multiNodePolicyClass(source);
    boolean allowZeroCapacitySum = configuration.getAllowZeroCapacitySum(path);
    boolean showReservationsAsQueues = configuration
        .getShowReservationAsQueues(path);
    Map<String, Float> maximumAmShares = maximumAmShares(source,
        configuredLabels);
    DynamicQueueSettings dynamicSettings = dynamicSettings(path);

    return new CompiledQueueSettings(maximumAllocation, userWeights,
        maximumLifetime, defaultLifetime, defaultLifetimeConfigured,
        maximumParallelApplications, preemptionDisabled,
        intraQueuePreemptionDisabled, priority, defaultApplicationPriority,
        applicationOrderingPolicy, parentQueueOrderingPolicy,
        multiNodePolicyClassName, allowZeroCapacitySum,
        showReservationsAsQueues, maximumAmShares, dynamicSettings);
  }

  private CompiledSchedulerSettings compileSchedulerSettings() {
    Map<String, Long> minimum = new LinkedHashMap<>();
    minimum.put(ResourceInformation.MEMORY_URI, parsed(
        () -> configuration.getLong(
            YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB),
        (long) YarnConfiguration
            .DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        null, YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        "invalid-integer"));
    minimum.put(ResourceInformation.VCORES_URI, parsed(
        () -> configuration.getLong(
            YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES),
        (long) YarnConfiguration
            .DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
        null, YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
        "invalid-integer"));

    Map<String, Long> maximum = new LinkedHashMap<>();
    maximum.put(ResourceInformation.MEMORY_URI, parsed(
        () -> configuration.getLong(
            YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB),
        (long) YarnConfiguration
            .DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        null, YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        "invalid-integer"));
    maximum.put(ResourceInformation.VCORES_URI, parsed(
        () -> configuration.getLong(
            YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES),
        (long) YarnConfiguration
            .DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        null, YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        "invalid-integer"));
    addCustomResourceBounds(minimum, maximum);

    boolean reservationsContinueLooking = configuration
        .getReservationContinueLook();
    int nodeLocalityDelay = parsed(configuration::getNodeLocalityDelay,
        CapacitySchedulerConfiguration.DEFAULT_NODE_LOCALITY_DELAY, null,
        CapacitySchedulerConfiguration.NODE_LOCALITY_DELAY,
        "invalid-integer");
    int rackLocalityAdditionalDelay = parsed(
        configuration::getRackLocalityAdditionalDelay,
        CapacitySchedulerConfiguration.DEFAULT_RACK_LOCALITY_ADDITIONAL_DELAY,
        null, CapacitySchedulerConfiguration.RACK_LOCALITY_ADDITIONAL_DELAY,
        "invalid-integer");
    boolean rackLocalityFullReset = configuration.getRackLocalityFullReset();
    return new CompiledSchedulerSettings(new ResourceSetting(minimum),
        new ResourceSetting(maximum), reservationsContinueLooking,
        nodeLocalityDelay, rackLocalityAdditionalDelay,
        rackLocalityFullReset);
  }

  private void addCustomResourceBounds(Map<String, Long> minimum,
      Map<String, Long> maximum) {
    for (String configuredName : model.getResourceTypes().split(",")) {
      String resourceName = normalizeResourceName(configuredName.trim());
      if (resourceName.isEmpty() || minimum.containsKey(resourceName)) {
        continue;
      }
      String prefix = YarnConfiguration.RESOURCE_TYPES + "."
          + configuredName.trim();
      minimum.put(resourceName, parsed(
          () -> configuration.getLong(prefix + ".minimum-allocation", 0L),
          0L, null, prefix + ".minimum-allocation", "invalid-integer"));
      maximum.put(resourceName, parsed(
          () -> configuration.getLong(prefix + ".maximum-allocation",
              Long.MAX_VALUE),
          Long.MAX_VALUE, null, prefix + ".maximum-allocation",
          "invalid-integer"));
    }
  }

  private ResourceSetting maximumAllocation(QueueConfigNode source,
      CompiledQueueSettings parent) {
    Map<String, Long> effective = new LinkedHashMap<>(parent == null
        ? schedulerSettings.getMaximumAllocation().getValues()
        : parent.getMaximumAllocation().getValues());
    String raw = source.getRawProperty(
        CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION);
    if (raw != null && !raw.trim().isEmpty()) {
      try {
        effective.putAll(parseResourceSetting(raw));
      } catch (RuntimeException failure) {
        diagnostic(source.getQueuePath(),
            fullKey(source.getQueuePath(),
                CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION),
            "invalid-resource", failure);
      }
      return new ResourceSetting(effective);
    }

    overlayLegacyMaximum(source, effective,
        CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION_MB,
        ResourceInformation.MEMORY_URI);
    overlayLegacyMaximum(source, effective,
        CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION_VCORES,
        ResourceInformation.VCORES_URI);
    return new ResourceSetting(effective);
  }

  private void overlayLegacyMaximum(QueueConfigNode source,
      Map<String, Long> effective, String property, String resourceName) {
    String raw = source.getRawProperty(property);
    if (raw == null) {
      return;
    }
    try {
      effective.put(resourceName, Long.parseLong(
          configuration.substituteCommonVariables(raw)));
    } catch (RuntimeException failure) {
      diagnostic(source.getQueuePath(), fullKey(source.getQueuePath(),
          property), "invalid-integer", failure);
    }
  }

  private Map<String, Long> parseResourceSetting(String configured) {
    Map<String, Long> parsed = new LinkedHashMap<>();
    String substituted = configuration.substituteCommonVariables(configured);
    for (String pair : substituted.split(",")) {
      String[] parts = pair.trim().split("=", -1);
      if (parts.length != 2 || parts[0].trim().isEmpty()) {
        throw new IllegalArgumentException("\"" + pair.trim()
            + "\" is not a valid resource type/amount pair. Please provide "
            + "key=amount pairs separated by commas.");
      }
      String resourceName = normalizeResourceName(parts[0].trim());
      Matcher matcher = RESOURCE_VALUE.matcher(parts[1].trim());
      if (!matcher.matches()) {
        throw new IllegalArgumentException("\"" + pair.trim()
            + "\" is not a valid resource type/amount pair. Please provide "
            + "key=amount pairs separated by commas.");
      }
      long value = Long.parseLong(matcher.group(1));
      String unit = matcher.group(2).toUpperCase(Locale.ROOT);
      if (ResourceInformation.MEMORY_URI.equals(resourceName)
          && "G".equals(unit)) {
        value = Math.multiplyExact(value, 1024L);
      }
      if (!unit.isEmpty() && !"M".equals(unit) && !"G".equals(unit)) {
        throw new IllegalArgumentException(
            "Acceptable units are M/G or empty");
      }
      parsed.put(resourceName, value);
    }
    return parsed;
  }

  private String normalizeResourceName(String configuredName) {
    if ("memory".equals(configuredName)
        || ResourceInformation.MEMORY_URI.equals(configuredName)) {
      return ResourceInformation.MEMORY_URI;
    }
    if ("vcore".equals(configuredName)
        || ResourceInformation.VCORES_URI.equals(configuredName)) {
      return ResourceInformation.VCORES_URI;
    }
    if ("gpu".equals(configuredName)) {
      return ResourceInformation.GPU_URI;
    }
    if ("fpga".equals(configuredName)) {
      return ResourceInformation.FPGA_URI;
    }
    return configuredName;
  }

  private Map<String, Float> userWeights(QueueConfigNode source,
      CompiledQueueSettings parent) {
    Map<String, Float> effective = new LinkedHashMap<>();
    if (parent != null) {
      effective.putAll(parent.getUserWeights());
    }
    String prefix = CapacitySchedulerConfiguration.USER_SETTINGS + ".";
    for (Map.Entry<String, String> entry
        : source.getRawProperties().entrySet()) {
      if (!entry.getKey().startsWith(prefix)) {
        continue;
      }
      String userProperty = entry.getKey().substring(prefix.length());
      Matcher matcher = CapacitySchedulerConfiguration.USER_WEIGHT_PATTERN
          .matcher(userProperty);
      if (!matcher.find()) {
        continue;
      }
      String userName = userProperty.replaceFirst("\\."
          + CapacitySchedulerConfiguration.USER_WEIGHT, "");
      if (userName.isEmpty()) {
        continue;
      }
      try {
        effective.put(userName, Float.parseFloat(
            configuration.substituteCommonVariables(entry.getValue())));
      } catch (RuntimeException failure) {
        diagnostic(source.getQueuePath(), fullKey(source.getQueuePath(),
            entry.getKey()), "invalid-float", failure);
      }
    }
    return effective;
  }

  private long defaultApplicationLifetime(QueuePath path,
      CompiledQueueSettings parent, long maximumLifetime,
      long configuredDefaultLifetime, boolean configuredInHierarchy) {
    if (path.isRoot()) {
      return configuredDefaultLifetime;
    }
    long defaultLifetime = configuredDefaultLifetime;
    if (defaultLifetime < 0) {
      defaultLifetime = configuredInHierarchy
          ? Math.min(parent.getDefaultApplicationLifetime(), maximumLifetime)
          : maximumLifetime;
    }
    return defaultLifetime <= 0 ? maximumLifetime : defaultLifetime;
  }

  private long queueLong(QueueConfigNode source, String property,
      long fallback) {
    String raw = source.getRawProperty(property);
    if (raw == null) {
      return fallback;
    }
    try {
      return Long.parseLong(configuration.substituteCommonVariables(raw));
    } catch (RuntimeException failure) {
      diagnostic(source.getQueuePath(), fullKey(source.getQueuePath(),
          property), "invalid-integer", failure);
      return fallback;
    }
  }

  private String multiNodePolicyClass(QueueConfigNode source) {
    String queuePolicy = source.getRawProperty("multi-node-sorting.policy");
    String policyName = queuePolicy == null
        ? configuration.get(
            CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME)
        : queuePolicy;
    if (policyName == null || policyName.trim().isEmpty()) {
      return null;
    }
    String classKey = CapacitySchedulerConfiguration
        .MULTI_NODE_SORTING_POLICY_NAME + "." + policyName.trim() + ".class";
    String className = configuration.get(classKey);
    if (className == null || className.trim().isEmpty()) {
      diagnostics.add(new ConfigDiagnostic(source.getQueuePath(), classKey,
          "queue-tree-build", policyName.trim()
              + " Class is not configured or not an instance of "
              + "org.apache.hadoop.yarn.server.resourcemanager.scheduler"
              + ".placement.MultiNodeLookupPolicy"));
      return null;
    }
    return className.trim();
  }

  private Map<String, Float> maximumAmShares(QueueConfigNode source,
      Iterable<String> configuredLabels) {
    Map<String, Float> values = new LinkedHashMap<>();
    for (String label : configuredLabels) {
      String suffix = label.isEmpty()
          ? CapacitySchedulerConfiguration.MAXIMUM_AM_RESOURCE_SUFFIX
          : CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS + "."
              + label + "."
              + CapacitySchedulerConfiguration.MAXIMUM_AM_RESOURCE_SUFFIX;
      String raw = source.getRawProperty(suffix);
      float fallback = source.getMaximumApplicationMasterShare();
      if (raw == null) {
        values.put(label, fallback);
        continue;
      }
      try {
        values.put(label, Float.parseFloat(
            configuration.substituteCommonVariables(raw)));
      } catch (RuntimeException failure) {
        diagnostic(source.getQueuePath(), fullKey(source.getQueuePath(),
            suffix), "invalid-float", failure);
        values.put(label, fallback);
      }
    }
    return values;
  }

  private DynamicQueueSettings dynamicSettings(QueuePath path) {
    boolean failWhenExceeded = configuration
        .getShouldFailAutoQueueCreationWhenGuaranteedCapacityExceeded(path);
    int legacyMax = parsed(
        () -> configuration.getAutoCreatedQueuesMaxChildQueuesLimit(path),
        CapacitySchedulerConfiguration.DEFAULT_AUTO_CREATE_QUEUE_MAX_QUEUES,
        path, fullKey(path,
            CapacitySchedulerConfiguration.AUTO_CREATE_QUEUE_MAX_QUEUES),
        "invalid-integer");
    int flexibleMax = parsed(
        () -> configuration.getAutoCreatedQueuesV2MaxChildQueuesLimit(path),
        CapacitySchedulerConfiguration
            .DEFAULT_AUTO_QUEUE_CREATION_V2_MAX_QUEUES,
        path, fullKey(path,
            CapacitySchedulerConfiguration.AUTO_QUEUE_CREATION_V2_MAX_QUEUES),
        "invalid-integer");
    String managementPolicy = sourceValue(path,
        CapacitySchedulerConfiguration.AUTO_CREATED_QUEUE_MANAGEMENT_POLICY,
        CapacitySchedulerConfiguration
            .DEFAULT_AUTO_CREATED_QUEUE_MANAGEMENT_POLICY);
    return new DynamicQueueSettings(failWhenExceeded, legacyMax, flexibleMax,
        managementPolicy, model.getCommonTemplateProperties(path),
        model.getLeafTemplateProperties(path),
        model.getParentTemplateProperties(path));
  }

  private String sourceValue(QueuePath path, String suffix,
      String fallback) {
    return configuration.get(fullKey(path, suffix), fallback).trim();
  }

  private <T> T parsed(Supplier<T> parser, T fallback, QueuePath path,
      String propertyKey, String code) {
    try {
      return parser.get();
    } catch (RuntimeException failure) {
      diagnostic(path, propertyKey, code, failure);
      return fallback;
    }
  }

  private void diagnostic(QueuePath path, String propertyKey, String code,
      RuntimeException failure) {
    String message = failure.getMessage() == null
        ? failure.getClass().getSimpleName() : failure.getMessage();
    diagnostics.add(new ConfigDiagnostic(path, propertyKey, code, message));
  }

  private String fullKey(QueuePath path, String suffix) {
    return QueuePrefixes.getQueuePrefix(path) + suffix;
  }
}
