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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueAppLifetimeAndLimitSettings;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueNodeLabelsSettings;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueStateHelper;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.CSConfigModel;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.ConfigDiagnostic;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.QueueConfigNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.ApplicationSettings;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.CalculatorSemantics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.CapacitySetting;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.DynamicQueueSettings;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.DiagnosticStage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.FactsSnapshot;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.OldQueueKind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.OldQueueSnapshot;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.PlanDiagnostic;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.PlanIdentity;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.PlacementRuleDefinition;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.PolicyDescriptor;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.QueueKind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.QueueListDefinition;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.QueuePlanNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.QueueSettings;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.ReservationSettings;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.ResourceValues;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.SchedulerSettings;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.SchedulingSettings;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.TemplateSettings;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;

/** Compiles one eligible model and facts snapshot into immutable values. */
final class QueuePlanCompiler {
  private static final List<String> RESOURCE_NAMES = List.of(
      ResourceInformation.MEMORY_URI, ResourceInformation.VCORES_URI);
  private static final String DEFAULT_MANAGEMENT_POLICY =
      "org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity."
          + "queuemanagement.GuaranteedOrZeroCapacityOverTimePolicy";
  private static final String PARENT_POLICY_CLASS =
      "org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity."
          + "policy.PriorityUtilizationQueueOrderingPolicy";

  private final CSConfigModel model;
  private final List<PlanDiagnostic> diagnostics = new ArrayList<>();
  private final FactsSnapshot facts;
  private final SchedulerSettings schedulerSettings;
  private final Map<String, QueuePlanNode> queues = new LinkedHashMap<>();
  private final boolean systemPreemptionEnabled;
  private final boolean systemIntraQueuePreemptionEnabled;

  private QueuePlanCompiler(CSConfigModel model, ClusterFacts facts) {
    this.model = model;
    this.facts = normalizeFacts(facts);
    this.schedulerSettings = schedulerSettings();
    this.systemPreemptionEnabled = configurationBoolean(
        YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ENABLE_MONITORS);
    this.systemIntraQueuePreemptionEnabled = configurationBoolean(
        CapacitySchedulerConfiguration.INTRAQUEUE_PREEMPTION_ENABLED,
        CapacitySchedulerConfiguration.DEFAULT_INTRAQUEUE_PREEMPTION_ENABLED);
    model.getDiagnostics().forEach(this::addDiagnostic);
  }

  static ValidatedQueuePlan compile(CSConfigModel model, ClusterFacts facts) {
    QueuePlanCompiler compiler = new QueuePlanCompiler(model, facts);
    QueuePlanNode root = compiler.compileNode(model.getRoot(), null);
    PlanIdentity identity = new PlanIdentity(candidateFingerprint(model),
        factsFingerprint(compiler.facts));
    return new ValidatedQueuePlan(identity, root, compiler.queues,
        compiler.facts, compiler.schedulerSettings, compiler.diagnostics);
  }

  static String candidateFingerprint(CSConfigModel model) {
    MessageDigest digest = sha256();
    model.getRawProperties().entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .forEach(entry -> update(digest, entry.getKey(), entry.getValue()));
    return hex(digest.digest());
  }

  static String factsFingerprint(FactsSnapshot facts) {
    MessageDigest digest = sha256();
    update(digest, "calculator", facts.calculatorSemantics().name());
    facts.resourceNames().forEach(resource ->
        update(digest, "resourceName", resource));
    facts.resourceUnits().entrySet().stream()
        .sorted(Map.Entry.comparingByKey()).forEach(entry ->
            update(digest, "resourceUnit:" + entry.getKey(),
                entry.getValue()));
    updateResources(digest, "cluster", facts.clusterResource());
    updateResources(digest, "minimum", facts.minimumAllocation());
    updateResources(digest, "maximum", facts.maximumAllocation());
    facts.nodeLabels().stream().sorted().forEach(label ->
        update(digest, "label", label));
    facts.resourcesByLabel().entrySet().stream()
        .sorted(Map.Entry.comparingByKey()).forEach(entry ->
            updateResources(digest, "partition:" + entry.getKey(),
                entry.getValue()));
    facts.oldHierarchy().entrySet().stream()
        .sorted(Map.Entry.comparingByKey()).forEach(entry -> {
          OldQueueSnapshot queue = entry.getValue();
          update(digest, "old:" + entry.getKey(), queue.kind().name()
              + ":" + queue.state() + ":" + queue.dynamic()
              + ":" + queue.autoCreatedLeaf());
        });
    update(digest, "skipHierarchy",
        Boolean.toString(facts.hierarchyValidationSkipped()));
    return hex(digest.digest());
  }

  private QueuePlanNode compileNode(QueueConfigNode source,
      QueuePlanNode parent) {
    String path = source.getQueuePath().getFullPath();
    Set<String> labels = new LinkedHashSet<>(source.getConfiguredNodeLabels());
    labels.add(RMNodeLabelsManager.NO_LABEL);
    if (source.getQueuePath().isRoot()) {
      labels.addAll(model.getConfiguredNodeLabels(source.getQueuePath()));
    }

    Set<String> accessibleLabels;
    String defaultExpression;
    try {
      QueueNodeLabelsSettings.ResolvedNodeLabels resolved =
          QueueNodeLabelsSettings.resolve(path,
              source.getQueuePath().isRoot(),
              source.getAccessibleNodeLabels(),
              source.getDefaultNodeLabelExpression(), labels,
              parent == null ? null : parent.accessibleNodeLabels(),
              parent == null ? null : parent.defaultNodeLabelExpression());
      accessibleLabels = resolved.accessibleLabels();
      defaultExpression = resolved.defaultLabelExpression();
    } catch (Exception failure) {
      diagnostic(path, null, "queue-tree-build", failure);
      accessibleLabels = source.getAccessibleNodeLabels() == null
          ? parent == null ? Collections.emptySet()
              : parent.accessibleNodeLabels()
          : source.getAccessibleNodeLabels();
      defaultExpression = source.getDefaultNodeLabelExpression();
    }

    QueueState initialState;
    try {
      initialState = QueueStateHelper.resolveInitialState(path,
          parent == null ? null : parent.queuePath(), source.getState(),
          parent == null ? null : parent.initialState()).state();
    } catch (RuntimeException failure) {
      diagnostic(path, fullKey(source, CapacitySchedulerConfiguration.STATE),
          "queue-tree-build", failure);
      initialState = source.getState() == null
          ? parent == null ? QueueState.RUNNING : parent.initialState()
          : source.getState();
    }

    Map<String, CapacitySetting> capacities = new LinkedHashMap<>();
    Map<String, CapacitySetting> maximumCapacities = new LinkedHashMap<>();
    for (String label : labels) {
      capacities.put(label, capacitySetting(source.getCapacity(label),
          source.getQueuePath().isRoot()));
      maximumCapacities.put(label, capacitySetting(
          source.getMaximumCapacity(label), source.getQueuePath().isRoot()));
    }

    QueueSettings settings = compileSettings(source, parent, labels);
    List<String> childPaths = source.getChildren().values().stream()
        .map(child -> child.getQueuePath().getFullPath()).toList();
    QueuePlanNode node = new QueuePlanNode(path,
        parent == null ? null : parent.queuePath(), childPaths, kind(source),
        capacities, maximumCapacities, labels, accessibleLabels,
        source.getAccessibleNodeLabels() != null, source.getState(),
        initialState, defaultExpression, settings);
    queues.put(path, node);
    for (QueueConfigNode child : source.getChildren().values()) {
      compileNode(child, node);
    }
    return node;
  }

  private QueueSettings compileSettings(QueueConfigNode source,
      QueuePlanNode parent, Set<String> labels) {
    ResourceValues inheritedMaximum = parent == null
        ? schedulerSettings.maximumAllocation()
        : parent.settings().maximumAllocation();
    ResourceValues maximumAllocation = maximumAllocation(source,
        inheritedMaximum);
    Map<String, Float> userWeights = parent == null
        ? new HashMap<>() : new HashMap<>(parent.settings().userWeights());
    userWeights.putAll(userWeights(source));

    long configuredMaximum = queueLong(source,
        CapacitySchedulerConfiguration.MAXIMUM_LIFETIME_SUFFIX, -1L);
    long configuredDefault = queueLong(source,
        CapacitySchedulerConfiguration.DEFAULT_LIFETIME_SUFFIX, -1L);
    QueueAppLifetimeAndLimitSettings.LifetimeSettings lifetimes;
    try {
      lifetimes = QueueAppLifetimeAndLimitSettings.resolve(
          source.getQueuePath().isRoot(), configuredMaximum,
          configuredDefault,
          parent == null ? -1L
              : parent.settings().applications().maximumLifetime(),
          parent == null ? -1L
              : parent.settings().applications().defaultLifetime(),
          parent != null && parent.settings().applications()
              .defaultLifetimeConfigured());
    } catch (RuntimeException failure) {
      diagnostic(source.getQueuePath().getFullPath(), null,
          "queue-tree-build", failure);
      lifetimes = new QueueAppLifetimeAndLimitSettings.LifetimeSettings(
          configuredMaximum, configuredDefault, configuredDefault >= 0);
    }

    int maximumParallel = maximumParallelApplications(source);
    int defaultPriority = queueInt(source,
        CapacitySchedulerConfiguration.DEFAULT_APPLICATION_PRIORITY,
        CapacitySchedulerConfiguration.DEFAULT_CONFIGURATION_APPLICATION_PRIORITY);
    Map<String, Float> maximumAmShares = new LinkedHashMap<>();
    for (String label : labels) {
      String suffix = label.isEmpty()
          ? CapacitySchedulerConfiguration.MAXIMUM_AM_RESOURCE_SUFFIX
          : CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS + "."
              + label + "."
              + CapacitySchedulerConfiguration.MAXIMUM_AM_RESOURCE_SUFFIX;
      maximumAmShares.put(label, queueFloat(source, suffix,
          source.getMaximumApplicationMasterShare()));
    }
    ApplicationSettings applications = new ApplicationSettings(
        source.getMaximumApplications(),
        source.getMaximumApplicationMasterShare(), source.getUserLimit(),
        source.getUserLimitFactor(), lifetimes.maximumLifetime(),
        lifetimes.defaultLifetime(), lifetimes.defaultLifetimeConfigured(),
        maximumParallel, defaultPriority, maximumAmShares);

    boolean parentPreemption = parent != null
        && parent.settings().scheduling().preemptionDisabled();
    boolean preemptionDisabled = !systemPreemptionEnabled
        || queueBoolean(source,
            CapacitySchedulerConfiguration.QUEUE_PREEMPTION_DISABLED,
            parentPreemption);
    boolean parentIntra = parent != null
        && parent.settings().scheduling().intraQueuePreemptionDisabled();
    boolean intraDisabled = !systemIntraQueuePreemptionEnabled
        || queueBoolean(source,
            "intra-queue-preemption.disable_preemption", parentIntra);
    SchedulingSettings scheduling = new SchedulingSettings(
        preemptionDisabled, intraDisabled, queueInt(source, "priority", 0),
        queueBoolean(source,
            CapacitySchedulerConfiguration.ALLOW_ZERO_CAPACITY_SUM, false));

    boolean parentQueue = !source.getChildren().isEmpty()
        || source.isAutoCreateChildQueueEnabled()
        || source.isAutoQueueCreationV2Enabled();
    PolicyDescriptor appOrdering = applicationPolicy(source);
    PolicyDescriptor parentOrdering = parentPolicy(source, parent,
        parentQueue);
    DynamicQueueSettings dynamic = dynamicSettings(source);
    ReservationSettings reservations = reservationSettings(source);
    String multiNode = multiNodePolicy(source);
    return new QueueSettings(maximumAllocation, userWeights, applications,
        scheduling, appOrdering, parentOrdering, multiNode,
        source.getAclProperties(), dynamic, reservations);
  }

  private SchedulerSettings schedulerSettings() {
    long minimumMemory = configuredInt(
        YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    long minimumVcores = configuredInt(
        YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
    long maximumMemory = configuredInt(
        YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);
    long maximumVcores = configuredInt(
        YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);
    ResourceValues minimum = resources(minimumMemory, minimumVcores);
    ResourceValues maximum = resources(maximumMemory, maximumVcores);
    int clusterMaximumPriority = clusterMaximumPriority();
    ResourceValues ruleMinimum = resources(
        ruleValue(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
            minimumMemory, facts.minimumAllocation().value(
                ResourceInformation.MEMORY_URI),
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB),
        ruleValue(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
            minimumVcores, facts.minimumAllocation().value(
                ResourceInformation.VCORES_URI),
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES));
    ResourceValues ruleMaximum = resources(
        ruleValue(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
            maximumMemory, facts.maximumAllocation().value(
                ResourceInformation.MEMORY_URI),
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB),
        ruleValue(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
            maximumVcores, facts.maximumAllocation().value(
                ResourceInformation.VCORES_URI),
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES));
    return new SchedulerSettings(model.isLegacyQueueMode(), minimum, maximum,
        ruleMinimum, ruleMaximum, model.getMappingRuleFormat(),
        rawPlacementRules(), placementRules(),
        model.getMaximumAutoCreatedQueueDepth(), queueLists(),
        transitionStates(), clusterMaximumPriority);
  }

  private int clusterMaximumPriority() {
    String property = YarnConfiguration.MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY;
    String raw = model.getRawProperties().get(property);
    if (raw != null) {
      try {
        return ConfigurationValueParsers.parseInt(raw);
      } catch (NumberFormatException failure) {
        diagnostic(null, property, "queue-tree-build", failure);
      }
    }
    return YarnConfiguration.DEFAULT_CLUSTER_LEVEL_APPLICATION_PRIORITY;
  }

  private int maximumParallelApplications(QueueConfigNode node) {
    String suffix = CapacitySchedulerConfiguration.MAX_PARALLEL_APPLICATIONS;
    String raw = node.getRawProperty(suffix);
    if (raw != null) {
      try {
        // This getter deliberately uses decimal, untrimmed Integer.valueOf,
        // unlike Configuration.getInt used by the other queue limits.
        return Integer.parseInt(raw);
      } catch (NumberFormatException failure) {
        diagnostic(node.getQueuePath().getFullPath(), fullKey(node, suffix),
            "invalid-integer", failure);
      }
    }
    return CapacitySchedulerConfiguration.DEFAULT_MAX_PARALLEL_APPLICATIONS;
  }

  private ResourceValues resources(long memory, long vcores) {
    return new ResourceValues(Map.of(ResourceInformation.MEMORY_URI, memory,
        ResourceInformation.VCORES_URI, vcores));
  }

  private long ruleValue(String property, long configuredValue,
      long factsValue, long defaultValue) {
    return model.getRawProperties().containsKey(property) ? configuredValue
        : positiveOrDefault(factsValue, defaultValue);
  }

  private List<String> rawPlacementRules() {
    String configured = model.getRawProperties().get(
        CapacitySchedulerConfiguration.QUEUE_MAPPING);
    if (configured == null || configured.trim().isEmpty()) {
      return List.of();
    }
    List<String> result = new ArrayList<>();
    for (String rawRule : configured.split(",")) {
      result.add(rawRule.trim());
    }
    return result;
  }

  private Map<String, QueueState> transitionStates() {
    Map<String, QueueState> result = new LinkedHashMap<>();
    for (String path : facts.oldHierarchy().keySet()) {
      String raw = model.getRawProperties().get(
          QueuePrefixes.getQueuePrefix(new QueuePath(path))
              + CapacitySchedulerConfiguration.STATE);
      if (raw != null) {
        try {
          result.put(path, QueueState.valueOf(raw));
        } catch (IllegalArgumentException ignored) {
          // The legacy transition rule also ignores non-case-exact values.
        }
      }
    }
    return result;
  }

  private List<QueueListDefinition> queueLists() {
    List<QueueListDefinition> result = new ArrayList<>();
    model.getRawProperties().entrySet().stream()
        .filter(entry -> entry.getKey().startsWith(
            CapacitySchedulerConfiguration.PREFIX)
            && entry.getKey().endsWith("."
                + CapacitySchedulerConfiguration.QUEUES))
        .sorted(Map.Entry.comparingByKey())
        .forEach(entry -> result.add(new QueueListDefinition(entry.getKey(),
            List.of(entry.getValue().split(",", -1)))));
    return result;
  }

  private List<PlacementRuleDefinition> placementRules() {
    String configured = model.getRawProperties().get(
        CapacitySchedulerConfiguration.QUEUE_MAPPING);
    if (configured == null || configured.trim().isEmpty()) {
      return List.of();
    }
    List<PlacementRuleDefinition> rules = new ArrayList<>();
    for (String rawRule : configured.trim().split("\\s*[,\\n]\\s*")) {
      if (rawRule.isEmpty()) {
        continue;
      }
      String[] parts = rawRule.trim().split(":", -1);
      if (parts.length == 3
          && ("u".equals(parts[0].trim())
              || "g".equals(parts[0].trim()))
          && !parts[1].trim().isEmpty()
          && !parts[2].trim().isEmpty()) {
        rules.add(new PlacementRuleDefinition(rawRule.trim(), parts[0].trim(),
            parts[1].trim(), parts[2].trim()));
      } else {
        throw new IllegalArgumentException(
            "Unsupported legacy queue mapping " + rawRule.trim());
      }
    }
    return rules;
  }

  private ResourceValues maximumAllocation(QueueConfigNode source,
      ResourceValues inherited) {
    Map<String, Long> values = new LinkedHashMap<>(inherited.values());
    String raw = source.getRawProperty(
        CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION);
    if (raw != null && !raw.trim().isEmpty()) {
      try {
        values = new LinkedHashMap<>(
            BuiltInResourceValueParser.parseQueueMaximum(raw));
      } catch (RuntimeException failure) {
        diagnostic(source.getQueuePath().getFullPath(),
            fullKey(source, CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION),
            "invalid-resource", failure);
      }
      return new ResourceValues(values);
    }
    overlayLong(source, values,
        CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION_MB,
        ResourceInformation.MEMORY_URI);
    overlayLong(source, values,
        CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION_VCORES,
        ResourceInformation.VCORES_URI);
    return new ResourceValues(values);
  }

  private Map<String, Float> userWeights(QueueConfigNode source) {
    Map<String, Float> values = new LinkedHashMap<>();
    String prefix = CapacitySchedulerConfiguration.USER_SETTINGS + ".";
    source.getRawProperties().forEach((suffix, value) -> {
      if (!suffix.startsWith(prefix) || !suffix.endsWith("."
          + CapacitySchedulerConfiguration.USER_WEIGHT)) {
        return;
      }
      String user = suffix.substring(prefix.length(), suffix.length()
          - CapacitySchedulerConfiguration.USER_WEIGHT.length() - 1);
      if (user.isEmpty()) {
        return;
      }
      try {
        values.put(user, Float.parseFloat(value.trim()));
      } catch (NumberFormatException failure) {
        diagnostic(source.getQueuePath().getFullPath(), fullKey(source, suffix),
            "invalid-float", failure);
      }
    });
    return values;
  }

  private DynamicQueueSettings dynamicSettings(QueueConfigNode source) {
    boolean legacyEnabled = source.isAutoCreateChildQueueEnabled();
    boolean flexibleEnabled = source.isAutoQueueCreationV2Enabled();
    String managementName = source.getRawProperty(
        "auto-create-child-queue.management-policy");
    if (managementName == null || managementName.trim().isEmpty()) {
      managementName = DEFAULT_MANAGEMENT_POLICY;
    }
    PolicyDescriptor management = new PolicyDescriptor(managementName.trim(),
        managementName.trim(), Map.of());
    Map<String, String> legacy = propertiesWithPrefix(source,
        CapacitySchedulerConfiguration.AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX
            + ".");
    return new DynamicQueueSettings(legacyEnabled, flexibleEnabled,
        legacyEnabled && queueBoolean(source,
            CapacitySchedulerConfiguration
                .FAIL_AUTO_CREATION_ON_EXCEEDING_CAPACITY,
            false), legacyEnabled ? queueInt(source,
                CapacitySchedulerConfiguration.AUTO_CREATE_QUEUE_MAX_QUEUES,
                1000) : 1000, flexibleEnabled ? queueInt(source,
                CapacitySchedulerConfiguration.AUTO_QUEUE_CREATION_V2_MAX_QUEUES,
                1000) : 1000, flexibleEnabled ? queueInt(source,
                CapacitySchedulerConfiguration.MAXIMUM_QUEUE_DEPTH,
                schedulerSettings.maximumAutoCreatedQueueDepth())
            : schedulerSettings.maximumAutoCreatedQueueDepth(), management,
        templateSettings(legacy, false), templateSettings(
            model.getCommonTemplateProperties(source.getQueuePath()), null),
        templateSettings(model.getLeafTemplateProperties(
            source.getQueuePath()), false),
        templateSettings(model.getParentTemplateProperties(
            source.getQueuePath()), true));
  }

  private TemplateSettings templateSettings(Map<String, String> properties,
      Boolean parentTemplate) {
    Map<String, CapacitySetting> capacities = new LinkedHashMap<>();
    Map<String, CapacitySetting> maximums = new LinkedHashMap<>();
    Integer maximumApplications = null;
    PolicyDescriptor ordering = null;
    for (Map.Entry<String, String> property : properties.entrySet().stream()
        .sorted(Map.Entry.comparingByKey()).toList()) {
      String suffix = property.getKey();
      String value = property.getValue();
      String label = templateCapacityLabel(suffix,
          CapacitySchedulerConfiguration.CAPACITY);
      if (label != null) {
        capacities.put(label, capacitySetting(value));
        continue;
      }
      label = templateCapacityLabel(suffix,
          CapacitySchedulerConfiguration.MAXIMUM_CAPACITY);
      if (label != null) {
        maximums.put(label, capacitySetting(value));
        continue;
      }
      if (suffix.equals(
          CapacitySchedulerConfiguration.MAXIMUM_APPLICATIONS_SUFFIX)) {
        maximumApplications = ConfigurationValueParsers.parseInt(value);
      } else if (suffix.equals(
          CapacitySchedulerConfiguration.ORDERING_POLICY)) {
        ordering = parentTemplate != null && parentTemplate
            ? parentTemplatePolicy(value) : applicationTemplatePolicy(value);
      }
    }
    return new TemplateSettings(capacities, maximums, maximumApplications,
        ordering);
  }

  private String templateCapacityLabel(String suffix, String property) {
    if (suffix.equals(property)) {
      return RMNodeLabelsManager.NO_LABEL;
    }
    String prefix = CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS
        + ".";
    String ending = "." + property;
    if (suffix.startsWith(prefix) && suffix.endsWith(ending)) {
      return suffix.substring(prefix.length(),
          suffix.length() - ending.length());
    }
    return null;
  }

  private CapacitySetting capacitySetting(String raw) {
    return BuiltInCapacityParser.parse(raw, false);
  }

  private PolicyDescriptor applicationTemplatePolicy(String raw) {
    return applicationPolicy(raw, Map.of());
  }

  private PolicyDescriptor parentTemplatePolicy(String raw) {
    String configured = raw.trim();
    return new PolicyDescriptor(configured, PARENT_POLICY_CLASS,
        configured.equals(CapacitySchedulerConfiguration
            .QUEUE_PRIORITY_UTILIZATION_ORDERING_POLICY)
            ? Map.of("respect-priority", "true")
            : Map.of("respect-priority", "false"));
  }

  private ReservationSettings reservationSettings(QueueConfigNode source) {
    Map<String, String> values = new LinkedHashMap<>();
    for (String suffix : List.of(CapacitySchedulerConfiguration.IS_RESERVABLE,
        CapacitySchedulerConfiguration.RESERVATION_WINDOW,
        CapacitySchedulerConfiguration.RESERVATION_ADMISSION_POLICY,
        CapacitySchedulerConfiguration.RESERVATION_AGENT_NAME,
        CapacitySchedulerConfiguration.RESERVATION_SHOW_RESERVATION_AS_QUEUE,
        CapacitySchedulerConfiguration.RESERVATION_PLANNER_NAME,
        CapacitySchedulerConfiguration.RESERVATION_MOVE_ON_EXPIRY,
        CapacitySchedulerConfiguration.RESERVATION_ENFORCEMENT_WINDOW)) {
      if (source.getRawProperty(suffix) != null) {
        values.put(suffix, source.getRawProperty(suffix));
      }
    }
    return new ReservationSettings(Boolean.parseBoolean(values.getOrDefault(
        CapacitySchedulerConfiguration.IS_RESERVABLE, "false")), values);
  }

  private PolicyDescriptor applicationPolicy(QueueConfigNode source) {
    return applicationPolicy(source.getOrderingPolicy(),
        source.getOrderingPolicyParameters());
  }

  private PolicyDescriptor applicationPolicy(String raw,
      Map<String, String> parameters) {
    String alias = raw.trim();
    String configured = switch (alias) {
    case CapacitySchedulerConfiguration.FIFO_APP_ORDERING_POLICY,
        CapacitySchedulerConfiguration.FAIR_APP_ORDERING_POLICY,
        CapacitySchedulerConfiguration.FIFO_WITH_PARTITIONS_APP_ORDERING_POLICY,
        CapacitySchedulerConfiguration.FIFO_FOR_PENDING_APPS -> alias;
    default -> raw;
    };
    String canonical = switch (configured) {
    case CapacitySchedulerConfiguration.FIFO_APP_ORDERING_POLICY ->
        "org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy."
            + "FifoOrderingPolicy";
    case CapacitySchedulerConfiguration.FAIR_APP_ORDERING_POLICY ->
        "org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy."
            + "FairOrderingPolicy";
    case CapacitySchedulerConfiguration.FIFO_WITH_PARTITIONS_APP_ORDERING_POLICY ->
        "org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy."
            + "FifoOrderingPolicyWithExclusivePartitions";
    case CapacitySchedulerConfiguration.FIFO_FOR_PENDING_APPS ->
        "org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy."
            + "FifoOrderingPolicyForPendingApps";
    default -> configured;
    };
    return new PolicyDescriptor(configured, canonical, parameters);
  }

  private PolicyDescriptor parentPolicy(QueueConfigNode source,
      QueuePlanNode parent, boolean parentQueue) {
    String configured = source.getRawProperty(
        CapacitySchedulerConfiguration.ORDERING_POLICY);
    if (!parentQueue || configured == null) {
      if (parent != null) {
        return parent.settings().parentOrdering();
      }
      configured = CapacitySchedulerConfiguration.DEFAULT_QUEUE_ORDERING_POLICY;
    }
    configured = configured.trim();
    Map<String, String> parameters = configured.equals(
        CapacitySchedulerConfiguration.QUEUE_PRIORITY_UTILIZATION_ORDERING_POLICY)
        ? Map.of("respect-priority", "true")
        : Map.of("respect-priority", "false");
    return new PolicyDescriptor(configured, PARENT_POLICY_CLASS, parameters);
  }

  private String multiNodePolicy(QueueConfigNode source) {
    String name = source.getRawProperty("multi-node-sorting.policy");
    if (name == null) {
      name = model.getRawProperties().get(
          CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME);
    }
    if (name == null || name.trim().isEmpty()) {
      return null;
    }
    return model.getRawProperties().get(CapacitySchedulerConfiguration
        .MULTI_NODE_SORTING_POLICY_NAME + "." + name.trim() + ".class");
  }

  private CapacitySetting capacitySetting(QueueConfigNode.CapacityValue value,
      boolean root) {
    String raw = value == null ? null : value.getRawValue();
    return BuiltInCapacityParser.parse(raw, root);
  }

  private QueueKind kind(QueueConfigNode node) {
    if (Boolean.parseBoolean(node.getRawProperty(
        CapacitySchedulerConfiguration.IS_RESERVABLE))
        && node.getChildren().isEmpty()
        && !node.isAutoCreateChildQueueEnabled()
        && !node.isAutoQueueCreationV2Enabled()) {
      return QueueKind.PLAN;
    }
    if (node.isAutoCreateChildQueueEnabled()) {
      return QueueKind.MANAGED_PARENT;
    }
    if (!node.getChildren().isEmpty() || node.isAutoQueueCreationV2Enabled()) {
      return QueueKind.PARENT;
    }
    return QueueKind.LEAF;
  }

  private FactsSnapshot normalizeFacts(ClusterFacts sourceFacts) {
    Map<String, ResourceValues> resourcesByLabel = new LinkedHashMap<>();
    sourceFacts.getResourceValuesByLabel().entrySet().stream()
        .sorted(Map.Entry.comparingByKey()).forEach(entry ->
            resourcesByLabel.put(entry.getKey(),
                new ResourceValues(entry.getValue())));
    resourcesByLabel.putIfAbsent(RMNodeLabelsManager.NO_LABEL,
        new ResourceValues(sourceFacts.getClusterResourceValues()));
    Map<String, OldQueueSnapshot> oldHierarchy = new LinkedHashMap<>();
    sourceFacts.getOldHierarchy().entrySet().stream()
        .sorted(Comparator.comparing(entry -> entry.getKey().getFullPath()))
        .forEach(entry -> oldHierarchy.put(entry.getKey().getFullPath(),
            oldQueue(entry.getValue())));
    CalculatorSemantics calculator = DefaultResourceCalculator.class.getName()
        .equals(sourceFacts.getResourceCalculatorClassName())
        ? CalculatorSemantics.DEFAULT : CalculatorSemantics.DOMINANT;
    Map<String, String> resourceUnits = new LinkedHashMap<>();
    for (String resourceName : RESOURCE_NAMES) {
      resourceUnits.put(resourceName,
          sourceFacts.getResourceUnits().getOrDefault(resourceName, ""));
    }
    return new FactsSnapshot(
        new ResourceValues(sourceFacts.getClusterResourceValues()),
        new ResourceValues(sourceFacts.getMinimumAllocationValues()),
        new ResourceValues(sourceFacts.getMaximumAllocationValues()),
        sourceFacts.getNodeLabels(),
        resourcesByLabel, oldHierarchy,
        sourceFacts.isHierarchyValidationSkipped(), calculator, RESOURCE_NAMES,
        resourceUnits);
  }

  private OldQueueSnapshot oldQueue(ClusterFacts.OldQueue old) {
    OldQueueKind kind = switch (old.getKind()) {
    case LEAF -> OldQueueKind.LEAF;
    case PARENT -> OldQueueKind.PARENT;
    case MANAGED_PARENT -> OldQueueKind.MANAGED_PARENT;
    case PLAN -> OldQueueKind.PLAN;
    case RESERVATION -> OldQueueKind.RESERVATION;
    };
    QueueState state = old.getState() == null
        ? QueueState.RUNNING : old.getState();
    return new OldQueueSnapshot(kind, state, old.isDynamic(),
        old.isAutoCreatedLeaf());
  }

  private long configuredInt(String key, long fallback) {
    String raw = model.getRawProperties().get(key);
    if (raw == null) {
      return fallback;
    }
    try {
      return ConfigurationValueParsers.parseInt(raw);
    } catch (NumberFormatException failure) {
      diagnostics.add(new PlanDiagnostic(DiagnosticStage.MODEL, null, key,
          key.contains("vcores") ? "vcores-allocation"
              : "memory-allocation",
          "Invalid integer value '" + raw + "' for " + key));
      return fallback;
    }
  }

  private long positiveOrDefault(long value, long defaultValue) {
    return value > 0L ? value : defaultValue;
  }

  private long queueLong(QueueConfigNode node, String suffix, long fallback) {
    String raw = node.getRawProperty(suffix);
    if (raw == null) {
      return fallback;
    }
    try {
      return ConfigurationValueParsers.parseLong(raw);
    } catch (NumberFormatException failure) {
      diagnostic(node.getQueuePath().getFullPath(), fullKey(node, suffix),
          "invalid-integer", failure);
      return fallback;
    }
  }

  private int queueInt(QueueConfigNode node, String suffix, int fallback) {
    String raw = node.getRawProperty(suffix);
    if (raw == null) {
      return fallback;
    }
    try {
      return ConfigurationValueParsers.parseInt(raw);
    } catch (NumberFormatException failure) {
      diagnostic(node.getQueuePath().getFullPath(), fullKey(node, suffix),
          "invalid-integer", failure);
      return fallback;
    }
  }

  private float queueFloat(QueueConfigNode node, String suffix,
      float fallback) {
    String raw = node.getRawProperty(suffix);
    if (raw == null) {
      return fallback;
    }
    try {
      return Float.parseFloat(raw.trim());
    } catch (NumberFormatException failure) {
      diagnostic(node.getQueuePath().getFullPath(), fullKey(node, suffix),
          "invalid-float", failure);
      return fallback;
    }
  }

  private boolean queueBoolean(QueueConfigNode node, String suffix,
      boolean fallback) {
    String raw = node.getRawProperty(suffix);
    return ConfigurationValueParsers.parseBoolean(raw, fallback);
  }

  private boolean configurationBoolean(String key, boolean fallback) {
    return ConfigurationValueParsers.parseBoolean(
        model.getRawProperties().get(key), fallback);
  }

  private void overlayLong(QueueConfigNode node, Map<String, Long> values,
      String suffix, String resourceName) {
    String raw = node.getRawProperty(suffix);
    if (raw == null) {
      return;
    }
    try {
      long parsed = Long.parseLong(raw);
      long narrowed = ResourceInformation.VCORES_URI.equals(resourceName)
          ? (long) (int) parsed : parsed;
      if (narrowed != -1L) {
        values.put(resourceName, narrowed);
      }
    } catch (NumberFormatException failure) {
      diagnostic(node.getQueuePath().getFullPath(), fullKey(node, suffix),
          "invalid-integer", failure);
    }
  }

  private Map<String, String> propertiesWithPrefix(QueueConfigNode node,
      String prefix) {
    Map<String, String> result = new LinkedHashMap<>();
    node.getRawProperties().forEach((key, value) -> {
      if (key.startsWith(prefix)) {
        result.put(key.substring(prefix.length()), value);
      }
    });
    return result;
  }

  private void addDiagnostic(ConfigDiagnostic diagnostic) {
    diagnostics.add(new PlanDiagnostic(DiagnosticStage.MODEL,
        diagnostic.getQueuePath() == null
        ? null : diagnostic.getQueuePath().getFullPath(),
        diagnostic.getPropertyKey(), diagnostic.getCode(),
        diagnostic.getMessage()));
  }

  private void diagnostic(String path, String property, String code,
      Throwable failure) {
    diagnostic(DiagnosticStage.QUEUE_SETTINGS, path, property, code, failure);
  }

  private void diagnostic(DiagnosticStage stage, String path,
      String property, String code, Throwable failure) {
    String message = failure.getMessage() == null
        ? failure.getClass().getSimpleName() : failure.getMessage();
    diagnostics.add(new PlanDiagnostic(stage, path, property, code, message));
  }

  private String fullKey(QueueConfigNode node, String suffix) {
    return QueuePrefixes.getQueuePrefix(node.getQueuePath()) + suffix;
  }

  private static MessageDigest sha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
    }
  }

  private static void update(MessageDigest digest, String key, String value) {
    String normalized = key.length() + ":" + key + "="
        + (value == null ? -1 : value.length()) + ":"
        + (value == null ? "" : value) + "\n";
    digest.update(normalized.getBytes(StandardCharsets.UTF_8));
  }

  private static void updateResources(MessageDigest digest, String prefix,
      ResourceValues resources) {
    resources.values().entrySet().stream().sorted(Map.Entry.comparingByKey())
        .forEach(entry -> update(digest, prefix + ":" + entry.getKey(),
            Long.toString(entry.getValue())));
  }

  private static String hex(byte[] digest) {
    StringBuilder result = new StringBuilder(digest.length * 2);
    for (byte value : digest) {
      result.append(String.format(Locale.ROOT, "%02x", value));
    }
    return result.toString();
  }
}
