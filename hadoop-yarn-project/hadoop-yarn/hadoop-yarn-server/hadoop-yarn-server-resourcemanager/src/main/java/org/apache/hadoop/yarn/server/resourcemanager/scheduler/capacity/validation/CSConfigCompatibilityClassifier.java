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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.CSConfigModel;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.QueueConfigNode;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedQueueTemplate.AUTO_QUEUE_LEAF_TEMPLATE_PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedQueueTemplate.AUTO_QUEUE_PARENT_TEMPLATE_PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedQueueTemplate.AUTO_QUEUE_TEMPLATE_PREFIX;

/**
 * Fail-closed descriptor classifier for compiled queue-plan validation.
 *
 * <p>Classification compares raw names only. It does not load, construct,
 * configure, initialize, or otherwise execute a configured extension.</p>
 */
final class CSConfigCompatibilityClassifier {
  private static final String DEFAULT_MANAGEMENT_POLICY =
      "org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity."
          + "queuemanagement.GuaranteedOrZeroCapacityOverTimePolicy";
  private static final String BUILTIN_MULTI_NODE_POLICY =
      "org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement."
          + "ResourceUsageMultiNodeLookupPolicy";
  private static final String DEFAULT_AUTHORIZER =
      "org.apache.hadoop.yarn.security.ConfiguredYarnAuthorizer";

  private static final Set<String> APPLICATION_POLICY_ALIASES = Set.of(
      CapacitySchedulerConfiguration.FIFO_APP_ORDERING_POLICY,
      CapacitySchedulerConfiguration.FAIR_APP_ORDERING_POLICY,
      CapacitySchedulerConfiguration.FIFO_WITH_PARTITIONS_APP_ORDERING_POLICY,
      CapacitySchedulerConfiguration.FIFO_FOR_PENDING_APPS);
  private static final Set<String> APPLICATION_POLICY_CLASSES = Set.of(
      "org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy."
          + "FifoOrderingPolicy",
      "org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy."
          + "FairOrderingPolicy",
      "org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy."
          + "FifoOrderingPolicyWithExclusivePartitions",
      "org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy."
          + "FifoOrderingPolicyForPendingApps");
  private static final Set<String> PARENT_POLICIES = Set.of(
      CapacitySchedulerConfiguration.QUEUE_UTILIZATION_ORDERING_POLICY,
      CapacitySchedulerConfiguration.QUEUE_PRIORITY_UTILIZATION_ORDERING_POLICY);
  private static final Set<String> APPLICATION_POLICY_PARAMETERS = Set.of(
      "ordering-policy.fair.enable-size-based-weight",
      "ordering-policy.exclusive-enforced-partitions");
  private static final Set<String> KNOWN_ACL_PROPERTIES = Set.of(
      "acl_submit_applications", "acl_administer_queue");
  private static final Set<String> RESERVATION_SUFFIXES = Set.of(
      CapacitySchedulerConfiguration.IS_RESERVABLE,
      CapacitySchedulerConfiguration.RESERVATION_WINDOW,
      CapacitySchedulerConfiguration.RESERVATION_ADMISSION_POLICY,
      CapacitySchedulerConfiguration.RESERVATION_AGENT_NAME,
      CapacitySchedulerConfiguration.RESERVATION_SHOW_RESERVATION_AS_QUEUE,
      CapacitySchedulerConfiguration.RESERVATION_PLANNER_NAME,
      CapacitySchedulerConfiguration.RESERVATION_MOVE_ON_EXPIRY,
      CapacitySchedulerConfiguration.RESERVATION_ENFORCEMENT_WINDOW);
  private static final Set<String> SIMPLE_QUEUE_PROPERTIES = Set.of(
      CapacitySchedulerConfiguration.QUEUES,
      CapacitySchedulerConfiguration.CAPACITY,
      CapacitySchedulerConfiguration.MAXIMUM_CAPACITY,
      CapacitySchedulerConfiguration.USER_LIMIT,
      CapacitySchedulerConfiguration.USER_LIMIT_FACTOR,
      CapacitySchedulerConfiguration.STATE,
      CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS,
      CapacitySchedulerConfiguration.DEFAULT_NODE_LABEL_EXPRESSION,
      CapacitySchedulerConfiguration.MAXIMUM_APPLICATIONS_SUFFIX,
      CapacitySchedulerConfiguration.MAXIMUM_AM_RESOURCE_SUFFIX,
      CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION,
      CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION_MB,
      CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION_VCORES,
      CapacitySchedulerConfiguration.MAX_PARALLEL_APPLICATIONS,
      CapacitySchedulerConfiguration.MAXIMUM_LIFETIME_SUFFIX,
      CapacitySchedulerConfiguration.DEFAULT_LIFETIME_SUFFIX,
      CapacitySchedulerConfiguration.QUEUE_PREEMPTION_DISABLED,
      "intra-queue-preemption.disable_preemption",
      "priority",
      CapacitySchedulerConfiguration.DEFAULT_APPLICATION_PRIORITY,
      CapacitySchedulerConfiguration.ALLOW_ZERO_CAPACITY_SUM,
      CapacitySchedulerConfiguration.AUTO_CREATE_CHILD_QUEUE_ENABLED,
      CapacitySchedulerConfiguration.AUTO_QUEUE_CREATION_V2_ENABLED,
      CapacitySchedulerConfiguration.AUTO_QUEUE_CREATION_V2_MAX_QUEUES,
      CapacitySchedulerConfiguration.MAXIMUM_QUEUE_DEPTH,
      CapacitySchedulerConfiguration.AUTO_CREATE_QUEUE_MAX_QUEUES,
      CapacitySchedulerConfiguration.FAIL_AUTO_CREATION_ON_EXCEEDING_CAPACITY,
      "multi-node-sorting.policy");
  private static final Set<String> UNSUPPORTED_QUEUE_PROPERTIES = Set.of(
      CapacitySchedulerConfiguration.AUTO_CREATE_CHILD_QUEUE_AUTO_REMOVAL_ENABLE);
  private static final Set<String> TEMPLATE_PROPERTIES = Set.of(
      CapacitySchedulerConfiguration.CAPACITY,
      CapacitySchedulerConfiguration.MAXIMUM_CAPACITY,
      CapacitySchedulerConfiguration.MAXIMUM_APPLICATIONS_SUFFIX,
      CapacitySchedulerConfiguration.ORDERING_POLICY);
  private static final Set<String> KNOWN_GLOBAL_CAPACITY_PROPERTIES = Set.of(
      CapacitySchedulerConfiguration.QUEUE_MAPPING,
      CapacitySchedulerConfiguration.ENABLE_QUEUE_MAPPING_OVERRIDE,
      CapacitySchedulerConfiguration.MAPPING_RULE_FORMAT,
      CapacitySchedulerConfiguration.PREFIX + "legacy-queue-mode.enabled",
      CapacitySchedulerConfiguration.PREFIX
          + CapacitySchedulerConfiguration.MAXIMUM_QUEUE_DEPTH,
      CapacitySchedulerConfiguration.MAXIMUM_SYSTEM_APPLICATIONS,
      CapacitySchedulerConfiguration.MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT,
      CapacitySchedulerConfiguration.PREFIX + CapacitySchedulerConfiguration.USER_LIMIT,
      CapacitySchedulerConfiguration.PREFIX
          + CapacitySchedulerConfiguration.USER_LIMIT_FACTOR,
      CapacitySchedulerConfiguration.RESERVE_CONT_LOOK_ALL_NODES,
      CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
      CapacitySchedulerConfiguration.MULTI_NODE_PLACEMENT_ENABLED,
      CapacitySchedulerConfiguration.AUTO_CREATE_CHILD_QUEUE_EXPIRED_TIME,
      CapacitySchedulerConfiguration.INTRAQUEUE_PREEMPTION_ENABLED);

  private static final Comparator<LegacyFallbackReason> REASON_ORDER =
      Comparator.comparing((LegacyFallbackReason reason) ->
              reason.code().name())
          .thenComparing(LegacyFallbackReason::queuePath,
              Comparator.nullsFirst(Comparator.naturalOrder()))
          .thenComparing(LegacyFallbackReason::propertyKey,
              Comparator.nullsFirst(Comparator.naturalOrder()))
          .thenComparing(LegacyFallbackReason::descriptor,
              Comparator.nullsFirst(Comparator.naturalOrder()))
          .thenComparing(LegacyFallbackReason::message);

  record Classification(List<LegacyFallbackReason> reasons) {
    Classification {
      reasons = List.copyOf(reasons);
    }

    boolean isEligible() {
      return reasons.isEmpty();
    }
  }

  Classification classify(CSConfigModel model, ClusterFacts facts) {
    List<LegacyFallbackReason> reasons = new ArrayList<>();
    model.getRawProperties().entrySet().stream()
        .filter(entry -> entry.getValue() != null
            && entry.getValue().contains("${"))
        .sorted(Map.Entry.comparingByKey())
        .forEach(entry -> add(reasons,
            LegacyFallbackReason.Code.CONFIGURATION_VARIABLE_SUBSTITUTION,
            null, entry.getKey(), entry.getValue(),
            "Configuration substitution depends on unmodeled runtime facts"));
    checkResourceSemantics(model, facts, reasons);
    checkLegacyQueueMappings(model, reasons);
    checkGlobalExtensions(model.getRawProperties(), reasons);
    checkQueues(model, facts, reasons);
    checkUnknownCapacityProperties(model, reasons);
    reasons.sort(REASON_ORDER);
    return new Classification(reasons);
  }

  private void checkResourceSemantics(CSConfigModel model, ClusterFacts facts,
      List<LegacyFallbackReason> reasons) {
    String factsCalculator = facts.getResourceCalculatorClassName();
    String configuredCalculator = model.getRawProperties().getOrDefault(
        CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
        DefaultResourceCalculator.class.getName()).trim();
    if (!isBuiltInCalculator(factsCalculator)) {
      add(reasons, LegacyFallbackReason.Code.CUSTOM_RESOURCE_CALCULATOR,
          null, CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
          factsCalculator,
          "Resource calculator is not represented by compiled validation");
    }
    if (!isBuiltInCalculator(configuredCalculator)
        || !configuredCalculator.equals(factsCalculator)) {
      add(reasons, LegacyFallbackReason.Code.CUSTOM_RESOURCE_CALCULATOR,
          null, CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
          configuredCalculator,
          "Candidate resource calculator does not match represented live facts");
    }
    Set<String> resourceNames = new LinkedHashSet<>(facts.getResourceNames());
    resourceNames.remove(ResourceInformation.MEMORY_URI);
    resourceNames.remove(ResourceInformation.VCORES_URI);
    String configuredTypes = model.getResourceTypes() == null
        ? "" : model.getResourceTypes().trim();
    String declaredTypes = model.getRawProperties().get(
        YarnConfiguration.RESOURCE_TYPES);
    boolean builtInConfiguredTypes = declaredTypes == null
        || declaredTypes.trim().isEmpty();
    if (!resourceNames.isEmpty() || !builtInConfiguredTypes
        || model.getRawProperties().keySet().stream()
            .anyMatch(key -> key.startsWith(YarnConfiguration.RESOURCE_TYPES + "."))) {
      add(reasons, LegacyFallbackReason.Code.CUSTOM_RESOURCE_SCHEMA, null,
          YarnConfiguration.RESOURCE_TYPES, configuredTypes,
          "Custom resource schema is not represented by compiled validation");
    }
  }

  private void checkGlobalExtensions(Map<String, String> properties,
      List<LegacyFallbackReason> reasons) {
    String placementRules = properties.get(YarnConfiguration.QUEUE_PLACEMENT_RULES);
    if (placementRules != null
        && !placementRules.isEmpty()
        && !placementRules.equals(YarnConfiguration.USER_GROUP_PLACEMENT_RULE)
        && !placementRules.equals(YarnConfiguration.APP_NAME_PLACEMENT_RULE)) {
      add(reasons, LegacyFallbackReason.Code.CUSTOM_PLACEMENT_RULE, null,
          YarnConfiguration.QUEUE_PLACEMENT_RULES, placementRules,
          "Placement rule selection is not represented by compiled validation");
    }
    String runtimeBuckets = properties.get(
        YarnConfiguration.RM_METRICS_RUNTIME_BUCKETS);
    if (runtimeBuckets != null && !runtimeBuckets.equals(
        YarnConfiguration.DEFAULT_RM_METRICS_RUNTIME_BUCKETS)) {
      add(reasons, LegacyFallbackReason.Code.UNMODELED_VALIDATION_DEPENDENCY,
          null, YarnConfiguration.RM_METRICS_RUNTIME_BUCKETS, runtimeBuckets,
          "Non-default queue metric buckets require legacy initialization");
    }
    String mappingFormat = properties.getOrDefault(
        CapacitySchedulerConfiguration.MAPPING_RULE_FORMAT,
        CapacitySchedulerConfiguration.MAPPING_RULE_FORMAT_DEFAULT);
    if (!CapacitySchedulerConfiguration.MAPPING_RULE_FORMAT_LEGACY.equals(
        mappingFormat)) {
      LegacyFallbackReason.Code code = properties.containsKey(
          CapacitySchedulerConfiguration.MAPPING_RULE_JSON_FILE)
          ? LegacyFallbackReason.Code.EXTERNAL_PLACEMENT_RULE_SOURCE
          : LegacyFallbackReason.Code.CUSTOM_PLACEMENT_RULE;
      add(reasons, code, null,
          CapacitySchedulerConfiguration.MAPPING_RULE_FORMAT, mappingFormat,
          "Only inline legacy mapping rules are represented by compiled validation");
    }
    if (properties.containsKey(CapacitySchedulerConfiguration.MAPPING_RULE_JSON_FILE)) {
      add(reasons, LegacyFallbackReason.Code.EXTERNAL_PLACEMENT_RULE_SOURCE,
          null, CapacitySchedulerConfiguration.MAPPING_RULE_JSON_FILE,
          properties.get(CapacitySchedulerConfiguration.MAPPING_RULE_JSON_FILE),
          "External mapping-rule files require legacy validation");
    }
    if (properties.containsKey(CapacitySchedulerConfiguration.MAPPING_RULE_JSON)) {
      add(reasons, LegacyFallbackReason.Code.CUSTOM_PLACEMENT_RULE, null,
          CapacitySchedulerConfiguration.MAPPING_RULE_JSON,
          "inline-json", "JSON mapping rules are not yet represented");
    }

    String monitorsEnabled = properties.get(
        YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS);
    String monitorPolicies = properties.get(
        YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES);
    if (ConfigurationValueParsers.parseBoolean(monitorsEnabled, false)
        || monitorPolicies != null) {
      add(reasons, LegacyFallbackReason.Code.SCHEDULING_MONITOR_POLICY, null,
          YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES, monitorPolicies,
          "Scheduling monitor lifecycle is outside compiled validation");
    }
    String authorizer = properties.get(YarnConfiguration.YARN_AUTHORIZATION_PROVIDER);
    if (authorizer != null && !DEFAULT_AUTHORIZER.equals(authorizer.trim())) {
      add(reasons, LegacyFallbackReason.Code.CUSTOM_AUTHORIZATION_PROVIDER,
          null, YarnConfiguration.YARN_AUTHORIZATION_PROVIDER, authorizer,
          "Custom authorization provider requires legacy validation");
    }
    String groupMapping = properties.get(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUP_MAPPING);
    if (groupMapping != null) {
      add(reasons, LegacyFallbackReason.Code.CUSTOM_GROUP_MAPPING, null,
          CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUP_MAPPING,
          groupMapping,
          "Explicit group-mapping providers remain on legacy validation");
    }
    String workflowMappings = properties.get(
        CapacitySchedulerConfiguration.WORKFLOW_PRIORITY_MAPPINGS);
    if (workflowMappings != null && !workflowMappings.trim().isEmpty()) {
      add(reasons, LegacyFallbackReason.Code.WORKFLOW_PRIORITY_MAPPING, null,
          CapacitySchedulerConfiguration.WORKFLOW_PRIORITY_MAPPINGS,
          workflowMappings,
          "Workflow priority mappings are not represented by pure rules");
    }
    if (properties.containsKey(
        CapacitySchedulerConfiguration.AUTO_CREATE_CHILD_QUEUE_EXPIRED_TIME)) {
      add(reasons, LegacyFallbackReason.Code.UNSUPPORTED_TEMPLATE_PROPERTY,
          null,
          CapacitySchedulerConfiguration.AUTO_CREATE_CHILD_QUEUE_EXPIRED_TIME,
          properties.get(CapacitySchedulerConfiguration
              .AUTO_CREATE_CHILD_QUEUE_EXPIRED_TIME),
          "Dynamic queue expiration is not represented by compiled validation");
    }
  }

  private void checkLegacyQueueMappings(CSConfigModel model,
      List<LegacyFallbackReason> reasons) {
    Map<String, String> properties = model.getRawProperties();
    String mappings = properties.get(
        CapacitySchedulerConfiguration.QUEUE_MAPPING);
    if (mappings != null && !mappings.trim().isEmpty()) {
      for (String rule : mappings.split(",", -1)) {
        if (!isStaticLeafMapping(model, rule)) {
          add(reasons, LegacyFallbackReason.Code.CUSTOM_PLACEMENT_RULE, null,
              CapacitySchedulerConfiguration.QUEUE_MAPPING, rule,
              "Only fully qualified static leaf mapping targets are represented");
        }
      }
    }
    String applicationMappings = properties.get(
        CapacitySchedulerConfiguration.QUEUE_MAPPING_NAME);
    if (applicationMappings != null
        && !applicationMappings.trim().isEmpty()) {
      add(reasons, LegacyFallbackReason.Code.CUSTOM_PLACEMENT_RULE, null,
          CapacitySchedulerConfiguration.QUEUE_MAPPING_NAME,
          applicationMappings,
          "Application-name queue mappings require legacy validation");
    }
  }

  private boolean isStaticLeafMapping(CSConfigModel model, String rule) {
    if (rule.contains("\n") || rule.contains("\r")) {
      return false;
    }
    String[] fields = rule.trim().split(":", -1);
    if (fields.length != 3 || fields[1].trim().isEmpty()
        || !("u".equals(fields[0].trim())
            || "g".equals(fields[0].trim()))) {
      return false;
    }
    String target = fields[2].trim();
    if (!target.startsWith("root.") || target.contains("%")) {
      return false;
    }
    QueueConfigNode node = model.getNodes().get(new QueuePath(target));
    return node != null && node.getChildren().isEmpty()
        && !node.isAutoCreateChildQueueEnabled()
        && !node.isAutoQueueCreationV2Enabled();
  }

  private void checkQueues(CSConfigModel model, ClusterFacts facts,
      List<LegacyFallbackReason> reasons) {
    Map<String, String> raw = model.getRawProperties();
    facts.getOldHierarchy().entrySet().stream()
        .sorted(Comparator.comparing(entry ->
            entry.getKey().getFullPath()))
        .forEach(entry -> {
          ClusterFacts.OldQueue old = entry.getValue();
          String path = entry.getKey().getFullPath();
          if (old.getKind() == ClusterFacts.QueueKind.PLAN
              || old.getKind() == ClusterFacts.QueueKind.RESERVATION) {
            add(reasons, LegacyFallbackReason.Code.RESERVATION_EXTENSION,
                path, null, old.getKind().name(),
                "Existing plan and reservation queues require legacy validation");
          }
          if (old.isDynamic() || old.isAutoCreatedLeaf()) {
            add(reasons, LegacyFallbackReason.Code.DYNAMIC_QUEUE_STATE,
                path, null, null,
                "Existing dynamic queues require live policy recomputation");
          }
        });
    for (QueueConfigNode node : model.getNodes().values()) {
      String path = node.getQueuePath().getFullPath();
      checkAutoCreationFlag(node,
          CapacitySchedulerConfiguration.AUTO_CREATE_CHILD_QUEUE_ENABLED,
          node.isAutoCreateChildQueueEnabled(), reasons);
      checkAutoCreationFlag(node,
          CapacitySchedulerConfiguration.AUTO_QUEUE_CREATION_V2_ENABLED,
          node.isAutoQueueCreationV2Enabled(), reasons);
      boolean parent = !node.getChildren().isEmpty()
          || node.isAutoCreateChildQueueEnabled()
          || node.isAutoQueueCreationV2Enabled();
      String ordering = node.getRawProperty(
          CapacitySchedulerConfiguration.ORDERING_POLICY);
      if (ordering != null) {
        if (!isSupportedOrderingPolicy(parent, ordering)) {
          add(reasons, parent
                  ? LegacyFallbackReason.Code.CUSTOM_PARENT_ORDERING_POLICY
                  : LegacyFallbackReason.Code.CUSTOM_APPLICATION_ORDERING_POLICY,
              path, fullKey(node, CapacitySchedulerConfiguration.ORDERING_POLICY),
              ordering, "Ordering policy is not a represented built-in");
        }
      }
      for (String suffix : node.getOrderingPolicyParameters().keySet()) {
        String fullSuffix = CapacitySchedulerConfiguration.ORDERING_POLICY
            + "." + suffix;
        if (!APPLICATION_POLICY_PARAMETERS.contains(fullSuffix)) {
          add(reasons,
              LegacyFallbackReason.Code.CUSTOM_APPLICATION_ORDERING_POLICY,
              path, fullKey(node, fullSuffix), suffix,
              "Ordering policy parameter is not represented");
        }
      }

      String managementPolicy = node.getRawProperty(
          "auto-create-child-queue.management-policy");
      if (managementPolicy != null
          && !DEFAULT_MANAGEMENT_POLICY.equals(managementPolicy)) {
        add(reasons, LegacyFallbackReason.Code.CUSTOM_QUEUE_MANAGEMENT_POLICY,
            path, fullKey(node,
                "auto-create-child-queue.management-policy"),
            managementPolicy,
            "Custom queue-management policy requires legacy validation");
      }
      if (node.isAutoCreateChildQueueEnabled()
          && !node.getChildren().isEmpty()) {
        add(reasons, LegacyFallbackReason.Code.DYNAMIC_QUEUE_STATE, path,
            fullKey(node,
                CapacitySchedulerConfiguration
                    .AUTO_CREATE_CHILD_QUEUE_ENABLED),
            "true", "Managed parents with configured children are not represented");
      }
      if (node.isAutoCreateChildQueueEnabled()) {
        checkDynamicInteger(node,
            CapacitySchedulerConfiguration.AUTO_CREATE_QUEUE_MAX_QUEUES,
            reasons);
      }
      if (node.isAutoQueueCreationV2Enabled()) {
        checkDynamicInteger(node, CapacitySchedulerConfiguration
            .AUTO_QUEUE_CREATION_V2_MAX_QUEUES, reasons);
        checkDynamicInteger(node,
            CapacitySchedulerConfiguration.MAXIMUM_QUEUE_DEPTH, reasons);
      }
      for (String suffix : node.getRawProperties().keySet()) {
        if (RESERVATION_SUFFIXES.contains(suffix)) {
          add(reasons, LegacyFallbackReason.Code.RESERVATION_EXTENSION, path,
              fullKey(node, suffix), node.getRawProperty(suffix),
              "Reservation and plan-queue extensions require legacy validation");
        }
        if (suffix.equals("acl_application_max_priority")) {
          add(reasons, LegacyFallbackReason.Code.PRIORITY_ACL, path,
              fullKey(node, suffix), null,
              "Application priority ACL parsing requires legacy validation");
        }
        if (UNSUPPORTED_QUEUE_PROPERTIES.contains(suffix)) {
          add(reasons,
              LegacyFallbackReason.Code.UNSUPPORTED_TEMPLATE_PROPERTY, path,
              fullKey(node, suffix), node.getRawProperty(suffix),
              "Dynamic queue removal settings are not represented");
        }
      }

      node.getRawProperties().forEach((suffix, value) -> {
        if (!isTemplateCapacityProperty(suffix)) {
          return;
        }
        boolean maximum = suffix.endsWith(
            CapacitySchedulerConfiguration.MAXIMUM_CAPACITY);
        if (!isValidCapacityValue(value, maximum)) {
          add(reasons, LegacyFallbackReason.Code.UNSUPPORTED_RESOURCE_VALUE,
              path, fullKey(node, suffix), value,
              "Queue capacity syntax is outside the captured built-in schema");
        }
      });

      String maximumAllocation = node.getRawProperty(
          CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION);
      if (maximumAllocation != null) {
        try {
          BuiltInResourceValueParser.parseQueueMaximum(maximumAllocation);
        } catch (RuntimeException failure) {
          add(reasons, LegacyFallbackReason.Code.UNSUPPORTED_RESOURCE_VALUE,
              path, fullKey(node,
                  CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION),
              maximumAllocation,
              "Queue maximum-allocation syntax is not represented");
        }
      }

      checkTemplate(node, AUTO_QUEUE_TEMPLATE_PREFIX, null, reasons);
      checkTemplate(node, AUTO_QUEUE_LEAF_TEMPLATE_PREFIX, false, reasons);
      checkTemplate(node, AUTO_QUEUE_PARENT_TEMPLATE_PREFIX, true, reasons);
      checkTemplate(node,
          CapacitySchedulerConfiguration.AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX
              + ".", false, reasons);

      String queueMultiNode = node.getRawProperty("multi-node-sorting.policy");
      if (queueMultiNode != null
          && !isBoundBuiltInMultiNodeName(queueMultiNode, raw)) {
        add(reasons, LegacyFallbackReason.Code.CUSTOM_MULTI_NODE_POLICY, path,
            fullKey(node, "multi-node-sorting.policy"), queueMultiNode,
            "Queue multi-node policy is not a represented built-in");
      }
    }
    checkGlobalMultiNode(raw, reasons);
  }

  private void checkDynamicInteger(QueueConfigNode node, String suffix,
      List<LegacyFallbackReason> reasons) {
    String configured = node.getRawProperty(suffix);
    if (configured == null) {
      return;
    }
    try {
      ConfigurationValueParsers.parseInt(configured);
    } catch (NumberFormatException failure) {
      add(reasons, LegacyFallbackReason.Code.UNSUPPORTED_TEMPLATE_PROPERTY,
          node.getQueuePath().getFullPath(), fullKey(node, suffix), configured,
          "Dynamic queue numeric descriptor is not represented");
    }
  }

  private void checkTemplate(QueueConfigNode node, String prefix,
      Boolean parentTemplate, List<LegacyFallbackReason> reasons) {
    node.getRawProperties().forEach((suffix, value) -> {
      if (!suffix.startsWith(prefix)) {
        return;
      }
      String templateSuffix = suffix.substring(prefix.length());
      if (isTemplateCapacityProperty(templateSuffix)
          || TEMPLATE_PROPERTIES.contains(templateSuffix)) {
        if (templateSuffix.equals(CapacitySchedulerConfiguration.ORDERING_POLICY)) {
          boolean supported = parentTemplate != null
              && isSupportedOrderingPolicy(parentTemplate, value);
          if (!supported) {
            add(reasons, parentTemplate != null && parentTemplate
                    ? LegacyFallbackReason.Code.CUSTOM_PARENT_ORDERING_POLICY
                    : LegacyFallbackReason.Code.CUSTOM_APPLICATION_ORDERING_POLICY,
                node.getQueuePath().getFullPath(), fullKey(node, suffix), value,
                "Template ordering policy is not represented");
          }
        }
        if (!isValidTemplateValue(templateSuffix, value)) {
          add(reasons, LegacyFallbackReason.Code.UNSUPPORTED_TEMPLATE_PROPERTY,
              node.getQueuePath().getFullPath(), fullKey(node, suffix), value,
              "Dynamic queue template value is not represented");
        }
        return;
      }
      LegacyFallbackReason.Code code = templateSuffix.equals(
          "acl_application_max_priority")
          ? LegacyFallbackReason.Code.PRIORITY_ACL
          : LegacyFallbackReason.Code.UNSUPPORTED_TEMPLATE_PROPERTY;
      add(reasons, code, node.getQueuePath().getFullPath(),
          fullKey(node, suffix), templateSuffix,
          "Dynamic queue template property is not represented");
    });
  }

  private void checkGlobalMultiNode(Map<String, String> properties,
      List<LegacyFallbackReason> reasons) {
    String selected = properties.get(
        CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME);
    if (selected != null && !selected.isEmpty()
        && !isBoundBuiltInMultiNodeName(selected, properties)) {
      add(reasons, LegacyFallbackReason.Code.CUSTOM_MULTI_NODE_POLICY, null,
          CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME,
          selected,
          "Selected multi-node policy is not bound to a represented built-in");
    }
    String names = properties.get(
        CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICIES);
    if (names == null || names.trim().isEmpty()) {
      return;
    }
    for (String configured : names.split(",")) {
      String name = configured.trim();
      if (name.isEmpty()) {
        continue;
      }
      String classKey = CapacitySchedulerConfiguration
          .MULTI_NODE_SORTING_POLICY_NAME + "." + name + ".class";
      String className = properties.get(classKey);
      if (className == null && name.equals(
          CapacitySchedulerConfiguration.DEFAULT_NODE_SORTING_POLICY)) {
        className = BUILTIN_MULTI_NODE_POLICY;
      }
      if (className == null
          || !BUILTIN_MULTI_NODE_POLICY.equals(className.trim())) {
        add(reasons, LegacyFallbackReason.Code.CUSTOM_MULTI_NODE_POLICY, null,
            classKey, className,
            "Multi-node policy is not a represented built-in");
      }
      String intervalKey = CapacitySchedulerConfiguration
          .MULTI_NODE_SORTING_POLICY_NAME + "." + name
          + ".sorting-interval.ms";
      String interval = properties.get(intervalKey);
      if (interval != null) {
        try {
          if (ConfigurationValueParsers.parseLong(interval) < 0) {
            throw new NumberFormatException("negative interval");
          }
        } catch (NumberFormatException failure) {
          add(reasons, LegacyFallbackReason.Code.CUSTOM_MULTI_NODE_POLICY,
              null, intervalKey, interval,
              "Multi-node sorting interval cannot be validated by the compiled path");
        }
      }
    }
  }

  private void checkAutoCreationFlag(QueueConfigNode node, String suffix,
      boolean modeled, List<LegacyFallbackReason> reasons) {
    String raw = node.getRawProperty(suffix);
    if (ConfigurationValueParsers.parseBoolean(raw, false) != modeled) {
      add(reasons, LegacyFallbackReason.Code.UNMODELED_VALIDATION_DEPENDENCY,
          node.getQueuePath().getFullPath(), fullKey(node, suffix), raw,
          "Live and model auto-creation descriptors select different queue kinds");
    }
  }

  private void checkUnknownCapacityProperties(CSConfigModel model,
      List<LegacyFallbackReason> reasons) {
    Set<String> accounted = new HashSet<>(KNOWN_GLOBAL_CAPACITY_PROPERTIES);
    for (QueueConfigNode node : model.getNodes().values()) {
      String queuePrefix = QueuePrefixes.getQueuePrefix(node.getQueuePath());
      for (String suffix : node.getRawProperties().keySet()) {
        if (isKnownQueueProperty(suffix)) {
          accounted.add(queuePrefix + suffix);
        }
      }
    }
    Map<String, String> properties = model.getRawProperties();
    for (String key : properties.keySet()) {
      if (!key.startsWith(CapacitySchedulerConfiguration.PREFIX)
          || accounted.contains(key)
          || isKnownGlobalMultiNodeKey(key, properties)) {
        continue;
      }
      add(reasons, LegacyFallbackReason.Code.UNKNOWN_CAPACITY_PROPERTY, null,
          key, properties.get(key),
          "Capacity Scheduler property is not represented by compiled validation");
    }
  }

  private boolean isKnownQueueProperty(String suffix) {
    return SIMPLE_QUEUE_PROPERTIES.contains(suffix)
        || UNSUPPORTED_QUEUE_PROPERTIES.contains(suffix)
        || suffix.equals(CapacitySchedulerConfiguration.ORDERING_POLICY)
        || APPLICATION_POLICY_PARAMETERS.contains(suffix)
        || KNOWN_ACL_PROPERTIES.contains(suffix)
        || suffix.startsWith("user-settings.") && suffix.endsWith(".weight")
        || isLabelProperty(suffix)
        || suffix.startsWith(AUTO_QUEUE_TEMPLATE_PREFIX)
        || suffix.startsWith(AUTO_QUEUE_LEAF_TEMPLATE_PREFIX)
        || suffix.startsWith(AUTO_QUEUE_PARENT_TEMPLATE_PREFIX)
        || suffix.startsWith(
            CapacitySchedulerConfiguration.AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX
                + ".")
        || suffix.equals("auto-create-child-queue.management-policy")
        || RESERVATION_SUFFIXES.contains(suffix);
  }

  private boolean isLabelProperty(String suffix) {
    String prefix = CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS + ".";
    if (!suffix.startsWith(prefix)) {
      return false;
    }
    int delimiter = suffix.indexOf('.', prefix.length());
    if (delimiter < 0) {
      return false;
    }
    String nested = suffix.substring(delimiter + 1);
    return nested.equals(CapacitySchedulerConfiguration.CAPACITY)
        || nested.equals(CapacitySchedulerConfiguration.MAXIMUM_CAPACITY)
        || nested.equals(CapacitySchedulerConfiguration.MAXIMUM_AM_RESOURCE_SUFFIX);
  }

  private boolean isTemplateCapacityProperty(String suffix) {
    return suffix.equals(CapacitySchedulerConfiguration.CAPACITY)
        || suffix.equals(CapacitySchedulerConfiguration.MAXIMUM_CAPACITY)
        || isLabelProperty(suffix) && (suffix.endsWith("."
            + CapacitySchedulerConfiguration.CAPACITY)
            || suffix.endsWith("."
                + CapacitySchedulerConfiguration.MAXIMUM_CAPACITY));
  }

  private boolean isValidTemplateValue(String suffix, String value) {
    if (suffix.equals(CapacitySchedulerConfiguration.ORDERING_POLICY)) {
      return true;
    }
    if (suffix.equals(
        CapacitySchedulerConfiguration.MAXIMUM_APPLICATIONS_SUFFIX)) {
      try {
        ConfigurationValueParsers.parseInt(value);
        return true;
      } catch (NumberFormatException failure) {
        return false;
      }
    }
    if (!isTemplateCapacityProperty(suffix)) {
      return false;
    }
    boolean maximum = suffix.endsWith(
        CapacitySchedulerConfiguration.MAXIMUM_CAPACITY);
    return isValidCapacityValue(value, maximum);
  }

  private boolean isValidCapacityValue(String value, boolean maximum) {
    if (maximum && "-1".equals(value.trim())) {
      return true;
    }
    try {
      return !BuiltInCapacityParser.parse(value, false).vector().entries()
          .isEmpty();
    } catch (RuntimeException failure) {
      return false;
    }
  }

  private boolean isKnownGlobalMultiNodeKey(String key,
      Map<String, String> properties) {
    if (key.equals(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICIES)
        || key.equals(CapacitySchedulerConfiguration
            .MULTI_NODE_SORTING_POLICY_NAME)) {
      return true;
    }
    Set<String> names = new LinkedHashSet<>();
    String configuredNames = properties.get(
        CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICIES);
    if (configuredNames != null) {
      Arrays.stream(configuredNames.split(","))
          .map(String::trim).filter(name -> !name.isEmpty())
          .forEach(names::add);
    }
    String selected = properties.get(
        CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME);
    if (selected != null && !selected.trim().isEmpty()) {
      names.add(selected.trim());
    }
    return names.stream().anyMatch(name -> key.equals(
        CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME
            + "." + name + ".class") || key.equals(
        CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME
            + "." + name + ".sorting-interval.ms"));
  }

  private boolean isBoundBuiltInMultiNodeName(String policyName,
      Map<String, String> properties) {
    String name = policyName.trim();
    String className = properties.get(CapacitySchedulerConfiguration
        .MULTI_NODE_SORTING_POLICY_NAME + "." + name + ".class");
    return className != null
        && BUILTIN_MULTI_NODE_POLICY.equals(className.trim());
  }

  private boolean isSupportedOrderingPolicy(boolean parent, String raw) {
    String alias = raw.trim();
    if (parent) {
      return PARENT_POLICIES.contains(alias);
    }
    return APPLICATION_POLICY_ALIASES.contains(alias)
        || APPLICATION_POLICY_CLASSES.contains(raw);
  }

  private boolean isBuiltInCalculator(String className) {
    return DefaultResourceCalculator.class.getName().equals(className)
        || DominantResourceCalculator.class.getName().equals(className);
  }

  private String fullKey(QueueConfigNode node, String suffix) {
    return QueuePrefixes.getQueuePrefix(node.getQueuePath()) + suffix;
  }

  private void add(List<LegacyFallbackReason> reasons,
      LegacyFallbackReason.Code code, String queuePath, String property,
      String descriptor, String message) {
    LegacyFallbackReason candidate = new LegacyFallbackReason(code, queuePath,
        property, descriptor, message);
    if (!reasons.contains(candidate)) {
      reasons.add(candidate);
    }
  }
}
