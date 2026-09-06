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

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.placement.ApplicationPlacementContext;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractAutoCreatedLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedLeafQueueConfig;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedQueueManagementPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSConfigBenchmarkGenerator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueManagementChange;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.policy.QueueOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.queuemanagement.GuaranteedOrZeroCapacityOverTimePolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.CSConfigModel;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.MultiNodeLookupPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.ResourceUsageMultiNodeLookupPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.SchedulableEntity;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FairOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FifoOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FifoOrderingPolicyForPendingApps;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FifoOrderingPolicyWithExclusivePartitions;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Fail-closed extension-boundary tests that never execute descriptors. */
public class TestCSConfigCompatibilityClassifier {
  private static final QueuePath ROOT = new QueuePath("root");
  private static final QueuePath A = new QueuePath("root.a");
  private static final AtomicInteger INITIALIZATIONS = new AtomicInteger();
  private static final AtomicInteger CONSTRUCTIONS = new AtomicInteger();
  private static final AtomicInteger CONFIGURES = new AtomicInteger();
  private static final AtomicInteger LIFECYCLE_CALLS = new AtomicInteger();
  private final CSConfigValidationEngine engine =
      new CSConfigValidationEngine();

  public static final class ThrowingOrderingPolicy
      extends FifoOrderingPolicy<SchedulableEntity> {
    static {
      INITIALIZATIONS.incrementAndGet();
    }

    public ThrowingOrderingPolicy() {
      CONSTRUCTIONS.incrementAndGet();
    }

    @Override
    public void configure(Map<String, String> conf) {
      CONFIGURES.incrementAndGet();
      throw new IllegalStateException("configure must not run");
    }
  }

  public static final class ParentPolicyLifecycleSentinel
      implements QueueOrderingPolicy {
    static {
      INITIALIZATIONS.incrementAndGet();
    }

    public ParentPolicyLifecycleSentinel() {
      CONSTRUCTIONS.incrementAndGet();
    }

    @Override
    public void setQueues(List<CSQueue> queues) {
      LIFECYCLE_CALLS.incrementAndGet();
    }

    @Override
    public Iterator<CSQueue> getAssignmentIterator(String partition) {
      LIFECYCLE_CALLS.incrementAndGet();
      return List.<CSQueue>of().iterator();
    }

    @Override
    public String getConfigName() {
      LIFECYCLE_CALLS.incrementAndGet();
      return "sentinel";
    }
  }

  public static final class ManagementPolicyLifecycleSentinel
      implements AutoCreatedQueueManagementPolicy {
    static {
      INITIALIZATIONS.incrementAndGet();
    }

    public ManagementPolicyLifecycleSentinel() {
      CONSTRUCTIONS.incrementAndGet();
    }

    @Override
    public void init(AbstractParentQueue parentQueue) {
      LIFECYCLE_CALLS.incrementAndGet();
    }

    @Override
    public void reinitialize(AbstractParentQueue parentQueue) {
      LIFECYCLE_CALLS.incrementAndGet();
    }

    @Override
    public AutoCreatedLeafQueueConfig getInitialLeafQueueConfiguration(
        AbstractAutoCreatedLeafQueue leafQueue) {
      LIFECYCLE_CALLS.incrementAndGet();
      return null;
    }

    @Override
    public List<QueueManagementChange> computeQueueManagementChanges() {
      LIFECYCLE_CALLS.incrementAndGet();
      return List.of();
    }

    @Override
    public void commitQueueManagementChanges(
        List<QueueManagementChange> queueManagementChanges) {
      LIFECYCLE_CALLS.incrementAndGet();
    }
  }

  public static final class MultiNodePolicyLifecycleSentinel
      implements MultiNodeLookupPolicy<SchedulerNode> {
    static {
      INITIALIZATIONS.incrementAndGet();
    }

    public MultiNodePolicyLifecycleSentinel() {
      CONSTRUCTIONS.incrementAndGet();
    }

    @Override
    public Iterator<SchedulerNode> getPreferredNodeIterator(
        Collection<SchedulerNode> nodes, String partition) {
      LIFECYCLE_CALLS.incrementAndGet();
      return nodes.iterator();
    }

    @Override
    public void addAndRefreshNodesSet(Collection<SchedulerNode> nodes,
        String partition) {
      LIFECYCLE_CALLS.incrementAndGet();
    }

    @Override
    public Set<SchedulerNode> getNodesPerPartition(String partition) {
      LIFECYCLE_CALLS.incrementAndGet();
      return Set.of();
    }
  }

  public static final class PlacementRuleLifecycleSentinel
      extends PlacementRule {
    static {
      INITIALIZATIONS.incrementAndGet();
    }

    public PlacementRuleLifecycleSentinel() {
      CONSTRUCTIONS.incrementAndGet();
    }

    @Override
    public boolean initialize(ResourceScheduler scheduler) {
      LIFECYCLE_CALLS.incrementAndGet();
      return true;
    }

    @Override
    public ApplicationPlacementContext getPlacementForApp(
        ApplicationSubmissionContext application, String user) {
      LIFECYCLE_CALLS.incrementAndGet();
      return null;
    }
  }

  @BeforeEach
  public void resetCounters() {
    INITIALIZATIONS.set(0);
    CONSTRUCTIONS.set(0);
    CONFIGURES.set(0);
    LIFECYCLE_CALLS.set(0);
  }

  @Test
  public void testCustomThrowingPolicyFallsBackBeforePluginExecution() {
    CapacitySchedulerConfiguration conf = percentageTree();
    conf.setOrderingPolicy(A, sentinelName("ThrowingOrderingPolicy"));

    CompileResult result = engine.compile(conf.getModel(),
        ClusterFacts.empty());

    assertFallback(result,
        LegacyFallbackReason.Code.CUSTOM_APPLICATION_ORDERING_POLICY);
    assertEquals(0, CONSTRUCTIONS.get());
    assertEquals(0, CONFIGURES.get());
  }

  @Test
  public void testEveryCustomPolicyFamilyFallsBackBeforeLifecycleExecution() {
    CapacitySchedulerConfiguration application = percentageTree();
    application.setOrderingPolicy(A, sentinelName("ThrowingOrderingPolicy"));
    assertFallback(engine.compile(application.getModel(), ClusterFacts.empty()),
        LegacyFallbackReason.Code.CUSTOM_APPLICATION_ORDERING_POLICY);

    CapacitySchedulerConfiguration parent = percentageTree();
    parent.set(QueuePrefixes.getQueuePrefix(ROOT)
        + CapacitySchedulerConfiguration.ORDERING_POLICY,
        sentinelName("ParentPolicyLifecycleSentinel"));
    assertFallback(engine.compile(parent.getModel(), ClusterFacts.empty()),
        LegacyFallbackReason.Code.CUSTOM_PARENT_ORDERING_POLICY);

    CapacitySchedulerConfiguration management = conf();
    management.setQueues(ROOT, new String[] {"a"});
    management.setCapacity(A, 100F);
    management.setAutoCreateChildQueueEnabled(A, true);
    management.set(QueuePrefixes.getQueuePrefix(A)
        + "auto-create-child-queue.management-policy",
        sentinelName("ManagementPolicyLifecycleSentinel"));
    assertFallback(engine.compile(management.getModel(), ClusterFacts.empty()),
        LegacyFallbackReason.Code.CUSTOM_QUEUE_MANAGEMENT_POLICY);

    CapacitySchedulerConfiguration multiNode = percentageTree();
    multiNode.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICIES,
        "sentinel");
    multiNode.set(CapacitySchedulerConfiguration
        .MULTI_NODE_SORTING_POLICY_NAME + ".sentinel.class",
        sentinelName("MultiNodePolicyLifecycleSentinel"));
    assertFallback(engine.compile(multiNode.getModel(), ClusterFacts.empty()),
        LegacyFallbackReason.Code.CUSTOM_MULTI_NODE_POLICY);

    CapacitySchedulerConfiguration placement = percentageTree();
    placement.set(YarnConfiguration.QUEUE_PLACEMENT_RULES,
        sentinelName("PlacementRuleLifecycleSentinel"));
    assertFallback(engine.compile(placement.getModel(), ClusterFacts.empty()),
        LegacyFallbackReason.Code.CUSTOM_PLACEMENT_RULE);

    assertEquals(0, INITIALIZATIONS.get());
    assertEquals(0, CONSTRUCTIONS.get());
    assertEquals(0, CONFIGURES.get());
    assertEquals(0, LIFECYCLE_CALLS.get());
  }

  @Test
  public void testBuiltInApplicationAliasesAndClassesAreExplicitlyEligible() {
    for (String descriptor : new String[] {
        CapacitySchedulerConfiguration.FIFO_APP_ORDERING_POLICY,
        CapacitySchedulerConfiguration.FAIR_APP_ORDERING_POLICY,
        CapacitySchedulerConfiguration.FIFO_WITH_PARTITIONS_APP_ORDERING_POLICY,
        CapacitySchedulerConfiguration.FIFO_FOR_PENDING_APPS,
        FifoOrderingPolicy.class.getName(), FairOrderingPolicy.class.getName(),
        FifoOrderingPolicyWithExclusivePartitions.class.getName(),
        FifoOrderingPolicyForPendingApps.class.getName()}) {
      CapacitySchedulerConfiguration conf = percentageTree();
      conf.setOrderingPolicy(A, descriptor);
      assertEligible(conf, descriptor);
    }
  }

  @Test
  public void testBuiltInParentManagementAndCalculatorDescriptorsAreEligible() {
    for (String descriptor : new String[] {
        CapacitySchedulerConfiguration.QUEUE_UTILIZATION_ORDERING_POLICY,
        CapacitySchedulerConfiguration
            .QUEUE_PRIORITY_UTILIZATION_ORDERING_POLICY}) {
      CapacitySchedulerConfiguration parent = percentageTree();
      parent.set(QueuePrefixes.getQueuePrefix(ROOT)
          + CapacitySchedulerConfiguration.ORDERING_POLICY, descriptor);
      assertEligible(parent, descriptor);
    }

    CapacitySchedulerConfiguration managed = conf();
    managed.setQueues(ROOT, new String[] {"a"});
    managed.setCapacity(A, 100F);
    managed.setAutoCreateChildQueueEnabled(A, true);
    managed.set(QueuePrefixes.getQueuePrefix(A)
        + "auto-create-child-queue.management-policy",
        GuaranteedOrZeroCapacityOverTimePolicy.class.getName());
    assertEligible(managed, "default management-policy class");

    CapacitySchedulerConfiguration calculator = percentageTree();
    calculator.set(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
        DefaultResourceCalculator.class.getName());
    assertEligible(calculator, "default resource-calculator class");
  }

  @Test
  public void testDescriptorWhitespaceMatchesLegacyAsymmetry() {
    CapacitySchedulerConfiguration alias = percentageTree();
    alias.set(QueuePrefixes.getQueuePrefix(A)
        + CapacitySchedulerConfiguration.ORDERING_POLICY, " fair ");
    assertEligible(alias, "trimmed application alias");

    CapacitySchedulerConfiguration applicationClass = percentageTree();
    applicationClass.set(QueuePrefixes.getQueuePrefix(A)
        + CapacitySchedulerConfiguration.ORDERING_POLICY,
        " " + FairOrderingPolicy.class.getName() + " ");
    assertFallback(engine.compile(applicationClass.getModel(),
            ClusterFacts.empty()),
        LegacyFallbackReason.Code.CUSTOM_APPLICATION_ORDERING_POLICY);

    CapacitySchedulerConfiguration parentAlias = percentageTree();
    parentAlias.set(QueuePrefixes.getQueuePrefix(ROOT)
        + CapacitySchedulerConfiguration.ORDERING_POLICY,
        " priority-utilization ");
    assertEligible(parentAlias, "trimmed parent alias");

    CapacitySchedulerConfiguration managementClass = conf();
    managementClass.setQueues(ROOT, new String[] {"a"});
    managementClass.setCapacity(A, 100F);
    managementClass.setAutoCreateChildQueueEnabled(A, true);
    managementClass.set(QueuePrefixes.getQueuePrefix(A)
        + "auto-create-child-queue.management-policy",
        " " + GuaranteedOrZeroCapacityOverTimePolicy.class.getName() + " ");
    assertFallback(engine.compile(managementClass.getModel(),
            ClusterFacts.empty()),
        LegacyFallbackReason.Code.CUSTOM_QUEUE_MANAGEMENT_POLICY);
  }

  @Test
  public void testMultiNodeDefaultSelectorRequiresLiveClassBinding() {
    CapacitySchedulerConfiguration namedDefault = percentageTree();
    namedDefault.set(CapacitySchedulerConfiguration
        .MULTI_NODE_SORTING_POLICIES, "default");
    assertEligible(namedDefault, "implicit default policy-list entry");

    CapacitySchedulerConfiguration selectedDefault = percentageTree();
    selectedDefault.set(CapacitySchedulerConfiguration
        .MULTI_NODE_SORTING_POLICY_NAME, "default");
    assertFallback(engine.compile(selectedDefault.getModel(),
            ClusterFacts.empty()),
        LegacyFallbackReason.Code.CUSTOM_MULTI_NODE_POLICY);

    CapacitySchedulerConfiguration queueDefault = percentageTree();
    queueDefault.set(QueuePrefixes.getQueuePrefix(A)
        + "multi-node-sorting.policy", "default");
    assertFallback(engine.compile(queueDefault.getModel(),
            ClusterFacts.empty()),
        LegacyFallbackReason.Code.CUSTOM_MULTI_NODE_POLICY);

    CapacitySchedulerConfiguration boundDefault = percentageTree();
    boundDefault.set(CapacitySchedulerConfiguration
        .MULTI_NODE_SORTING_POLICY_NAME, "default");
    boundDefault.set(CapacitySchedulerConfiguration
        .MULTI_NODE_SORTING_POLICY_NAME + ".default.class",
        ResourceUsageMultiNodeLookupPolicy.class.getName());
    assertEligible(boundDefault, "explicitly bound default selector");

    CapacitySchedulerConfiguration badInterval = percentageTree();
    badInterval.set(CapacitySchedulerConfiguration
        .MULTI_NODE_SORTING_POLICIES, "default");
    badInterval.set(CapacitySchedulerConfiguration
        .MULTI_NODE_SORTING_POLICY_NAME
        + ".default.sorting-interval.ms", "-1");
    assertFallback(engine.compile(badInterval.getModel(),
            ClusterFacts.empty()),
        LegacyFallbackReason.Code.CUSTOM_MULTI_NODE_POLICY);
  }

  @Test
  public void testExtensionFamiliesSelectStructuredFallbacks() {
    CapacitySchedulerConfiguration parentPolicy = percentageTree();
    parentPolicy.set(QueuePrefixes.getQueuePrefix(ROOT)
        + CapacitySchedulerConfiguration.ORDERING_POLICY,
        "example.CustomParentPolicy");
    assertCode(parentPolicy,
        LegacyFallbackReason.Code.CUSTOM_PARENT_ORDERING_POLICY);

    CapacitySchedulerConfiguration management = conf();
    management.setQueues(ROOT, new String[] {"a"});
    management.setCapacity(A, 100F);
    management.setAutoCreateChildQueueEnabled(A, true);
    management.set(QueuePrefixes.getQueuePrefix(A)
        + "auto-create-child-queue.management-policy",
        "example.CustomManagementPolicy");
    assertCode(management,
        LegacyFallbackReason.Code.CUSTOM_QUEUE_MANAGEMENT_POLICY);

    CapacitySchedulerConfiguration multiNode = percentageTree();
    multiNode.set(QueuePrefixes.getQueuePrefix(A)
        + "multi-node-sorting.policy", "custom");
    assertCode(multiNode,
        LegacyFallbackReason.Code.CUSTOM_MULTI_NODE_POLICY);

    CapacitySchedulerConfiguration reservation = percentageTree();
    reservation.setBoolean(QueuePrefixes.getQueuePrefix(A)
        + CapacitySchedulerConfiguration.IS_RESERVABLE, true);
    assertCode(reservation, LegacyFallbackReason.Code.RESERVATION_EXTENSION);

    CapacitySchedulerConfiguration monitor = percentageTree();
    monitor.set(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS, " true ");
    assertCode(monitor,
        LegacyFallbackReason.Code.SCHEDULING_MONITOR_POLICY);

    CapacitySchedulerConfiguration authorizer = percentageTree();
    authorizer.set(YarnConfiguration.YARN_AUTHORIZATION_PROVIDER,
        "example.Authorizer");
    assertCode(authorizer,
        LegacyFallbackReason.Code.CUSTOM_AUTHORIZATION_PROVIDER);

    CapacitySchedulerConfiguration substitution = percentageTree();
    substitution.set(QueuePrefixes.getQueuePrefix(A)
        + CapacitySchedulerConfiguration.USER_LIMIT, "${user.limit}");
    assertCode(substitution,
        LegacyFallbackReason.Code.CONFIGURATION_VARIABLE_SUBSTITUTION);

    CapacitySchedulerConfiguration unknown = percentageTree();
    unknown.set(QueuePrefixes.getQueuePrefix(A) + "future-setting", "true");
    assertCode(unknown,
        LegacyFallbackReason.Code.UNKNOWN_CAPACITY_PROPERTY);

    CapacitySchedulerConfiguration placement = percentageTree();
    placement.set(YarnConfiguration.QUEUE_PLACEMENT_RULES,
        "example.CustomPlacementRule");
    assertCode(placement, LegacyFallbackReason.Code.CUSTOM_PLACEMENT_RULE);

    CapacitySchedulerConfiguration externalPlacement = percentageTree();
    externalPlacement.set(CapacitySchedulerConfiguration.MAPPING_RULE_FORMAT,
        CapacitySchedulerConfiguration.MAPPING_RULE_FORMAT_JSON);
    externalPlacement.set(
        CapacitySchedulerConfiguration.MAPPING_RULE_JSON_FILE,
        "/tmp/rules.json");
    assertCode(externalPlacement,
        LegacyFallbackReason.Code.EXTERNAL_PLACEMENT_RULE_SOURCE);

    CapacitySchedulerConfiguration groups = percentageTree();
    groups.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUP_MAPPING,
        "example.Groups");
    assertCode(groups, LegacyFallbackReason.Code.CUSTOM_GROUP_MAPPING);

    CapacitySchedulerConfiguration workflow = percentageTree();
    workflow.set(CapacitySchedulerConfiguration.WORKFLOW_PRIORITY_MAPPINGS,
        "workflow1:root.a:1");
    assertCode(workflow,
        LegacyFallbackReason.Code.WORKFLOW_PRIORITY_MAPPING);

    CapacitySchedulerConfiguration priorityAcl = percentageTree();
    priorityAcl.set(QueuePrefixes.getQueuePrefix(A)
        + "acl_application_max_priority", "alice=1");
    assertCode(priorityAcl, LegacyFallbackReason.Code.PRIORITY_ACL);

    CapacitySchedulerConfiguration calculator = percentageTree();
    calculator.set(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
        "example.CustomResourceCalculator");
    assertCode(calculator,
        LegacyFallbackReason.Code.CUSTOM_RESOURCE_CALCULATOR);

    CapacitySchedulerConfiguration schema = percentageTree();
    schema.set(YarnConfiguration.RESOURCE_TYPES, "vendor/gpu");
    assertCode(schema, LegacyFallbackReason.Code.CUSTOM_RESOURCE_SCHEMA);

    CapacitySchedulerConfiguration removal = percentageTree();
    removal.setBoolean(QueuePrefixes.getQueuePrefix(A)
        + CapacitySchedulerConfiguration
            .AUTO_CREATE_CHILD_QUEUE_AUTO_REMOVAL_ENABLE, true);
    assertCode(removal,
        LegacyFallbackReason.Code.UNSUPPORTED_TEMPLATE_PROPERTY);
  }

  @Test
  public void testMalformedAndUnsupportedTemplateValuesFailClosed() {
    assertTemplateFallback("auto-queue-creation-v2.template.capacity",
        "not-a-capacity");
    assertTemplateFallback(
        "auto-queue-creation-v2.leaf-template.maximum-capacity", "bad");
    assertTemplateFallback(
        "auto-queue-creation-v2.leaf-template.maximum-applications", "bad");
    assertTemplateFallback(
        "auto-queue-creation-v2.leaf-template.ordering-policy", "custom",
        LegacyFallbackReason.Code.CUSTOM_APPLICATION_ORDERING_POLICY);
    assertTemplateFallback(
        "auto-queue-creation-v2.leaf-template.state", "RUNNING");
    assertTemplateFallback(
        "auto-queue-creation-v2.leaf-template.accessible-node-labels", "x");
    assertTemplateFallback(
        "auto-queue-creation-v2.leaf-template.acl_submit_applications", "*");
    assertTemplateFallback(
        "auto-queue-creation-v2.leaf-template.user-settings.alice.weight",
        "1.0");
  }

  @Test
  public void testUnsupportedLiveResourceSyntaxFallsBackInsteadOfRejecting() {
    CapacitySchedulerConfiguration conf = percentageTree();
    conf.set(QueuePrefixes.getQueuePrefix(A)
        + CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION,
        "memory=1024,vcores=1");

    assertCode(conf, LegacyFallbackReason.Code.UNSUPPORTED_RESOURCE_VALUE);
  }

  @Test
  public void testUnsupportedOrdinaryCapacityVectorsFailClosedFromRawValues() {
    ClusterFacts builtInFacts = ClusterFacts.empty();
    CapacitySchedulerConfiguration conf = percentageTree();
    String vector = "[memory=100%,vcores=100%,vendor/gpu=1]";
    String[] suffixes = {
        CapacitySchedulerConfiguration.CAPACITY,
        CapacitySchedulerConfiguration.MAXIMUM_CAPACITY,
        CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS
            + ".blue." + CapacitySchedulerConfiguration.CAPACITY,
        CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS
            + ".blue." + CapacitySchedulerConfiguration.MAXIMUM_CAPACITY
    };
    for (String suffix : suffixes) {
      conf.set(QueuePrefixes.getQueuePrefix(A) + suffix, vector);
    }

    Map<String, ResourceInformation> original = copyResourceTypes(
        ResourceUtils.getResourceTypes());
    Map<String, ResourceInformation> polluted = copyResourceTypes(original);
    polluted.put("vendor/gpu", ResourceInformation.newInstance("vendor/gpu",
        "", 0L, ResourceTypes.COUNTABLE, 0L, 100L));
    CSConfigModel model;
    try {
      ResourceUtils.initializeResourcesFromResourceInformationMap(polluted);
      model = conf.getModel();
    } finally {
      ResourceUtils.initializeResourcesFromResourceInformationMap(original);
    }

    CompileResult result = engine.compile(model, builtInFacts);

    assertFallback(result, LegacyFallbackReason.Code.UNSUPPORTED_RESOURCE_VALUE);
    for (String suffix : suffixes) {
      String key = QueuePrefixes.getQueuePrefix(A) + suffix;
      assertTrue(result.getFallbackReasons().stream().anyMatch(reason ->
          reason.code() == LegacyFallbackReason.Code.UNSUPPORTED_RESOURCE_VALUE
              && key.equals(reason.propertyKey())),
          result.getFallbackReasons().toString());
    }
  }

  @Test
  public void testLegacyQueueMappingGrammarUsesLiveParserAsOracle() {
    CapacitySchedulerConfiguration valid = percentageTree();
    valid.set(CapacitySchedulerConfiguration.QUEUE_MAPPING,
        "u:alice:root.a,g:users:root.a");
    assertEquals(2, valid.parseLegacyMappingRules().size());
    assertEligible(valid, "fully qualified static leaf targets");

    for (String mapping : new String[] {
        "x:user:root.a", "u::root.a", "u:user:"}) {
      CapacitySchedulerConfiguration malformed = percentageTree();
      malformed.set(CapacitySchedulerConfiguration.QUEUE_MAPPING, mapping);
      assertThrows(IllegalArgumentException.class,
          malformed::parseLegacyMappingRules);
      assertCode(malformed, LegacyFallbackReason.Code.CUSTOM_PLACEMENT_RULE);
    }

    CapacitySchedulerConfiguration application = percentageTree();
    application.set(CapacitySchedulerConfiguration.QUEUE_MAPPING_NAME,
        "analytics:root.a");
    assertEquals(1, application.parseLegacyMappingRules().size());
    assertCode(application, LegacyFallbackReason.Code.CUSTOM_PLACEMENT_RULE);
  }

  @Test
  public void testUnmodeledInitializationInputsFailClosed() {
    for (String target : new String[] {"a", "root", "root.%user",
        "root.missing"}) {
      CapacitySchedulerConfiguration conf = percentageTree();
      conf.set(CapacitySchedulerConfiguration.QUEUE_MAPPING,
          "u:alice:" + target);
      assertCode(conf, LegacyFallbackReason.Code.CUSTOM_PLACEMENT_RULE);
    }
    CapacitySchedulerConfiguration multiline = percentageTree();
    multiline.set(CapacitySchedulerConfiguration.QUEUE_MAPPING,
        "u:alice:root.a\ng:users:root.b");
    assertCode(multiline, LegacyFallbackReason.Code.CUSTOM_PLACEMENT_RULE);
    CapacitySchedulerConfiguration parentTarget = percentageTree();
    parentTarget.setQueues(A, new String[] {"child"});
    parentTarget.setCapacity(new QueuePath("root.a.child"), 100);
    parentTarget.set(CapacitySchedulerConfiguration.QUEUE_MAPPING,
        "u:alice:root.a");
    assertCode(parentTarget, LegacyFallbackReason.Code.CUSTOM_PLACEMENT_RULE);
    for (String flag : new String[] {
        CapacitySchedulerConfiguration.AUTO_CREATE_CHILD_QUEUE_ENABLED,
        CapacitySchedulerConfiguration.AUTO_QUEUE_CREATION_V2_ENABLED}) {
      CapacitySchedulerConfiguration conf = percentageTree();
      conf.set(QueuePrefixes.getQueuePrefix(A) + flag, " true ");
      conf.setOrderingPolicy(A, "fifo");
      assertCode(conf,
          LegacyFallbackReason.Code.UNMODELED_VALIDATION_DEPENDENCY);
    }
    CapacitySchedulerConfiguration multiNode = percentageTree();
    multiNode.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME,
        " ");
    assertCode(multiNode, LegacyFallbackReason.Code.CUSTOM_MULTI_NODE_POLICY);
    for (String selector : new String[] {
        "user-group,user-group", " user-group ", "user-group,app-name"}) {
      CapacitySchedulerConfiguration conf = percentageTree();
      conf.set(YarnConfiguration.QUEUE_PLACEMENT_RULES, selector);
      assertCode(conf, LegacyFallbackReason.Code.CUSTOM_PLACEMENT_RULE);
    }
    for (String types : new String[] {"memory,vcores", "memory-mb,vcores"}) {
      CapacitySchedulerConfiguration conf = percentageTree();
      conf.set(YarnConfiguration.RESOURCE_TYPES, types);
      assertCode(conf, LegacyFallbackReason.Code.CUSTOM_RESOURCE_SCHEMA);
    }
    for (String buckets : new String[] {"x", "0", "60,60", "1,5,10"}) {
      CapacitySchedulerConfiguration conf = percentageTree();
      conf.set(YarnConfiguration.RM_METRICS_RUNTIME_BUCKETS, buckets);
      assertCode(conf,
          LegacyFallbackReason.Code.UNMODELED_VALIDATION_DEPENDENCY);
    }
  }

  @Test
  public void testModelBackedPlanKindUsesConfigurationBooleanSemantics() {
    CapacitySchedulerConfiguration current = percentageTree();
    current.set(QueuePrefixes.getQueuePrefix(A)
        + CapacitySchedulerConfiguration.IS_RESERVABLE, " TrUe ");
    CapacityScheduler scheduler = Mockito.mock(CapacityScheduler.class);
    ClusterFacts facts = ClusterFacts.capture(scheduler, current.getModel());

    assertEquals(ClusterFacts.QueueKind.PLAN,
        facts.getOldHierarchy().get(A).getKind());
    CompileResult result = engine.compile(percentageTree().getModel(), facts);
    assertFallback(result, LegacyFallbackReason.Code.RESERVATION_EXTENSION);
  }

  @Test
  public void testExistingDynamicAndReservationQueuesAlwaysFallback() {
    CapacitySchedulerConfiguration conf = percentageTree();
    ClusterFacts dynamic = ClusterFacts.builder()
        .withOldQueue(A, ClusterFacts.QueueKind.LEAF,
            QueueState.RUNNING, true)
        .build();
    assertFallback(engine.compile(conf.getModel(), dynamic),
        LegacyFallbackReason.Code.DYNAMIC_QUEUE_STATE);

    ClusterFacts plan = ClusterFacts.builder()
        .withOldQueue(A, ClusterFacts.QueueKind.PLAN,
            QueueState.RUNNING, false)
        .build();
    assertFallback(engine.compile(conf.getModel(), plan),
        LegacyFallbackReason.Code.RESERVATION_EXTENSION);
  }

  @Test
  public void testFallbackReasonOrderDoesNotDependOnPropertyInsertion() {
    CapacitySchedulerConfiguration first = percentageTree();
    first.set(QueuePrefixes.getQueuePrefix(A) + "future-setting", "true");
    first.set(YarnConfiguration.YARN_AUTHORIZATION_PROVIDER,
        "example.Authorizer");
    CapacitySchedulerConfiguration second = percentageTree();
    second.set(YarnConfiguration.YARN_AUTHORIZATION_PROVIDER,
        "example.Authorizer");
    second.set(QueuePrefixes.getQueuePrefix(A) + "future-setting", "true");

    CompileResult firstResult = engine.compile(first.getModel(),
        ClusterFacts.empty());
    CompileResult secondResult = engine.compile(second.getModel(),
        ClusterFacts.empty());

    assertEquals(firstResult.getFallbackReasons(),
        secondResult.getFallbackReasons());
  }

  @Test
  public void testExactBenchmarkProfileIsEligible() {
    CSConfigBenchmarkGenerator.GeneratedConfig generated =
        CSConfigBenchmarkGenerator.generate(100);

    CompileResult result = engine.compile(generated.getConf().getModel(),
        ClusterFacts.empty());

    assertTrue(result.isCompiledActivationEligible(),
        result.getFallbackReasons() + " " + result.getIssues());
  }

  private void assertTemplateFallback(String suffix, String value) {
    assertTemplateFallback(suffix, value,
        LegacyFallbackReason.Code.UNSUPPORTED_TEMPLATE_PROPERTY);
  }

  private void assertTemplateFallback(String suffix, String value,
      LegacyFallbackReason.Code code) {
    CapacitySchedulerConfiguration conf = percentageTree();
    conf.set(QueuePrefixes.getQueuePrefix(A) + suffix, value);
    assertFallback(engine.compile(conf.getModel(), ClusterFacts.empty()),
        code);
  }

  private void assertCode(CapacitySchedulerConfiguration conf,
      LegacyFallbackReason.Code code) {
    assertFallback(engine.compile(conf.getModel(), ClusterFacts.empty()), code);
  }

  private void assertFallback(CompileResult result,
      LegacyFallbackReason.Code code) {
    assertTrue(result.requiresLegacyValidation(), result.getIssues().toString());
    assertFalse(result.isValid());
    assertFalse(result.isCompiledActivationEligible());
    assertTrue(result.getFallbackReasons().stream().anyMatch(reason ->
        reason.code() == code), result.getFallbackReasons().toString());
  }

  private void assertEligible(CapacitySchedulerConfiguration conf,
      String descriptor) {
    CompileResult result = engine.compile(conf.getModel(),
        ClusterFacts.empty());
    assertTrue(result.isCompiledActivationEligible(), descriptor + ": "
        + result.getFallbackReasons() + " " + result.getIssues());
  }

  private Map<String, ResourceInformation> copyResourceTypes(
      Map<String, ResourceInformation> source) {
    Map<String, ResourceInformation> result = new LinkedHashMap<>();
    source.forEach((name, information) -> result.put(name,
        ResourceInformation.newInstance(information)));
    return result;
  }

  private String sentinelName(String simpleName) {
    return TestCSConfigCompatibilityClassifier.class.getName()
        + "$" + simpleName;
  }

  private CapacitySchedulerConfiguration percentageTree() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[] {"a"});
    conf.setCapacity(A, 100F);
    return conf;
  }

  private CapacitySchedulerConfiguration conf() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration(
        new Configuration(false), false);
    conf.set(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
        DefaultResourceCalculator.class.getName());
    return conf;
  }
}
