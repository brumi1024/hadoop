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

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractCSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedQueueTemplate;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.plan.ValidatedQueuePlan.QueueKind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.plan.ValidatedQueuePlan.QueuePlanNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.CSConfigValidationEngine;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ClusterFacts;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.CompileResult;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationQueueBuildContext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestValidatedQueuePlan {
  private static final QueuePath ROOT = new QueuePath("root");

  @Test
  public void testCompilesConfiguredQueueKindsAndOrder() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[] {"leaf", "parent", "managed", "v2",
        "plan"});
    for (String child : List.of("leaf", "parent", "managed", "v2", "plan")) {
      conf.setCapacity(new QueuePath("root." + child), 20);
    }
    QueuePath parent = new QueuePath("root.parent");
    conf.setQueues(parent, new String[] {"child"});
    conf.setCapacity(new QueuePath("root.parent.child"), 100);
    conf.setAutoCreateChildQueueEnabled(new QueuePath("root.managed"), true);
    conf.setAutoQueueCreationV2Enabled(new QueuePath("root.v2"), true);
    conf.setReservable(new QueuePath("root.plan"), true);

    ValidatedQueuePlan plan = ValidatedQueuePlan.fromModel(conf.getModel());

    assertEquals(List.of(new QueuePath("root.leaf"), parent,
        new QueuePath("root.managed"), new QueuePath("root.v2"),
        new QueuePath("root.plan")), plan.getRoot().getChildPaths());
    assertEquals(QueueKind.LEAF,
        plan.getQueue(new QueuePath("root.leaf")).getKind());
    assertEquals(QueueKind.PARENT, plan.getQueue(parent).getKind());
    assertEquals(QueueKind.MANAGED_PARENT,
        plan.getQueue(new QueuePath("root.managed")).getKind());
    assertEquals(QueueKind.PARENT,
        plan.getQueue(new QueuePath("root.v2")).getKind());
    assertEquals(QueueKind.PLAN,
        plan.getQueue(new QueuePath("root.plan")).getKind());
  }

  @Test
  public void testCompilesInheritedLabelsDefaultExpressionAndInitialState() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[] {"a"});
    conf.setCapacity(new QueuePath("root.a"), 100);
    conf.setState(ROOT, QueueState.STOPPED);
    conf.setDefaultNodeLabelExpression(ROOT, "GPU");

    QueuePlanNode child = ValidatedQueuePlan.fromModel(conf.getModel())
        .getQueue(new QueuePath("root.a"));

    assertNull(child.getDeclaredAccessibleNodeLabels());
    assertEquals(java.util.Set.of(RMNodeLabelsManager.ANY),
        child.getAccessibleNodeLabels());
    assertEquals("GPU", child.getDefaultNodeLabelExpression());
    assertEquals(QueueState.STOPPED, child.getInitialState());
  }

  @Test
  public void testCapacityVectorsAreDefensiveCopies() {
    CapacitySchedulerConfiguration conf = conf();
    QueuePath a = new QueuePath("root.a");
    conf.setQueues(ROOT, new String[] {"a"});
    conf.setCapacity(a, "[memory=75w,vcores=25%]");

    QueuePlanNode node = ValidatedQueuePlan.fromModel(conf.getModel())
        .getQueue(a);
    QueueCapacityVector returned = node.getCapacity("").getVector();
    returned.setResource("memory-mb", 999,
        QueueCapacityVector.ResourceUnitCapacityType.ABSOLUTE);

    assertEquals(75,
        node.getCapacity("").getVector().getResource("memory-mb")
            .getResourceValue());
    assertEquals(QueueCapacityVector.ResourceUnitCapacityType.WEIGHT,
        node.getCapacity("").getVector().getResource("memory-mb")
            .getVectorResourceType());
    assertEquals(0,
        node.getMaximumCapacity("").getVector().getResource("memory-mb")
            .getResourceValue());
    assertEquals(QueueCapacityVector.ResourceUnitCapacityType.ABSOLUTE,
        node.getMaximumCapacity("").getVector().getResource("memory-mb")
            .getVectorResourceType());
  }

  @Test
  public void testCompileResultCarriesPlanAndStructuredIssues() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[] {"a", "b"});
    conf.setCapacity(new QueuePath("root.a"), 25);
    conf.setCapacity(new QueuePath("root.b"), 25);

    CompileResult result = new CSConfigValidationEngine().compile(
        conf.getModel(), ClusterFacts.empty());

    assertFalse(result.isValid());
    assertEquals(3, result.getPlan().getQueues().size());
    assertTrue(result.getIssues().stream().anyMatch(issue ->
        "children-capacity-sum".equals(issue.getRuleId())));
  }

  @Test
  public void testCompilesConstructorSettingsAndInheritance() {
    CapacitySchedulerConfiguration conf = conf();
    QueuePath parent = new QueuePath("root.parent");
    QueuePath leaf = new QueuePath("root.parent.leaf");
    conf.setQueues(ROOT, new String[] {"parent"});
    conf.setQueues(parent, new String[] {"leaf"});
    conf.setCapacity(parent, 100);
    conf.setCapacity(leaf, 100);

    conf.setLong(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 8192);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES, 8);
    conf.setQueueMaximumAllocation(parent, "memory-mb=4096,vcores=4");
    conf.setQueueMaximumAllocation(leaf, "vcores=2");
    conf.setMaximumLifetimePerQueue(parent, 100);
    conf.setDefaultLifetimePerQueue(parent, 50);
    conf.setMaximumLifetimePerQueue(leaf, 40);
    conf.setMaxParallelAppsForQueue(leaf, "7");
    conf.setQueuePriority(leaf, 3);
    conf.setBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS, true);
    conf.setBoolean(CapacitySchedulerConfiguration
        .INTRAQUEUE_PREEMPTION_ENABLED, true);
    conf.setPreemptionDisabled(parent, true);
    conf.setPreemptionDisabled(leaf, false);
    conf.setBoolean("yarn.scheduler.capacity.root.parent."
        + "intra-queue-preemption.disable_preemption", true);
    conf.set("yarn.scheduler.capacity.root.parent.user-settings.alice.weight",
        "2.0");
    conf.set("yarn.scheduler.capacity.root.parent.leaf.user-settings.bob.weight",
        "1.5");
    conf.setQueueOrderingPolicy(parent,
        CapacitySchedulerConfiguration
            .QUEUE_PRIORITY_UTILIZATION_ORDERING_POLICY);
    conf.setOrderingPolicy(leaf,
        CapacitySchedulerConfiguration.FAIR_APP_ORDERING_POLICY);
    conf.setAllowZeroCapacitySum(parent, true);
    conf.setBoolean("yarn.scheduler.capacity.root.parent."
        + CapacitySchedulerConfiguration
            .RESERVATION_SHOW_RESERVATION_AS_QUEUE, true);
    conf.setBoolean(CapacitySchedulerConfiguration.RESERVE_CONT_LOOK_ALL_NODES,
        false);
    conf.setInt(CapacitySchedulerConfiguration.NODE_LOCALITY_DELAY, 12);
    conf.setInt(CapacitySchedulerConfiguration.RACK_LOCALITY_ADDITIONAL_DELAY,
        5);
    conf.setBoolean(CapacitySchedulerConfiguration.RACK_LOCALITY_FULL_RESET,
        false);
    conf.set(AutoCreatedQueueTemplate.getAutoQueueTemplatePrefix(parent)
        + "priority", "9");

    ValidatedQueuePlan plan = ValidatedQueuePlan.fromModel(conf.getModel());
    CompiledQueueSettings parentSettings = plan.getQueue(parent).getSettings();
    CompiledQueueSettings leafSettings = plan.getQueue(leaf).getSettings();

    assertEquals(4096, leafSettings.getMaximumAllocation()
        .getValue(ResourceInformation.MEMORY_URI));
    assertEquals(2, leafSettings.getMaximumAllocation()
        .getValue(ResourceInformation.VCORES_URI));
    assertEquals(40, leafSettings.getMaximumApplicationLifetime());
    assertEquals(40, leafSettings.getDefaultApplicationLifetime());
    assertTrue(leafSettings.isDefaultApplicationLifetimeConfigured());
    assertEquals(7, leafSettings.getMaximumParallelApplications());
    assertEquals(3, leafSettings.getPriority());
    assertTrue(parentSettings.isPreemptionDisabled());
    assertFalse(leafSettings.isPreemptionDisabled());
    assertTrue(leafSettings.isIntraQueuePreemptionDisabledInHierarchy());
    assertEquals(Map.of("alice", 2.0f, "bob", 1.5f),
        leafSettings.getUserWeights());
    assertEquals(CapacitySchedulerConfiguration
            .QUEUE_PRIORITY_UTILIZATION_ORDERING_POLICY,
        parentSettings.getParentQueueOrderingPolicy());
    assertEquals(CapacitySchedulerConfiguration.FAIR_APP_ORDERING_POLICY,
        leafSettings.getApplicationOrderingPolicy());
    assertTrue(parentSettings.isAllowZeroCapacitySum());
    assertTrue(parentSettings.isShowReservationsAsQueues());
    assertEquals("9", parentSettings.getDynamicQueueSettings()
        .getCommonTemplateProperties().get("priority"));
    assertFalse(plan.getSchedulerSettings().isReservationsContinueLooking());
    assertEquals(12, plan.getSchedulerSettings().getNodeLocalityDelay());
    assertEquals(5,
        plan.getSchedulerSettings().getRackLocalityAdditionalDelay());
    assertFalse(plan.getSchedulerSettings().isRackLocalityFullReset());
  }

  @Test
  public void testCompileRejectsMaximumAllocationAboveCluster() {
    CapacitySchedulerConfiguration conf = conf();
    QueuePath leaf = new QueuePath("root.a");
    conf.setQueues(ROOT, new String[] {"a"});
    conf.setCapacity(leaf, 100);
    conf.setLong(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 4096);
    conf.setQueueMaximumAllocation(leaf, "memory-mb=8192,vcores=1");

    CompileResult result = new CSConfigValidationEngine().compile(
        conf.getModel(), ClusterFacts.empty());

    assertFalse(result.isValid());
    assertTrue(result.getIssues().stream().anyMatch(issue ->
        "queue-tree-build".equals(issue.getRuleId())
            && issue.getMessage().contains(
                "Queue maximum allocation cannot be larger")),
        result.getIssues().toString());
  }

  @Test
  public void testCompileRejectsInvalidApplicationLifetime() {
    CapacitySchedulerConfiguration conf = conf();
    QueuePath leaf = new QueuePath("root.a");
    conf.setQueues(ROOT, new String[] {"a"});
    conf.setCapacity(leaf, 100);
    conf.setMaximumLifetimePerQueue(leaf, 40);
    conf.setDefaultLifetimePerQueue(leaf, 60);

    CompileResult result = new CSConfigValidationEngine().compile(
        conf.getModel(), ClusterFacts.empty());

    assertFalse(result.isValid());
    assertTrue(result.getIssues().stream().anyMatch(issue ->
        "queue-tree-build".equals(issue.getRuleId())
            && "Default lifetime 60 can't exceed maximum lifetime 40"
                .equals(issue.getMessage())),
        result.getIssues().toString());
  }

  @Test
  public void testCompileRejectsInvalidInheritedUserWeight() {
    CapacitySchedulerConfiguration conf = conf();
    QueuePath leaf = new QueuePath("root.a");
    conf.setQueues(ROOT, new String[] {"a"});
    conf.setCapacity(leaf, 100);
    conf.setUserLimit(leaf, 10);
    conf.set("yarn.scheduler.capacity.root.user-settings.alice.weight",
        "11");

    CompileResult result = new CSConfigValidationEngine().compile(
        conf.getModel(), ClusterFacts.empty());

    assertFalse(result.isValid());
    assertTrue(result.getIssues().stream().anyMatch(issue ->
        "queue-tree-build".equals(issue.getRuleId())
            && issue.getMessage().startsWith(
                "Weight (11.0) for user \"alice\"")),
        result.getIssues().toString());
  }

  @Test
  public void testCompiledSettingsMatchLegacyQueueConstruction()
      throws Exception {
    CapacitySchedulerConfiguration conf = conf();
    QueuePath leafPath = new QueuePath("root.a");
    conf.setQueues(ROOT, new String[] {"a"});
    conf.setCapacity(leafPath, 100);
    conf.setQueueMaximumAllocation(leafPath,
        "memory-mb=4096,vcores=2");
    conf.setMaximumLifetimePerQueue(leafPath, 80);
    conf.setDefaultLifetimePerQueue(leafPath, 40);
    conf.setMaxParallelAppsForQueue(leafPath, "6");
    conf.setQueuePriority(leafPath, 4);
    conf.setUserLimit(leafPath, 50);
    conf.setBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS, true);
    conf.setPreemptionDisabled(leafPath, true);
    conf.set("yarn.scheduler.capacity.root.a.user-settings.alice.weight",
        "2.0");
    conf.setBoolean(CapacitySchedulerConfiguration.RESERVE_CONT_LOOK_ALL_NODES,
        false);

    ValidatedQueuePlan plan = ValidatedQueuePlan.fromModel(conf.getModel());
    CompiledQueueSettings compiled = plan.getQueue(leafPath).getSettings();
    ValidationQueueBuildContext legacyContext =
        new ValidationQueueBuildContext(conf.getModel(), ClusterFacts.empty());
    CSQueue legacyRoot = CapacitySchedulerQueueManager
        .buildQueueTreeForValidation(legacyContext,
            legacyContext.getConfiguration());
    CSQueue legacy = legacyRoot.getChildQueues().get(0);

    assertEquals(legacy.getMaximumAllocation().getMemorySize(),
        compiled.getMaximumAllocation().getValue(
            ResourceInformation.MEMORY_URI));
    assertEquals(legacy.getMaximumAllocation().getVirtualCores(),
        compiled.getMaximumAllocation().getValue(
            ResourceInformation.VCORES_URI));
    assertEquals(legacy.getMaximumApplicationLifetime(),
        compiled.getMaximumApplicationLifetime());
    assertEquals(legacy.getDefaultApplicationLifetime(),
        compiled.getDefaultApplicationLifetime());
    assertEquals(legacy.getDefaultAppLifetimeWasSpecifiedInConfig(),
        compiled.isDefaultApplicationLifetimeConfigured());
    assertEquals(legacy.getMaxParallelApps(),
        compiled.getMaximumParallelApplications());
    assertEquals(legacy.getPriority().getPriority(), compiled.getPriority());
    assertEquals(legacy.getPreemptionDisabled(),
        compiled.isPreemptionDisabled());
    assertEquals(legacy.getIntraQueuePreemptionDisabledInHierarchy(),
        compiled.isIntraQueuePreemptionDisabledInHierarchy());
    assertEquals(legacy.getUserWeights().asMap(), compiled.getUserWeights());
    assertEquals(((AbstractCSQueue) legacy).isReservationsContinueLooking(),
        plan.getSchedulerSettings().isReservationsContinueLooking());
  }

  @Test
  public void testPlanOwnsNoLiveSchedulerTypes() {
    for (Class<?> type : List.of(ValidatedQueuePlan.class,
        QueuePlanNode.class, ValidatedQueuePlan.CapacitySetting.class,
        CompiledQueueSettings.class, CompiledSchedulerSettings.class,
        CompiledQueueSettings.ResourceSetting.class,
        CompiledQueueSettings.DynamicQueueSettings.class)) {
      for (Field field : type.getDeclaredFields()) {
        String fieldType = field.getType().getName();
        assertFalse(fieldType.endsWith("CSQueue"), field.toString());
        assertFalse(fieldType.endsWith("RMContext"), field.toString());
        assertFalse(fieldType.endsWith("QueueBuildContext"), field.toString());
        assertFalse(fieldType.endsWith("QueueMetrics"), field.toString());
        assertFalse(fieldType.endsWith("QueueResourceQuotas"), field.toString());
        assertFalse(fieldType.endsWith("YarnAuthorizationProvider"),
            field.toString());
      }
    }
  }

  private CapacitySchedulerConfiguration conf() {
    return new CapacitySchedulerConfiguration(new Configuration(false), false);
  }
}
