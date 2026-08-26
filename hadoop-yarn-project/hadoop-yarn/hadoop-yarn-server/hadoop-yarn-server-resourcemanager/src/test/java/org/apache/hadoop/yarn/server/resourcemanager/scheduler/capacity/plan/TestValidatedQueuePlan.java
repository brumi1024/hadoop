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

import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.plan.ValidatedQueuePlan.QueueKind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.plan.ValidatedQueuePlan.QueuePlanNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.CSConfigValidationEngine;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ClusterFacts;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.CompileResult;

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
  public void testPlanOwnsNoLiveSchedulerTypes() {
    for (Class<?> type : List.of(ValidatedQueuePlan.class,
        QueuePlanNode.class, ValidatedQueuePlan.CapacitySetting.class)) {
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
